"""
CareMatrix - Bronze to Silver ETL
AWS Glue PySpark Job (Final)

Reads raw JSONL inpatient claims from S3 Bronze, applies:
  - structural renaming for readability
  - type casting (decimal precision for money, date parsing)
  - PHI masking (patient identifier hashed, raw ID dropped)
  - a null-key data quality gate (before dedup, not after)
  - ICD-9 diagnosis/procedure code standardization
  - derived business fields (length of stay, payment-adjustment flag)
  - sentinel partitioning for missing dates (claim_year = 1900)
  - deterministic, null-safe deduplication via window function
Then writes partitioned Parquet to S3 Silver using dynamic partition
overwrite so incremental runs never wipe previously-written years.
"""

import os
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, trim, upper, datediff, when, year, coalesce, lit,
    row_number, sha2, concat, to_date
)
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

# ==============================================================================
# 1. INITIALIZATION & CONFIGURATION
# ==============================================================================
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Dynamic partition overwrite: future incremental runs only replace the
# claim_year partitions they actually touch, instead of wiping the whole
# Silver path.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# S3 Lakehouse Paths (override via env for your own account)
BRONZE_PATH = os.environ.get(
    "CAREMATRIX_BRONZE_PATH",
    "s3://carematrix-lakehouse-dev-demo/bronze/inpatient_claims/",
)
SILVER_PATH = os.environ.get(
    "CAREMATRIX_SILVER_PATH",
    "s3://carematrix-lakehouse-dev-demo/silver/inpatient_claims/",
)

# NOTE: inject this via env var / AWS Secrets Manager for production use.
# The dbt staging model (stg_beneficiary_demographics.sql) must use the SAME
# salt, or the patient-level joins between claims and demographics break.
# Default is a dev-only value for the synthetic DE-SynPUF dataset.
PHI_SALT = os.environ.get("CAREMATRIX_PHI_SALT", "carematrix-salt-2026")

print("=" * 78)
print("Starting Bronze to Silver Inpatient Claims ETL Execution...")
print("=" * 78)

# ==============================================================================
# 2. DATA INGESTION
# ==============================================================================
df_bronze = spark.read.json(BRONZE_PATH).cache()
raw_count = df_bronze.count()
print(f"[Bronze] Records read: {raw_count}")

# ==============================================================================
# 3. STRUCTURAL READABILITY (COLUMN RENAMING)
# ==============================================================================
# Preserves all ~80 schema columns while standardizing key business fields
rename_mapping = {
    "CLM_ID": "claim_id",
    "CLM_PMT_AMT": "claim_payment_amount",
    "NCH_PRMRY_PYR_CLM_PD_AMT": "primary_payer_paid_amount",
    "CLM_UTLZN_DAY_CNT": "utilization_days",
    "CLM_FROM_DT": "claim_start_date",
    "CLM_THRU_DT": "claim_end_date",
}

df_silver = df_bronze
for old_col, new_col in rename_mapping.items():
    if old_col in df_silver.columns:
        df_silver = df_silver.withColumnRenamed(old_col, new_col)

# ==============================================================================
# 4. DATA QUALITY GATE -- must happen BEFORE dedup, not after.
#    Rows with a null/blank claim_id would otherwise all fall into the same
#    partition in the Section 8 window function and get silently collapsed
#    into a single surviving row instead of being recognized as distinct
#    problem records.
# ==============================================================================
quality_df = df_silver.filter(
    col("claim_id").isNotNull() & (col("claim_id") != "") &
    col("DESYNPUF_ID").isNotNull() & (col("DESYNPUF_ID") != "")
)
quality_df = quality_df.cache()
quality_count = quality_df.count()
dropped_for_nulls = raw_count - quality_count
if dropped_for_nulls > 0:
    print(f"[Data Quality] Dropped {dropped_for_nulls} record(s) with null/blank claim_id or patient ID.")

# ==============================================================================
# 5. PHI MASKING
#    DESYNPUF_ID is a synthetic patient identifier. Even though this dataset
#    is CMS's public synthetic data, we hash it here to demonstrate the same
#    discipline a real HIPAA-aligned pipeline requires: raw identifiers must
#    not propagate past Bronze. Salted SHA-256 is deterministic, so joins
#    and dedup logic downstream (Gold-layer HCC/RAF rollups keyed on
#    patient) still work correctly without ever re-exposing the raw ID.
# ==============================================================================
df_silver = quality_df.withColumn(
    "patient_id_hash", sha2(concat(col("DESYNPUF_ID"), lit(PHI_SALT)), 256)
).drop("DESYNPUF_ID")

# ==============================================================================
# 6. TYPE CASTING & CODE STANDARDIZATION
# ==============================================================================
# CMS dates arrive as "yyyyMMdd" strings (e.g. "20080512"), not ISO format.
# year() and datediff() silently return NULL on unparsed date strings rather
# than erroring -- which would otherwise route every record to the 1900
# sentinel partition and make length_of_stay_days null for everything.
# .cast("string") first is a defensive guard in case Spark inferred these
# columns as a numeric type from the raw JSON rather than a string.
date_columns = ["claim_start_date", "claim_end_date"]
for d_col in date_columns:
    if d_col in df_silver.columns:
        df_silver = df_silver.withColumn(d_col, to_date(col(d_col).cast("string"), "yyyyMMdd"))

# Cast monetary metrics to Decimal(18,2) to eliminate floating-point rounding errors
financial_columns = ["claim_payment_amount", "primary_payer_paid_amount"]
for f_col in financial_columns:
    if f_col in df_silver.columns:
        df_silver = df_silver.withColumn(f_col, col(f_col).cast(DecimalType(18, 2)))

# Dynamically clean all ICD-9 Diagnosis and Procedure code strings (1 through 10).
# Raw CMS files have inconsistent casing/whitespace/leading-zero padding across
# codes (e.g. "486", "0389", "41401", "V5789") -- left as-is, these break exact
# string joins against the HCC/RAF reference table in Gold.
for i in range(1, 11):
    diag_col = f"ICD9_DGNS_CD_{i}"
    prcd_col = f"ICD9_PRCDR_CD_{i}"
    if diag_col in df_silver.columns:
        df_silver = df_silver.withColumn(diag_col, trim(upper(col(diag_col))))
    if prcd_col in df_silver.columns:
        df_silver = df_silver.withColumn(prcd_col, trim(upper(col(prcd_col))))

# ==============================================================================
# 7. DERIVED BUSINESS FIELDS
# ==============================================================================
df_silver = df_silver \
    .withColumn("length_of_stay_days", datediff(col("claim_end_date"), col("claim_start_date"))) \
    .withColumn("is_payment_adjustment", when(col("claim_payment_amount") < 0, True).otherwise(False))

# ==============================================================================
# 8. SENTINEL PARTITIONING
#    Missing claim dates are routed to sentinel year 1900 -- rather than
#    letting Spark create an unpartitioned __HIVE_DEFAULT_PARTITION__ folder
#    -- so no record is silently lost. Logged below for audit traceability;
#    exclude claim_year = 1900 from any year-based Gold aggregation.
# ==============================================================================
df_silver = df_silver.withColumn(
    "claim_year",
    coalesce(year(col("claim_start_date")), lit(1900))
)

null_date_count = df_silver.filter(col("claim_start_date").isNull()).count()
if null_date_count > 0:
    print(f"[Data Quality] {null_date_count} record(s) had null claim_start_date -- routed to claim_year=1900 sentinel partition.")

# ==============================================================================
# 9. DEDUPLICATION
#    Composite key (patient_id_hash, claim_id): safer than claim_id alone
#    unless global uniqueness of claim_id has been separately verified.
#    asc_nulls_last() ensures that when duplicates exist, a row with a real
#    claim_start_date is kept over a null-dated duplicate of the same claim
#    (Spark's default ascending sort puts nulls FIRST, which would otherwise
#    keep the null-dated row and discard the good one).
# ==============================================================================
window_spec = Window.partitionBy("patient_id_hash", "claim_id") \
    .orderBy(col("claim_start_date").asc_nulls_last())

df_deduped = df_silver \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num") \
    .cache()

deduped_count = df_deduped.count()
duplicates_removed = quality_count - deduped_count
if duplicates_removed > 0:
    print(f"[Data Quality] Removed {duplicates_removed} duplicate record(s) on (patient_id_hash, claim_id).")

# ==============================================================================
# 10. LOAD TO SILVER (PARQUET)
# ==============================================================================
final_count = df_deduped.count()
print(f"[Silver] Writing {final_count} records...")

df_deduped.write \
    .mode("overwrite") \
    .partitionBy("claim_year") \
    .parquet(SILVER_PATH)

# ==============================================================================
# 11. RUN SUMMARY -- surfaces in CloudWatch / Glue job logs so every run is
#     auditable: what came in, what was dropped and why, what was written.
# ==============================================================================
print("=" * 78)
print("ETL Run Summary")
print(f"  Bronze records read:            {raw_count}")
print(f"  Dropped (null claim/patient id): {dropped_for_nulls}")
print(f"  Null claim_year (sentinel 1900): {null_date_count}")
print(f"  Dropped (duplicate):             {duplicates_removed}")
print(f"  Silver records written:          {final_count}")
print("=" * 78)
print("Bronze to Silver Inpatient Claims Job Completed Successfully.")

job.commit()