# CareMatrix (RiskLens) — Healthcare Data Lakehouse & Risk-Adjustment MLOps Pipeline

An end-to-end, serverless healthcare data engineering pipeline built on CMS's
**DE-SynPUF** public synthetic Medicare dataset. It ingests raw inpatient
claims and beneficiary demographics, PHI-masks and cleans them through a
Bronze → Silver → Gold medallion architecture, computes real **CMS-HCC V12
Risk Adjustment Factor (RAF) scores** using the actual CMS methodology
(hierarchy suppression included), and trains an XGBoost model that predicts
30-day readmission risk directly from the Gold-layer feature store.

This README documents what was actually built, why each decision was made,
and the real bugs — including a "green build" that silently produced wrong
data — that shaped the final design. Every number below was verified against
real pipeline output, not estimated.

> **Data source:** CMS DE-SynPUF — 100% public, synthetic Medicare data.
> No real patient information is used or was ever accessible anywhere in this
> pipeline.

---

## Table of Contents
1. [The Problem](#1-the-problem)
2. [Architecture](#2-architecture)
3. [Tech Stack](#3-tech-stack)
4. [Repository Structure](#4-repository-structure)
5. [The Data](#5-the-data)
6. [Bronze → Silver — PySpark on AWS Glue](#6-bronze--silver--pyspark-on-aws-glue)
7. [Ingestion — n8n + FastAPI](#7-ingestion--n8n--fastapi)
8. [Silver → Gold — dbt Core on Athena](#8-silver--gold--dbt-core-on-athena)
9. [MLOps — 30-Day Readmission Model](#9-mlops--30-day-readmission-model)
10. [Business Intelligence — Power BI](#10-business-intelligence--power-bi)
11. [V1 Scope & Honest Boundaries](#11-v1-scope--honest-boundaries)
12. [Validation & Results](#12-validation--results)
13. [Engineering War Stories](#13-engineering-war-stories)
14. [Security & Cost Notes](#14-security--cost-notes)
15. [How to Reproduce](#15-how-to-reproduce)
16. [Roadmap](#16-roadmap)

---

## 1. The Problem

Medicare Advantage plans are paid a risk-adjusted premium based on the health
profile of their members. CMS's **HCC (Hierarchical Condition Category)
model** is the official methodology: it maps diagnosis codes to clinical
condition categories, suppresses lower-severity conditions when a more severe
one from the same disease chain is present (so a patient isn't double-counted
for both "mild diabetes" and "severe diabetes"), and produces a **Risk
Adjustment Factor (RAF)** per patient. That score has direct financial
consequences, which is why getting the methodology right — not an
approximation of it — was the whole point of this project.

Separately, **30-day hospital readmissions** are a major cost and quality
metric for payers and providers. CareMatrix treats its own Gold-layer risk
scores as a real feature store and trains a model on top of them, rather than
building the ML piece in an isolated notebook disconnected from the pipeline.

---

## 2. Architecture

> 📸 **Screenshot placement:** if you have an exported architecture diagram
> (e.g. from draw.io or Excalidraw), place it here as
> `docs/screenshots/architecture_diagram.png`. Otherwise the ASCII diagram
> below is accurate and can stand alone.

```
 INGESTION                BRONZE                SILVER                  GOLD                    ML / BI
┌────────────┐    ┌──────────────────┐  ┌────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
│ n8n         │    │ S3: bronze/       │  │ AWS Glue PySpark    │  │ dbt Core + Athena     │  │ XGBoost readmission   │
│ (watermark, │───▶│  inpatient_claims │─▶│ bronze_to_silver.py │─▶│ seeds → staging →     │─▶│ model (pyathena pull) │
│ paginated   │    │  (JSONL)          │  │ rename, PHI hash,   │  │ intermediate → mart   │  │                       │
│ FastAPI     │    │ beneficiary_      │  │ dedup, sentinel     │  │ fct_patient_          │  │ Power BI dashboard    │
│ mock claims)│    │  summary (CSV)    │  │ partition, cast     │  │ risk_scores           │  │ (Simba Athena ODBC)   │
└────────────┘    └──────────────────┘  └────────────────────┘  └──────────────────────┘  └──────────────────────┘
```

**Data flow in one sentence:** a mock FastAPI vendor feed is polled
incrementally by n8n into raw JSONL on S3; a Glue PySpark job cleans,
type-casts, PHI-masks, and deduplicates it into partitioned Parquet; dbt
joins that against CMS's own HCC crosswalk/hierarchy/weight reference tables
on Athena to compute real RAF scores; and both an ML model and a Power BI
dashboard read directly from that Gold table as their source of truth.

---

## 3. Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Storage | Amazon S3 | Decoupled storage/compute, cheap, partition-friendly |
| Catalog | AWS Glue Data Catalog | Single metadata layer spanning Bronze/Silver/Gold |
| Query engine | AWS Athena (Trino) | Serverless SQL over Parquet, pay-per-scan |
| Batch ETL | PySpark on AWS Glue | Distributed cleaning, PHI hashing, deterministic dedup |
| Transform | dbt Core + dbt-athena | Version-controlled SQL, lineage, tests, seeds, macros |
| ML | XGBoost + scikit-learn | Tabular, handles class imbalance via `scale_pos_weight` |
| ML data access | pyathena (`PandasCursor`) | Reads the Gold feature store directly, no CSV exports |
| Ingestion | n8n + FastAPI | Real incremental/watermarked orchestration, not a one-shot script |
| IaC | Terraform | Reproducible S3, IAM, Glue Catalog infrastructure |
| BI | Power BI (Simba Athena ODBC) | Direct connection to the Gold mart, no data duplication |

---

## 4. Repository Structure

```
carematrix/
├── terraform/                          # S3, IAM, Glue Catalog — IaC
│   ├── main.tf  s3.tf  iam.tf  variables.tf
├── mock_api/                           # FastAPI vendor-claims simulator
│   ├── main.py  inpatient_claims.csv
├── CareMatrix_Delta_Load_Pipeline.json # n8n watermarked ingestion workflow
├── docker-compose.yml                  # fastapi_server + n8n_orchestrator
├── carematrix_bronze_to_silver.py      # AWS Glue PySpark ETL
├── HCCSoftware2010/                    # Official CMS-HCC V12 reference files
│   ├── F1210F1Y.TXT   (ICD-9 → CC crosswalk)
│   ├── V12H70H.TXT    (hierarchy exclusion macro)
│   └── C1208F4Y       (SAS CPORT weight coefficients — see §13.5)
├── carematrix_dbt/
│   ├── dbt_project.yml  profiles.yml
│   ├── seeds/
│   │   ├── seed_icd9_hcc_crosswalk.csv
│   │   ├── seed_hcc_weights.csv        # 268 rows: Community/Institutional/New Enrollee
│   │   └── seed_hcc_hierarchy.csv      # 51 verified (parent_hcc, excluded_hcc) pairs
│   ├── macros/apply_hcc_hierarchy.sql  # dynamic hierarchy-suppression generator
│   └── models/
│       ├── staging/
│       │   ├── schema.yml
│       │   ├── stg_beneficiary_demographics.sql
│       │   └── stg_silver_inpatient_claims.sql
│       ├── intermediate/
│       │   ├── int_patient_demographics.sql
│       │   ├── int_patient_hcc_flags.sql
│       │   └── int_patient_hcc_hierarchy.sql
│       └── marts/
│           └── fct_patient_risk_scores.sql
├── ml/
│   ├── train_readmission_model.py
│   ├── readmission_model.joblib
│   └── metrics.json
├── docs/screenshots/                   # see per-section placement notes below
└── LICENSE                             # MIT
```

---

## 5. The Data

CMS's **DE-SynPUF** is public *synthetic* data engineered to statistically
resemble real Medicare claims — meaning it reproduces real-world data-quality
problems without any HIPAA exposure. Two source files:

- **Inpatient claims** (Bronze → Silver): **66,773 raw records ingested**,
  **66,705 after deduplication**. 68 records had unparseable/missing claim
  dates and were preserved in a `claim_year = 1900` sentinel partition rather
  than silently dropped (see §13).
- **Beneficiary Summary Files** (2008/2009/2010): 32 columns — patient ID,
  birth/death dates, sex, race, ESRD flag, state/county, coverage-months
  fields, 11 chronic-condition flags, and reimbursement amounts. **343,644
  total rows** across all three years combined.
- **CMS-HCC V12 reference artifacts** (`HCCSoftware2010/`): the actual
  official ICD-9→HCC crosswalk, hierarchy-exclusion macro, and coefficient
  file CMS itself distributes — not an approximation (see §13.5 for how the
  weights were actually extracted from a proprietary SAS format).

Real data-quality problems this pipeline had to solve, not hypothetical ones:

- Dates arrive as `yyyyMMdd` strings (`"20080512"`), not ISO format, and some
  are missing entirely.
- ICD-9 codes have inconsistent casing/whitespace/padding (`486`, `0389`,
  `41401`, `V5789`).
- Raw beneficiary CSVs have **no `year` column** — year exists only in the
  filename, requiring explicit S3 partition-key engineering.
- DE-SynPUF's public Beneficiary Summary File **does not include** state
  buy-in months or original-entitlement-reason fields — meaning Medicaid
  dual-eligibility and disability-interaction RAF terms are not just
  unbuilt, they are **not computable from this dataset at all** (§11).

---

## 6. Bronze → Silver — PySpark on AWS Glue

`carematrix_bronze_to_silver.py`, in order:

1. **Reads** all Bronze JSONL via `spark.read.json`, cached.
2. **Renames** CMS field codes to readable names (`CLM_ID`→`claim_id`,
   `CLM_PMT_AMT`→`claim_payment_amount`, `CLM_FROM_DT`→`claim_start_date`, …).
3. **Data quality gate — before deduplication, not after.** Rows with a
   null/blank `claim_id` or patient ID are filtered out *before* the window
   function runs. Order matters here: dedup-first would silently collapse
   every null-keyed row into a single survivor, making the problem invisible.
4. **PHI masking**: `sha2(concat(DESYNPUF_ID, PHI_SALT), 256)` →
   `patient_id_hash`; the raw identifier is dropped immediately after. Salted
   and deterministic, so downstream joins and dedup work without ever
   re-exposing the raw ID.
5. **Type casting**: `to_date(col, 'yyyyMMdd')` for dates, `Decimal(18,2)` for
   money, `trim(upper(...))` across all 10 ICD-9 diagnosis and procedure
   columns so they exact-match the HCC crosswalk downstream.
6. **Derived fields**: `length_of_stay_days`, `is_payment_adjustment` (flags
   genuine negative claim payment amounts — verified against real data, not
   assumed; see §13).
7. **Sentinel partitioning**: records with an unparseable/missing claim date
   route to `claim_year = 1900` instead of Spark's unpartitioned
   `__HIVE_DEFAULT_PARTITION__` — so no record is ever silently lost, and the
   count is logged for audit visibility.
8. **Deterministic deduplication** on `(patient_id_hash, claim_id)` using
   `row_number() ... order by claim_start_date asc nulls last` — this
   specific ordering matters, because Spark's default ascending sort puts
   nulls *first*, which would otherwise keep a junk null-dated duplicate over
   a row with a real date.
9. **Writes** partitioned Parquet with
   `spark.sql.sources.partitionOverwriteMode = dynamic`, so future
   incremental runs only replace the specific years they touch instead of
   wiping the whole Silver path.

> 📸 **Screenshot placement:** CloudWatch log output showing the run summary
> (`Bronze records read / Dropped / Silver records written`) →
> `docs/screenshots/glue_job_cloudwatch_log.png`, right after this section.

---

## 7. Ingestion — n8n + FastAPI

`mock_api/main.py` simulates a real vendor claims API: API-key auth,
paginated `GET /api/v1/claims?page=&limit=`, and an `updated_after` filter
backed by a synthesized `updated_at` field — this is what makes genuine
**incremental/watermarked** loading possible rather than a naive full reload
every run.

`CareMatrix_Delta_Load_Pipeline.json` is the n8n workflow: reads a stored
watermark, pages through the API, writes each page to
`bronze/inpatient_claims/`, then advances the watermark. `docker-compose.yml`
runs both services on a shared bridge network.

> 📸 **Screenshot placement:** the n8n workflow canvas, and one execution's
> success log → `docs/screenshots/n8n_delta_pipeline.png`, right after this
> section.

---

## 8. Silver → Gold — dbt Core on Athena

### Seeds (version-controlled CMS reference data)

| Seed | Verified shape | Purpose |
|---|---|---|
| `seed_icd9_hcc_crosswalk.csv` | ICD-9 code → `cc_category` | Maps diagnosis codes to CMS condition categories |
| `seed_hcc_weights.csv` | **268 rows** — 111 Community / 107 Institutional / 50 New Enrollee | The actual CMS V12 point weights |
| `seed_hcc_hierarchy.csv` | **51 verified `(parent_hcc, excluded_hcc)` pairs** | Which conditions suppress which lower-severity ones |

### Staging → Intermediate → Mart

- **`stg_beneficiary_demographics.sql`** — reads raw Bronze beneficiary data,
  computes the salted SHA-256 hash of the patient ID, pinned to **lowercase**
  hex explicitly (see §13.6 for why this specific detail mattered).
- **`stg_silver_inpatient_claims.sql`** — reads the already-masked Silver
  claims table; does **not** re-hash, since Silver already did that.
- **`int_patient_demographics.sql`** — age at payment year, sex code, model
  routing (`New_Enrollee` if coverage months < 12, else `Community`), and the
  CMS demographic variable code used to join the demographic weight.
- **`int_patient_hcc_flags.sql`** — unpivots all 10 ICD-9 diagnosis columns
  (not just the primary one — an early draft only used the primary code and
  would have significantly undercounted every patient's real risk), joins the
  crosswalk, and pivots into 177 `hcc_1..hcc_177` flag columns per
  patient-year.
- **`int_patient_hcc_hierarchy.sql`** — a single line calling the macro below.

### The macro — `apply_hcc_hierarchy.sql`

This is the actual CMS methodology, not an approximation of it. At compile
time it queries the hierarchy seed via `run_query`, builds a
`{excluded_hcc: [parent_hccs]}` map, and generates one `CASE` per HCC:

```sql
case when (hcc_parent_1 = 1 or hcc_parent_2 = 1 ...) then 0 else hcc_N end
```

— i.e. "if a more severe condition from the same disease chain is present,
suppress this lower-severity one," which is exactly how CMS prevents a
patient's risk score from being inflated by counting both "diabetes with
complications" and "diabetes without complications" simultaneously.

### The mart — `fct_patient_risk_scores.sql`

```sql
raw_raf_score = demographic_weight + disease_weight
```

Filtered to `model_type = 'Community'` (documented V1 scope, §11), and
excludes `claim_year = 1900` sentinel records with an explicit, commented
`WHERE` clause at this layer — not silently filtered upstream, preserving the
audit trail built into Silver.

> 📸 **Screenshot placement:** `dbt run`/`dbt test` terminal output showing
> 6/6 models and 4/4 tests passing → `docs/screenshots/dbt_run_success.png`,
> right after this section.

---

## 9. MLOps — 30-Day Readmission Model

`ml/train_readmission_model.py` treats the Gold mart as a real feature store.

**Target derivation, done in SQL for auditability** — a claim is a
readmission if it starts 1–30 days after the patient's prior discharge, using
`LAG`/`LEAD` window functions partitioned by patient and ordered by claim
date. Patient-years with **no observed claims are excluded entirely** — an
absence of claims is an *unknown* label, not a negative one.

**Features:** `raw_raf_score`, `demographic_weight`, `disease_weight`,
one-hot age band, and sex.

**Class imbalance:** roughly 86% of patient-years carry zero disease weight
(no inpatient admission that year), so training was restricted to the
**45,205 patient-years that actually had an admission**, at an **11.4%**
positive readmission rate. `scale_pos_weight ≈ 7.7` was passed to XGBoost to
weight the minority class appropriately, and evaluation used ROC-AUC and a
full precision/recall breakdown rather than raw accuracy — accuracy alone is
meaningless on this class distribution.

**Results:**

| Threshold | Precision | Recall | F1 | Business reading |
|---|---|---|---|---|
| 0.3 | 0.174 | 0.873 | 0.290 | Aggressive — catch nearly all readmissions, cheap outreach |
| 0.5 | 0.237 | 0.701 | 0.355 | Balanced clinical baseline |
| 0.7 | 0.367 | 0.409 | 0.387 | Conservative — high-confidence, intensive intervention only |

(sourced directly from `ml/metrics.json` — n=45,205, 36,164 train / 9,041 test, ROC-AUC 0.7778)

**ROC-AUC: 0.778.** The threshold table is the actual point of this section:
which threshold to use is a *business* decision about intervention capacity
and cost, not a single "correct" model output.

**Note on framing:** this predicts *annual* readmission risk at the
patient-year grain, not CMS's official per-admission 30-day readmission
measure — a given year's `disease_weight` can include the readmission claim
itself, so this should be described as population-level annual risk, not a
strict clinical readmission-prediction tool.

> 📸 **Screenshot placement:** the Athena query result panel showing the
> 303,396-row feature pull → `docs/screenshots/athena_ml_feature_query.png`,
> right after this section.

---

## 10. Business Intelligence — Power BI

Connected directly to the Gold mart via the Simba Athena ODBC driver — no
data duplication, the dashboard reads the same table the ML model trains on.

> 📸 **Screenshot placement:** full dashboard screenshot →
> `docs/screenshots/powerbi_dashboard_overview.png`, right after this
> section intro, followed by the individual visuals below if you want them
> broken out separately.

- **Population risk trend** — average `raw_raf_score` by payment year.
- **Risk tier distribution** — patients segmented into Low/Medium/High risk
  bands based on `raw_raf_score`.
- **Risk decomposition** — demographic baseline weight vs. active disease
  weight, stacked, showing how much of each patient's score comes from age/sex
  vs. actual diagnosed conditions.

---

## 11. V1 Scope & Honest Boundaries

1. **Community model only.** Verified end-to-end:
   `demographic_weight + disease_weight = raw_raf_score` on 100% of rows.
2. **Medicaid dual-eligibility and disability-interaction terms are not just
   deferred — they are unbuildable from this specific dataset.** DE-SynPUF's
   public Beneficiary Summary File genuinely does not include state buy-in
   months or original-entitlement-reason fields. This was discovered by
   checking the real 32-column file header against what the model needed,
   not assumed from general CMS methodology.
3. **New Enrollee and Institutional cohorts are excluded** from the mart via
   `model_type = 'Community'` — a documented, deliberate scope decision.
4. **Sentinel over silent loss**, consistently: missing-date claims are
   preserved (not dropped) through Silver, and excluded only at the Gold
   aggregation boundary with a visible, commented `WHERE` clause.
5. **The PHI salt is hardcoded** for portfolio simplicity, explicitly flagged
   in code comments — a production deployment would move it to AWS Secrets
   Manager or SSM Parameter Store.

---

## 12. Validation & Results

`dbt run`: **6/6 models pass**. `dbt test`: **4/4 pass**.

| Relation | Rows |
|---|---|
| `stg_beneficiary_demographics` | 343,644 |
| `stg_silver_inpatient_claims` | 66,705 |
| `int_patient_hcc_flags` / `int_patient_hcc_hierarchy` | 43,268 |
| `fct_patient_risk_scores` | **303,396** (2008: 97,659 · 2009: 105,262 · 2010: 100,475) |

`fct_patient_risk_scores` profile: **110,105 unique patients** · average RAF
**0.674** · max **11.978** · zero negative scores · **41,955** patient-years
carry a nonzero disease weight (**68,199** total disease-weight points across
the dataset) · **100% component consistency** — `demographic_weight +
disease_weight` equals `raw_raf_score` on every single row, checked directly,
not assumed.

The average RAF sitting below the ~1.0 typically seen in a fully-scored
CMS-HCC population is expected, not a red flag: it's a direct, honest
consequence of excluding Medicaid/disability additive terms per the
documented V1 scope in §11 — those terms only ever add weight, so their
absence pulls the average down.

---

## 13. Engineering War Stories

The value of this project is as much in what broke and got caught as in what
worked the first time.

### 13.1 A green `dbt run` that silently produced zero disease weight

`dbt run` completed 6/6 green. A post-run sanity check then showed
`disease_weight = 0` on **every single row**. Root cause: the PySpark Silver
job hashes patient IDs with `sha2(..., 256)`, producing **lowercase** hex; the
dbt beneficiary staging model hashed with Athena's `to_hex(sha256(...))`,
producing **uppercase** hex. Same patient, same salt, correctly computed hash
on both sides — but two different string cases, so every join silently
matched nothing. No query errored. The fix was pinning `lower()` explicitly
at the staging boundary. **The lesson that shaped the rest of this project:
a green build is not proof of correctness — validate output semantics (join
overlap counts, component consistency) rather than trusting exit codes.**

### 13.2 The watermark boundary bug

An early incremental ingestion run was missing exactly one record — CMS's
DE-SynPUF has 66,773 records, but only 66,772 landed. The default watermark
timestamp happened to exactly equal the earliest record's timestamp, and a
strict `>` comparison silently excluded it. Fixed by seeding the initial
watermark one day earlier than the true earliest possible record, then
re-verified against the full 66,773 count.

### 13.3 JSON array vs. JSONL

An early Bronze load wrote each API page as a JSON array
(`[{...},{...}]`) instead of newline-delimited JSON, which Spark's
`spark.read.json` silently misparses. Fixed at the source (n8n emitting
proper JSONL) rather than working around it with a custom Glue classifier.

### 13.4 Missing `job.init()` and unsafe partition overwrite

Two real Glue correctness gaps caught before they could cause damage:
`getResolvedOptions` was imported but `job.init()` was never called, meaning
`job.commit()` wouldn't correctly report run status. Separately, writing
Parquet with default `"overwrite"` mode plus `partitionBy` would silently
**delete every previously-written year's data** the moment a second
incremental run touched a different partition — fixed with
`spark.sql.sources.partitionOverwriteMode = dynamic`.

### 13.5 Decoding CMS's proprietary weight file

The official CMS-HCC V12 point-weight coefficients (`C1208F4Y`) are
distributed only as a SAS **CPORT** binary — a proprietary catalog transport
format with no open-source reader (confirmed directly: `pandas.read_sas`
throws `ValueError: Header record indicates a CPORT file, which is not
readable`). Rather than substitute weights from a newer, structurally
different model version — which was seriously considered and rejected once
it became clear the newer model's HCC category definitions don't line up
1:1 with V12's (V12's diabetes hierarchy has 5 tiers; the newer model's has
3) — the actual fix was decoding the real file using **SAS OnDemand for
Academics** (free), reshaping the resulting 268-column wide export into a
normalized long-format table, and verifying the row count (111 + 107 + 50 =
268) against independent hand-counted totals from the raw hierarchy macro.

### 13.6 Verifying assumptions instead of trusting them, twice

Two separate points in this project where a plausible-sounding claim was
checked against real data instead of accepted on inference:
- A finding that 68 "missing-date" records were the same 68 records removed
  as duplicates (explaining why both counts matched) turned out to be
  **false** on direct verification — the two were unrelated, coincidentally
  equal sets.
- Two source columns (`bene_state_buy_in_mons`,
  `bene_orig_reas_entlmt_cd`) were initially assumed to exist based on
  general CMS-HCC methodology — checking the actual DE-SynPUF file header
  proved they don't. This is *why* §11's scope boundary is a data limitation,
  not a postponed feature.

### 13.7 No Glue table existed for the beneficiary file at all

Discovered mid-Gold-layer-build: only raw CSVs existed in S3, with **no
`year` column** anywhere in the files (year existed only in the filename).
Fixed by copying each year's CSV into an explicit `year=YYYY/` S3 prefix and
registering the table + partitions directly through the Glue API after
Athena's standard DDL parser rejected several attempted `CREATE EXTERNAL
TABLE` variants.

---

## 14. Security & Cost Notes

- **PHI discipline**: raw patient identifiers never leave Bronze; every
  downstream layer works only with a salted SHA-256 hash.
- **S3 hardening** (Terraform): server-side AES-256 encryption, full public
  access blocks, and a bucket policy denying any non-TLS request.
- **Least-privilege IAM**: the pipeline's execution identity is scoped to
  `s3:PutObject/GetObject/ListBucket` on the lakehouse bucket only — no
  wildcard resource permissions.
- **Cost**: fully serverless — Athena bills per-scan against small,
  Parquet-partitioned data, keeping iteration costs negligible.
- **Known gaps, stated honestly**: the PHI salt is currently hardcoded and
  belongs in Secrets Manager before any real deployment; hashing should be
  treated as pseudonymization, not irreversible anonymization, since a fixed
  known salt is theoretically reversible against the finite space of valid
  patient ID formats.

---

## 15. How to Reproduce

```bash
# 1. Infrastructure
cd terraform && terraform init && terraform plan && terraform apply

# 2. Ingestion (local)
docker compose up -d          # starts fastapi_server + n8n_orchestrator
# import CareMatrix_Delta_Load_Pipeline.json into n8n and run it

# 3. Bronze -> Silver
# deploy and run carematrix_bronze_to_silver.py as a Glue job

# 4. Silver -> Gold (dbt)
cd carematrix_dbt
dbt seed  --profiles-dir .
dbt run   --profiles-dir .
dbt test  --profiles-dir .

# 5. ML
cd ../ml
python train_readmission_model.py
```

Expected final state: `carematrix_dev_db.fct_patient_risk_scores` with
303,396 rows, and `ml/readmission_model.joblib` + `ml/metrics.json`.

---

## 16. Roadmap

- Institutional and New Enrollee model scoring, if a data source with the
  required fields becomes available.
- Move the PHI salt to AWS Secrets Manager.
- Automated orchestration (n8n end-to-end, including the dbt + ML steps) so
  the pipeline runs on a schedule rather than manually.
- Formal CI: `dbt test` gating every pull request.

---

## License

MIT — see [LICENSE](LICENSE).
