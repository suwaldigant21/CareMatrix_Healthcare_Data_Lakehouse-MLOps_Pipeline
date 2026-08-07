with source as (
    select * from {{ source('carematrix_raw', 'inpatient_claims') }}
)

select
    cast(claim_id as varchar) as claim_id,
    cast(patient_id_hash as varchar) as patient_id_hash,   -- already masked in the PySpark Silver job, no re-hashing needed here
    cast(claim_year as integer) as claim_year,
    cast(claim_start_date as date) as claim_start_date,
    cast(claim_end_date as date) as claim_end_date,
    cast(icd9_dgns_cd_1 as varchar) as icd9_dgns_cd_1,
    cast(icd9_dgns_cd_2 as varchar) as icd9_dgns_cd_2,
    cast(icd9_dgns_cd_3 as varchar) as icd9_dgns_cd_3,
    cast(icd9_dgns_cd_4 as varchar) as icd9_dgns_cd_4,
    cast(icd9_dgns_cd_5 as varchar) as icd9_dgns_cd_5,
    cast(icd9_dgns_cd_6 as varchar) as icd9_dgns_cd_6,
    cast(icd9_dgns_cd_7 as varchar) as icd9_dgns_cd_7,
    cast(icd9_dgns_cd_8 as varchar) as icd9_dgns_cd_8,
    cast(icd9_dgns_cd_9 as varchar) as icd9_dgns_cd_9,
    cast(icd9_dgns_cd_10 as varchar) as icd9_dgns_cd_10,
    cast(claim_payment_amount as double) as claim_payment_amount
from source