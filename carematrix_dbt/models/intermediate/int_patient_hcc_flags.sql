with claims as (
    select * from {{ ref('stg_silver_inpatient_claims') }}
),

crosswalk as (
    select * from {{ ref('seed_icd9_hcc_crosswalk') }}
),

diagnosis_unpivoted as (
    select patient_id_hash, claim_year, icd9_dgns_cd_1 as icd9_code from claims where icd9_dgns_cd_1 is not null
    union all
    select patient_id_hash, claim_year, icd9_dgns_cd_2 from claims where icd9_dgns_cd_2 is not null
    union all
    select patient_id_hash, claim_year, icd9_dgns_cd_3 from claims where icd9_dgns_cd_3 is not null
    union all
    select patient_id_hash, claim_year, icd9_dgns_cd_4 from claims where icd9_dgns_cd_4 is not null
    union all
    select patient_id_hash, claim_year, icd9_dgns_cd_5 from claims where icd9_dgns_cd_5 is not null
    union all
    select patient_id_hash, claim_year, icd9_dgns_cd_6 from claims where icd9_dgns_cd_6 is not null
    union all
    select patient_id_hash, claim_year, icd9_dgns_cd_7 from claims where icd9_dgns_cd_7 is not null
    union all
    select patient_id_hash, claim_year, icd9_dgns_cd_8 from claims where icd9_dgns_cd_8 is not null
    union all
    select patient_id_hash, claim_year, icd9_dgns_cd_9 from claims where icd9_dgns_cd_9 is not null
    union all
    select patient_id_hash, claim_year, icd9_dgns_cd_10 from claims where icd9_dgns_cd_10 is not null
),

patient_cc as (
    select distinct
        d.patient_id_hash,
        d.claim_year,
        cast(c.cc_category as integer) as cc_category
    from diagnosis_unpivoted d
    inner join crosswalk c
        on d.icd9_code = c.icd9_code
)

select
    patient_id_hash,
    claim_year,
    {% for hcc_num in range(1, 178) %}
    max(case when cc_category = {{ hcc_num }} then 1 else 0 end) as hcc_{{ hcc_num }}
    {% if not loop.last %},{% endif %}
    {% endfor %}
from patient_cc
group by patient_id_hash, claim_year