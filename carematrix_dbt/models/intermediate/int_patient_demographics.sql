with beneficiary_base as (
    select
        desynpuf_id,
        year as payment_year,
        bene_birth_dt,
        bene_sex_ident_cd,
        bene_smi_cvrage_tot_mons
        -- bene_state_buy_in_mons and bene_orig_reas_entlmt_cd removed:
        -- not present in DE-SynPUF's public Beneficiary Summary File.
        -- Medicaid/disability interaction scoring is out of scope for this
        -- dataset, not just deferred.
    from {{ ref('stg_beneficiary_demographics') }}
),

demographics_calculated as (
    select
        desynpuf_id as patient_id_hash,
        payment_year,
        payment_year - extract(year from bene_birth_dt) as age,
        case when bene_sex_ident_cd = '2' then 'F' else 'M' end as sex_code,
        case when bene_smi_cvrage_tot_mons < 12 then 'New_Enrollee' else 'Community' end as model_type
    from beneficiary_base
)

select
    patient_id_hash,
    payment_year,
    age,
    sex_code,
    model_type,
    sex_code ||
    case
        when age between 0 and 34 then '0_34' when age between 35 and 44 then '35_44'
        when age between 45 and 54 then '45_54' when age between 55 and 59 then '55_59'
        when age between 60 and 64 then '60_64' when age between 65 and 69 then '65_69'
        when age between 70 and 74 then '70_74' when age between 75 and 79 then '75_79'
        when age between 80 and 84 then '80_84' when age between 85 and 89 then '85_89'
        when age between 90 and 94 then '90_94' else '95_GT'
    end as base_demographic_variable_code
from demographics_calculated
