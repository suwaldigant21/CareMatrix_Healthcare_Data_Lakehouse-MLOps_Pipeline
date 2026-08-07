with source as (
    select * from {{ source('carematrix_raw', 'bronze_beneficiary_summary') }}
),

phi_salt as (
    -- Must match the salt injected into the PySpark Silver claims job
    -- (CAREMATRIX_PHI_SALT). Different salts on either side silently break
    -- every patient-level join between claims and demographics.
    select '{{ env_var('CAREMATRIX_PHI_SALT', 'carematrix-salt-2026') }}' as salt
)

select
    lower(to_hex(sha256(to_utf8(concat(cast(source.desynpuf_id as varchar), phi_salt.salt))))) as desynpuf_id,
    cast(source.year as integer) as year,
    date_parse(source.bene_birth_dt, '%Y%m%d') as bene_birth_dt,
    cast(source.bene_sex_ident_cd as varchar) as bene_sex_ident_cd,
    cast(source.bene_smi_cvrage_tot_mons as integer) as bene_smi_cvrage_tot_mons
from source
cross join phi_salt