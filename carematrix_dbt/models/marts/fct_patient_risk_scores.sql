with demo as (
    select * from {{ ref('int_patient_demographics') }}
    where model_type = 'Community'
),

hccs as (
    select * from {{ ref('int_patient_hcc_hierarchy') }}
    -- Excludes 68 records with unparseable/missing claim dates (sentinel year 1900)
    -- from year-based aggregation
    where claim_year != 1900
),

weights as (
    select * from {{ ref('seed_hcc_weights') }}
    where model_type = 'Community'
)

select
    d.patient_id_hash,
    d.payment_year,

    coalesce(w_demo.weight, 0.0) as demographic_weight,

    (
        {% for hcc_num in range(1, 178) %}
        (coalesce(h.hcc_{{ hcc_num }}, 0) * coalesce(w_hcc_{{ hcc_num }}.weight, 0.0))
        {% if not loop.last %} + {% endif %}
        {% endfor %}
    ) as disease_weight,

    round(cast(
        coalesce(w_demo.weight, 0.0) +
        (
            {% for hcc_num in range(1, 178) %}
            (coalesce(h.hcc_{{ hcc_num }}, 0) * coalesce(w_hcc_{{ hcc_num }}.weight, 0.0))
            {% if not loop.last %} + {% endif %}
            {% endfor %}
        )
    as decimal(18, 3)), 3) as raw_raf_score

from demo d
left join hccs h
    on d.patient_id_hash = h.patient_id_hash
   and d.payment_year = h.claim_year
left join weights w_demo
    on d.base_demographic_variable_code = w_demo.variable_code
{% for hcc_num in range(1, 178) %}
left join weights w_hcc_{{ hcc_num }}
    on w_hcc_{{ hcc_num }}.variable_code = 'HCC{{ hcc_num }}'
{% endfor %}