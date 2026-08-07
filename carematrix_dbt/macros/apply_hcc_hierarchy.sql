{% macro apply_hcc_hierarchy(base_relation, max_hcc=177) %}

    {% set hierarchy_query %}
        select
            cast(parent_hcc as {{ dbt.type_int() }}) as parent_hcc,
            cast(excluded_hcc as {{ dbt.type_int() }}) as excluded_hcc
        from {{ ref('seed_hcc_hierarchy') }}
    {% endset %}

    {% if execute %}
        {% set results = run_query(hierarchy_query) %}
        {% set hierarchy_map = {} %}
        {% for row in results %}
            {% set parent = row['parent_hcc'] | int %}
            {% set excluded = row['excluded_hcc'] | int %}
            {% if excluded not in hierarchy_map %}
                {% do hierarchy_map.update({excluded: []}) %}
            {% endif %}
            {% do hierarchy_map[excluded].append(parent) %}
        {% endfor %}
    {% else %}
        {% set hierarchy_map = {} %}
    {% endif %}

    select
        patient_id_hash,
        claim_year,
        {% for hcc_num in range(1, max_hcc + 1) %}
            {% if hcc_num in hierarchy_map %}
                case
                    when (
                        {% for parent in hierarchy_map[hcc_num] %}
                            hcc_{{ parent }} = 1
                            {% if not loop.last %} or {% endif %}
                        {% endfor %}
                    ) then 0
                    else hcc_{{ hcc_num }}
                end as hcc_{{ hcc_num }}
            {% else %}
                hcc_{{ hcc_num }}
            {% endif %}
            {% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ base_relation }}

{% endmacro %}
