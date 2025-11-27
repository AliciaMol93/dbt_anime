{% macro surrogate_key(columns) %}
    {{
        dbt_utils.generate_surrogate_key( 
            columns | map("string") | map("trim") | map("lower")
        )
    }}
{% endmacro %}