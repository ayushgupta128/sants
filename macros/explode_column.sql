
{% macro explode_column(relation, array_column, exploded_column_name, untouched_columns=[]) %}
    {% set untouched_columns_str = ", ".join(untouched_columns) %}
    {% set array_column_str = "" if array_column == exploded_column_name else array_column + " ," %}

    WITH base_data AS (
        SELECT
            {{ untouched_columns_str }},
            {{ array_column }}
        FROM {{ relation }}
    )
    SELECT
        {{ untouched_columns_str }},
        {{ array_column_str }}
        exploded_col AS {{ exploded_column_name }}
    FROM base_data
    LATERAL VIEW OUTER EXPLODE({{ array_column }}) temp_table AS exploded_col
{% endmacro %}
