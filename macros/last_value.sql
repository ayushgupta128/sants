
{% macro last_value(colname) %}

(ARRAY_AGG({{colname}})[ARRAY_SIZE(ARRAY_AGG({{colname}})) - 1])

{% endmacro %}

