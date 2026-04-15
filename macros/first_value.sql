
{% macro first_value(colname) %}

(ARRAY_AGG({{colname}})[0])

{% endmacro %}
