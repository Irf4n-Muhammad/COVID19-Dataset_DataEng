{# Macros calculate the ratio of country population #}

{% macro calculate_ratio(column1, column2) %}

    ROUND(CAST({{ column1 }} AS INTEGER) / NULLIF(CAST({{ column2 }} AS DECIMAL), 0), 5)

{% endmacro %}

