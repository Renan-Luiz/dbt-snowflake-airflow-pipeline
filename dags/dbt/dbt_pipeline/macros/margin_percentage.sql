-- macros/margin_percentage.sql
{% macro margin_percentage(revenue, cost) %}
    round(({{ revenue }} - {{ cost }}) / NULLIF({{ revenue }}, 0) * 100, 2)
{% endmacro %}