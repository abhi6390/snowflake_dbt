{% macro date_macro(col) %}
COALESCE(
    TRY_TO_TIMESTAMP({{ col }}),
    TRY_TO_TIMESTAMP({{ col }}, 'DD/MM/YYYY'),
    TRY_TO_TIMESTAMP({{ col }}, 'YYYY/MM/DD'),
    TRY_TO_TIMESTAMP({{ col }}, 'YYYY-MM-DD'),
    TRY_TO_TIMESTAMP({{ col }}, 'DD-MM-YYYY'),
    TRY_TO_TIMESTAMP({{ col }}, 'YYYY-MM-DD HH24:MI:SS')
)
{% endmacro %}