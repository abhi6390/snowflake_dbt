{% macro load_sample_file(table_name, stage_name, file_format) %}

{% set sql %}
COPY INTO {{ table_name }}
FROM {{ stage_name }}
FILE_FORMAT = (FORMAT_NAME = {{ file_format }})
FORCE = TRUE;
{% endset %}

{{ run_query(sql) }}

{% endmacro %}