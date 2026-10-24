FROM astrocrpublic.azurecr.io/runtime:3.3-9

RUN python -m venv dbt_venv && \
    ./dbt_venv/bin/pip install --no-cache-dir dbt-snowflake
