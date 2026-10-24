"""
Airflow DAG that orchestrates a dbt project writing to Snowflake using Astronomer Cosmos.

Requirements (in the Airflow environment):
    pip install astronomer-cosmos apache-airflow-providers-snowflake

dbt itself should be installed in an isolated virtualenv (recommended) to avoid
dependency conflicts with Airflow, e.g.:
    python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install dbt-snowflake

Airflow connection (Admin > Connections, or env var AIRFLOW_CONN_SNOWFLAKE_DEFAULT):
    Conn Id:   snowflake_default
    Conn Type: Snowflake
    Login:     <user>
    Password:  <password>
    Schema:    <default schema>
    Extra:     {"account": "<account_identifier>",
                "warehouse": "<warehouse>",
                "database": "<database>",
                "role": "<role>"}
"""

from datetime import datetime, timedelta
from pathlib import Path
import os
import shutil
from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode, TestBehavior
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# --------------------------------------------------------------------------- #
# Paths
# --------------------------------------------------------------------------- #

DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/dbt_pipeline"

DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"


# --------------------------------------------------------------------------- #
# Snowflake profile: Cosmos builds profiles.yml on the fly from the Airflow
# connection, so no credentials live in the dbt project.
# --------------------------------------------------------------------------- #
profile_config = ProfileConfig(
    profile_name="default",  # must match `profile:` in dbt_project.yml
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",
        profile_args={"database": "dbt_database", "schema": "dbt_schema"},
    ),
)

# --------------------------------------------------------------------------- #
# Cosmos configs
# --------------------------------------------------------------------------- #
project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    # Pass dbt vars if you use them in models:
    # dbt_vars={"run_date": "{{ ds }}"},
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,  # runs dbt in the Airflow worker
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

render_config = RenderConfig(
    load_method=LoadMode.DBT_LS,  # parses the project with `dbt ls`
    test_behavior=TestBehavior.AFTER_EACH,  # run a model's tests right after the model
    # select=["tag:daily"],  # optional: dbt node selection
    # exclude=["tag:deprecated"],
)

# --------------------------------------------------------------------------- #
# DAG
# --------------------------------------------------------------------------- #
dbt_snowflake_dag = DbtDag(
    dag_id="dbt_snowflake_dag",
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    render_config=render_config,
    # Passed to every generated dbt operator
    operator_args={
        "install_deps": True,  # run `dbt deps` before each task
        "full_refresh": False,
    },
    # Standard Airflow DAG arguments
    schedule="@daily",
    start_date=datetime(2026, 10, 19),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["dbt", "snowflake", "cosmos"],
)