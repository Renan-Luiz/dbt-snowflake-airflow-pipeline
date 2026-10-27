# dbt-snowflake-airflow-pipeline

> ELT pipeline that transforms data in **Snowflake** with **dbt** and orchestrates it with **Apache Airflow**, using **Astronomer Cosmos** to turn every dbt model into its own Airflow task.

![Airflow](https://img.shields.io/badge/Airflow-3.x-017CEE?logo=apacheairflow)
![dbt](https://img.shields.io/badge/dbt-Core-FF694B?logo=dbt)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?logo=snowflake)
![Cosmos](https://img.shields.io/badge/Astronomer-Cosmos-5A4FCF)
![Docker](https://img.shields.io/badge/Docker-Astro%20CLI-2496ED?logo=docker)

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Snowflake Setup](#snowflake-setup)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [How the DAG Works](#how-the-dag-works)
- [dbt Models](#dbt-models)
- [Testing & Data Quality](#testing--data-quality)
- [Running dbt Locally (without Airflow)](#running-dbt-locally-without-airflow)
- [Troubleshooting](#troubleshooting)
- [Roadmap](#roadmap)
- [Author](#author)

---

## Overview

This project runs a dbt project against Snowflake from inside Airflow. Instead of a single `BashOperator` calling `dbt run`, **[Astronomer Cosmos](https://github.com/astronomer/astronomer-cosmos)** parses the dbt project and renders **each model, seed, snapshot and test as an individual Airflow task**, keeping dbt's dependency graph intact.

Why this matters:

- **Granular observability** — each model is visible (with logs and duration) in the Airflow UI.
- **Selective retries** — rerun only the model that failed, not the whole project.
- **Tests run right after the models they validate**, so bad data doesn't flow downstream.
- **Credentials are managed by Airflow** — Cosmos builds the dbt profile from an Airflow connection, so no secrets live in `profiles.yml`.

---

## Architecture

```
 ┌───────────────┐     ┌──────────────────────────────────────────┐
 │  Raw data in  │     │              Snowflake                    │
 │  Snowflake    │ ──▶ │  staging  ──▶  intermediate  ──▶  marts   │
 └───────────────┘     └──────────────────────────────────────────┘
                                         ▲
                                         │ dbt-snowflake (isolated venv)
                        ┌────────────────┴────────────────┐
                        │   Airflow 3 (Astro Runtime)     │
                        │   + Cosmos: 1 task per dbt node │
                        └─────────────────────────────────┘
```

<!-- Add a screenshot of the DAG graph from the Airflow UI: -->
<!-- ![DAG graph](docs/images/dag_graph.png) -->

---

## Tech Stack

| Layer            | Tool                                         |
|------------------|----------------------------------------------|
| Orchestration    | Apache Airflow 3 — Astro Runtime `3.3-9`     |
| dbt integration  | Astronomer Cosmos                            |
| Transformation   | dbt Core + `dbt-snowflake`                   |
| Data warehouse   | Snowflake                                    |
| Local runtime    | Astro CLI + Docker                           |
| Languages        | Python, SQL (Jinja)                          |

---

## Project Structure

```
dbt-snowflake-airflow-pipeline/
├── .astro/                         # Astro CLI project config
├── dags/
│   ├── dbt/
│   │   └── dbt_pipeline/           # dbt project
│   │       ├── analyses/
│   │       ├── macros/
│   │       ├── models/
│   │       ├── seeds/
│   │       ├── snapshots/
│   │       ├── tests/
│   │       ├── dbt_project.yml
│   │       └── .gitignore          # ignores target/, logs/, dbt_packages/
│   ├── .airflowignore              # stops Airflow from parsing the dbt folder as DAGs
│   └── dbt_snowflake_dag.py        # Cosmos DAG
├── include/
├── plugins/
├── tests/                          # DAG integrity tests
├── .dockerignore
├── .env                            # local secrets — NOT committed
├── .gitignore
├── airflow_settings.yaml           # local connections/variables — NOT committed if it holds secrets
├── Dockerfile                      # Astro Runtime + isolated dbt venv
├── packages.txt                    # OS-level packages
├── requirements.txt                # Python deps for Airflow
└── README.md
```

---

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) running
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)
- A Snowflake account with permission to create a warehouse, database and role (or an existing set provided by your admin)

---

## Snowflake Setup

Run once as `ACCOUNTADMIN` (adjust names as needed):

```sql
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS dbt_wh WITH WAREHOUSE_SIZE = 'X-SMALL' AUTO_SUSPEND = 60;
CREATE DATABASE  IF NOT EXISTS dbt_db;
CREATE ROLE      IF NOT EXISTS dbt_role;

GRANT USAGE ON WAREHOUSE dbt_wh TO ROLE dbt_role;
GRANT ALL   ON DATABASE  dbt_db TO ROLE dbt_role;
GRANT ROLE dbt_role TO USER <your_user>;

USE ROLE dbt_role;
CREATE SCHEMA IF NOT EXISTS dbt_db.dbt_schema;
```

---

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/Renan-Luiz/dbt-snowflake-airflow-pipeline.git
cd dbt-snowflake-airflow-pipeline
```

### 2. Start Airflow

```bash
astro dev start
```

This builds the image (installing `dbt-snowflake` in an isolated virtualenv) and starts the Airflow services.

### 3. Open the Airflow UI

Go to **http://localhost:8080**.

### 4. Create the Snowflake connection

**Admin → Connections → Add Connection**

| Field           | Value                                             |
|-----------------|---------------------------------------------------|
| Connection Id   | `snowflake_conn`                                  |
| Connection Type | Snowflake                                         |
| Schema          | `dbt_schema`                                      |
| Login           | `<your_user>`                                     |
| Password        | `<your_password>`                                 |
| Extra (JSON)    | see below                                         |

```json
{
  "account": "<orgname>-<account_name>",
  "warehouse": "dbt_wh",
  "database": "dbt_db",
  "role": "dbt_role"
}
```

> The Connection Id must match the `conn_id` used in `dags/dbt_snowflake_dag.py`.

### 5. Run the pipeline

Unpause the DAG in the UI and trigger it manually.

### Stop the environment

```bash
astro dev stop
```

---

## Configuration

### Dockerfile — isolated dbt environment

dbt and Airflow have conflicting Python dependencies, so dbt is installed in its own virtualenv inside the image:

```dockerfile
FROM astrocrpublic.azurecr.io/runtime:3.3-9

RUN python -m venv dbt_venv && \
    ./dbt_venv/bin/pip install --no-cache-dir dbt-snowflake
```

### requirements.txt

```
astronomer-cosmos
apache-airflow-providers-snowflake
```


## How the DAG Works

1. Cosmos reads `dags/dbt/dbt_pipeline` and builds the dbt dependency graph.
2. Each dbt node becomes an Airflow task group (`run` + `test`).
3. Tasks execute in dependency order: **seeds → staging → intermediate → marts**.
4. If a test fails, downstream models are skipped.

---

## dbt Models

| Layer        | Purpose                                       | Materialization |
|--------------|-----------------------------------------------|-----------------|
| Staging      | Clean, rename and cast raw sources (1:1)      | view            |
| Intermediate | Joins and business logic                      | view / ephemeral|
| Marts        | Fact and dimension tables for analytics       | table           |

---

## Testing & Data Quality

- **Generic tests** (`unique`, `not_null`, `relationships`, `accepted_values`) defined in the models' `.yml` files
- **Singular tests** in `dags/dbt/dbt_pipeline/tests/`
- **DAG integrity tests**: `astro dev pytest`

---

## Running dbt Locally (without Airflow)

Faster loop when developing models:

```bash
cd dags/dbt/dbt_pipeline
python -m venv .venv && source .venv/bin/activate
pip install dbt-snowflake
dbt debug      # needs a local ~/.dbt/profiles.yml pointing to Snowflake
dbt build
dbt docs generate && dbt docs serve
```

---

## Troubleshooting

| Problem | Likely cause / fix |
|---------|--------------------|
| DAG doesn't show up | Import error — run `astro dev parse` or check scheduler logs |
| `dbt: command not found` | `dbt_executable_path` doesn't match the venv created in the Dockerfile |
| `250001: Could not connect to Snowflake` | Wrong `account` format in the connection Extra — use `<orgname>-<account_name>` |
| Password auth rejected | Snowflake is phasing out single-factor password logins — switch to key-pair auth (`SnowflakePrivateKeyPemProfileMapping`) |
| Airflow tries to parse dbt files | Check that `dags/.airflowignore` contains `dbt` |
| Slow DAG parsing | Use `LoadMode.DBT_MANIFEST` with a pre-built `manifest.json` |

---

## Roadmap

- [ ] Switch Snowflake auth to key-pair
- [ ] CI with GitHub Actions (`dbt build` + DAG tests on each PR)
- [ ] Source freshness checks
- [ ] Failure alerts (Slack / email)
- [ ] Deploy to Astro

---

## Author

**Renan Luiz** — [LinkedIn](https://www.linkedin.com/in/renan-luiz-a05627127) · [GitHub](https://github.com/Renan-Luiz)