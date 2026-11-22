# Data Pipeline Sources

A collection of ready-to-run Python data pipelines that extract data from SaaS APIs, databases, message queues and files, and load it into a warehouse or lakehouse. Every pipeline is built on [dlt (data load tool)](https://dlthub.com/docs), which handles schema inference, normalization, incremental loading and writing to the destination.

![Python](https://img.shields.io/badge/python-3.10%E2%80%933.14-blue)
![dlt](https://img.shields.io/badge/built%20with-dlt-6F6FF7)
![License](https://img.shields.io/badge/license-Apache%202.0-green)

## Contents

- [How it works](#how-it-works)
- [Available sources](#available-sources)
- [Project structure](#project-structure)
- [Getting started](#getting-started)
- [Configuration and credentials](#configuration-and-credentials)
- [Destinations](#destinations)
- [Development](#development)
- [Credits and license](#credits-and-license)

## How it works

```
 ┌──────────────┐      ┌───────────────────┐      ┌──────────────┐      ┌─────────────────┐
 │   Source     │ ───► │   Extract         │ ───► │  Normalize   │ ───► │  Load           │
 │ API / DB /   │      │ paginate, filter, │      │ infer schema,│      │ DuckDB, Postgres│
 │ queue / file │      │ incremental state │      │ flatten JSON │      │ BigQuery, S3 …  │
 └──────────────┘      └───────────────────┘      └──────────────┘      └─────────────────┘
```

Each source is a Python package that exposes one or more `dlt` **resources** (for example `issues`, `tickets`, `players_games`). A small `*_pipeline.py` script next to it shows how to run the source end to end:

```python
import dlt
from chess import source

pipeline = dlt.pipeline(
    pipeline_name="chess_pipeline",
    destination="duckdb",
    dataset_name="chess_players_games_data",
)

data = source(["hikaru", "vincentkeymer"], start_month="2022/11", end_month="2022/12")
info = pipeline.run(data.with_resources("players_games", "players_profiles"))
print(info)
```

Running it again only loads new data: the pipeline keeps incremental state between runs.

## Available sources

| Category | Sources |
| --- | --- |
| **Generic** | `rest_api` (any REST API, declarative config), `sql_database` (any SQLAlchemy database), `filesystem` (local, S3, GCS, Azure: CSV, Parquet, JSONL) |
| **Databases & CDC** | `pg_replication` (Postgres logical replication), `mongodb` |
| **Streaming** | `kafka`, `kinesis` |
| **CRM & sales** | `hubspot`, `pipedrive`, `salesforce` |
| **Marketing & analytics** | `google_ads`, `google_analytics`, `facebook_ads`, `bing_webmaster`, `matomo` |
| **Support & ITSM** | `zendesk`, `freshdesk`, `servicenow`, `jira` |
| **Productivity & collaboration** | `airtable`, `asana_dlt`, `notion`, `google_sheets`, `sharepoint`, `slack`, `github`, `inbox` (IMAP email) |
| **E-commerce & payments** | `shopify_dlt`, `stripe_analytics` |
| **HR & recruiting** | `personio`, `workable` |
| **Content & media** | `strapi`, `mux` |
| **Other** | `scraping` (Scrapy), `unstructured_data` (documents to structured tables), `chess`, `pokemon` (demo sources, no credentials needed) |

Each source has its own `README.md` in `sources/<name>/` with setup steps, available resources and configuration options.

## Project structure

```
.
├── sources/
│   ├── <source_name>/          # source package: __init__.py, helpers, settings, README, requirements.txt
│   ├── <source_name>_pipeline.py   # runnable example pipeline for that source
│   └── .dlt/                   # config.toml and example.secrets.toml used by the examples
├── tests/                      # pytest suite, one folder per source
├── init/                       # template used when a new pipeline is scaffolded
├── tools/                      # helper scripts (new source scaffolding, version checks)
├── docs/                       # extra documentation
├── pyproject.toml / uv.lock    # dependencies managed with uv
└── Makefile                    # dev, lint and test shortcuts
```

## Getting started

### 1. Prerequisites

- Python 3.10 or newer
- [uv](https://docs.astral.sh/uv/) for dependency management

### 2. Install

```sh
git clone https://github.com/jasseurchibani/data-pipleline-with-spark.git
cd data-pipleline-with-spark
make dev          # or: uv sync
```

Some sources need extra packages. Install them from the source's own requirements file:

```sh
uv pip install -r sources/hubspot/requirements.txt
```

### 3. Run a pipeline

The demo sources need no credentials, so they are a good first test:

```sh
cd sources
uv run python chess_pipeline.py
uv run python pokemon_pipeline.py
```

Data is written to a local DuckDB file (`chess_pipeline.duckdb`). Inspect it with:

```sh
uv run dlt pipeline chess_pipeline show
```

## Configuration and credentials

Configuration lives in `sources/.dlt/`:

| File | Purpose | Committed? |
| --- | --- | --- |
| `config.toml` | non-secret settings (page sizes, ids, URLs) | yes |
| `secrets.toml` | API keys, tokens, database passwords | **no**, ignored by git |

To set up credentials, create `sources/.dlt/secrets.toml` and add the keys for the sources you use. Each source README lists the keys it expects, for example:

```toml
[sources.hubspot]
api_key = "your-hubspot-private-app-token"

[destination.postgres.credentials]
database = "dlt_data"
username = "loader"
password = "..."
host = "localhost"
port = 5432
```

Any value can also be passed as an environment variable, which is the recommended approach in CI and production:

```sh
export SOURCES__HUBSPOT__API_KEY="..."
export DESTINATION__POSTGRES__CREDENTIALS="postgresql://user:pass@host:5432/db"
```

## Destinations

Change the `destination` argument in `dlt.pipeline(...)` to load to a different target. Supported destinations include DuckDB, PostgreSQL, BigQuery, Snowflake, Redshift, Databricks, Synapse, MotherDuck, ClickHouse, and file-based storage (S3, GCS, Azure Blob) in Parquet, JSONL or Delta / Iceberg table formats. Parquet and Delta output on object storage can be read directly by Spark, Databricks or any lakehouse engine for downstream processing.

See the [dlt destinations docs](https://dlthub.com/docs/dlt-ecosystem/destinations/) for the full list and their credentials.

## Development

```sh
make dev     # install all dependency groups
make lint    # flake8, mypy, black, and dlt init checks
make test    # full test suite (needs credentials for API sources)
```

Run the tests for a single source against local destinations:

```sh
uv run pytest tests/chess -v
make test-local   # DuckDB + local Postgres
```

To scaffold a new source:

```sh
uv run python tools/new_source.py my_source   # copies the chess source as a starting point
```

More details on code style, testing and adding sources are in [CONTRIBUTING.md](CONTRIBUTING.md).

## Credits and license

The sources in this repository are based on the open-source [dlt-hub/verified-sources](https://github.com/dlt-hub/verified-sources) project. Licensed under the Apache License 2.0, see [LICENSE.txt](LICENSE.txt).
