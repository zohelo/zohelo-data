# zohelo-data

Data platform for zohelo.com: R2-native ingestion, native Landing/Archive,
Apache Iceberg Bronze/Silver/Gold, semantic models and the internal SQL/data
portal at https://data.zohelo.com/.

## Operate

Three GitHub Actions workflows, all manual except portal deploys:

- **Pipeline** — the dataflow. Pick a `source` and a `run`: one activity
  (`landing: inspect`, `landing: extract`, `landing: verify`, `bronze: load`,
  `bronze: verify`, `archive: copy Bronze-proven Landing`) or
  `landing → bronze (all steps)`; source `platform` runs `silver/gold: promote`
  and `serving: refresh portal index`. Only activities authorized in
  `config/sources.yaml` run.
- **Ops · ad-hoc** — runs one script from `ops/` (checks, inspections, one-off
  fixes). Nothing ad-hoc goes into the Pipeline.
- **Deploy portal** — builds and deploys the internal portal and its private R2
  read service.

Results of every activity are published as run annotations and the job summary.
Repository regression-test/CI workflows were intentionally removed. Data
operations keep integrity checks, exact-byte verification, bounded retries,
resumable checkpoints and one writer per source.

## Repository structure

```text
config/
  sources.yaml       source registry: authorized activities, storage, schedule
  api_campaigns.yaml settings of the paused API campaign engine
  storage.yaml       storage settings
pipeline/            the dataflow; entry point: python -m pipeline.run
  run.py             plans runs and executes one activity
  sources/<source>/  one folder per source; __init__.py maps activities to code
    eurostat/        landing.py (inspect/extract/verify), bronze.py + bronze_batch.py
                     + decode.py (Bronze load), api_adapter.py (paused)
    gus_bdl/  gus_dbw/  world_bank_wdi/  opendata_org/
  common/            R2 stores, transport, campaign engine, lineage, archive, report
  silver_gold/       Bronze → Silver → Gold promotion (pass-through today)
ops/                 ad-hoc scripts for the Ops workflow (see ops/README.md)
semantic/            dimensional model and metrics (not built yet)
serving/             portal, R2 read worker, portal index
docs/                architecture, data lifecycle, development
.github/
  workflows/         pipeline.yml, ops.yml, deploy-portal.yml
  actions/activity/  shared steps of a pipeline job
```

Python runs with the repository root on `PYTHONPATH`.

## Storage

```text
zohelo-landing-prod
├── 01_landing/   exact/native current source payloads
└── 05_archive/   verified historical raw payloads

zohelo-lakehouse-prod
├── Apache Iceberg namespace: bronze
├── Apache Iceberg namespace: silver
├── Apache Iceberg namespace: gold
├── 06_control/   ingestion/checkpoint/portal operational metadata
└── R2 Data Catalog metadata
```

The internal platform has no `current/` or `releases/` publication gate.
Validated production data in R2 is internally discoverable. A future
customer-facing publishing product, if needed, belongs above Gold and is a
separate concern.

Google Drive is no longer a production data store. Google sign-in in the portal
is identity/authentication only.

See [Architecture](docs/architecture.md), [Data lifecycle](docs/data-lifecycle.md),
[Development](docs/development.md) and [Agent guide](AGENTS.md).
