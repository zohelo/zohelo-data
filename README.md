# zohelo-data

Data platform for zohelo.com: R2-native ingestion, native Landing/Archive,
Apache Iceberg Bronze/Silver/Gold, semantic models and the internal SQL/data
portal at https://data.zohelo.com/.

## Operate

There are three operational GitHub Actions:

- **1 · Ingest → Landing** — manually selected, authorized source loads into
  private Cloudflare R2 Landing. It has no schedule.
- **2 · Transform → Bronze / Silver / Gold** — medallion/Iceberg promotion,
  reconciliation and bounded maintenance.
- **3 · Deploy portal** — builds and deploys the internal portal and its private
  R2 read service.

Operations are named `<source>: <action>` (or `platform: <action>`); see
AGENTS.md for the convention.

Repository regression-test/CI workflows were intentionally removed. Data
operations still retain ordinary integrity checks, exact-byte verification,
bounded retries, resumable checkpoints and writer serialization.

## Repository structure

The repository follows the dataflow:

```text
config/              source campaign settings, storage settings
ingestion/           sources → R2 Landing
  common/            R2 stores, HTTP transport, checkpointed campaign engines
  eurostat/          Eurostat full-file load to Landing (current), API adapter
  world_bank/        World Bank WDI API adapter (paused)
  gus_bdl/           GUS BDL API adapter (paused)
  gus_dbw/           GUS DBW API adapter (paused)
  run_api_campaign.py, run_full_file_campaign.py   paused campaign entrypoints
transformation/      Landing → Bronze → Silver → Gold
  bronze/            Eurostat decode and Bronze catch-up
  promotion/         Bronze → Silver → Gold promotion and optimization
  reconciliation/    archive, lineage and retained-input inspection
semantic/            dimensional model and metrics (not built yet)
serving/             portal, R2 read worker, portal index
  portal/            internal SQL/data portal (React, DuckDB-WASM)
  worker/            Cloudflare Worker serving private R2 reads
checks/              platform checks (R2 connectivity)
docs/                architecture, data lifecycle, development
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
