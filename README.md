# zohelo-data

Free data platform for zohelo.com: source files land in Cloudflare R2, are loaded
into Apache Iceberg Bronze / Silver / Gold in R2 Data Catalog, and are served
through the internal portal at https://data.zohelo.com/.

## Repository

In dataflow order:

```text
config/              platform configuration (storage contract, platform activities)
ingestion/           source → Landing (Python)
  sources/<source>/  source.yaml (extraction strategy, authorized activities) + code
  common/            shared ingestion code
  legacy/            retired code, kept until each source is rebuilt
landing_to_bronze/   Landing → Bronze (Python)
  sources/<source>/  Bronze loader per source
  common/            lineage and archive
warehouse/           Bronze → Silver → Gold, dimensional model, semantic layer (SQL project)
portal/              serving: app/ (portal), worker/ (R2 read API), portal index
.github/workflows/   orchestration: Pipeline, Ops · ad-hoc, Deploy portal
run.py               entry point for every Pipeline activity
ops/                 ad-hoc scripts, removed in the PR that finishes their task
docs/                architecture, landing, operating, sources
```

## Storage

```text
zohelo-landing-prod     01_landing/  exact source files + receipts
                        05_archive/  verified raw history
zohelo-lakehouse-prod   Iceberg bronze / silver / gold (R2 Data Catalog)
                        06_control/  run records, checkpoints, portal index
```

See [architecture](docs/architecture.md), [landing](docs/landing.md),
[operating](docs/operating.md) and [sources](docs/sources.md).
