# zohelo-data

Free data platform for zohelo.com: source files land in Cloudflare R2, are loaded
into Apache Iceberg Bronze / Silver / Gold in R2 Data Catalog, and are served
through the internal portal at https://data.zohelo.com/.

## Repository

In dataflow order:

```text
ingestion/           source → Landing → Bronze (Python)
  sources/           <source>.yaml: extraction strategy, authorized activities
  to_landing/        <source>/: extract to Landing
  to_bronze/         <source>/: load Landing into Bronze
  common/            reporting, lineage, archive
  legacy/            retired code, kept until each source is rebuilt
warehouse/           Bronze → Silver → Gold, dimensional model, semantic layer; warehouse.yaml
portal/              serving: app/ (portal), worker/ (R2 read API), portal index; portal.yaml
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
