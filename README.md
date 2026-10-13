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
.github/workflows/   Pipeline, Ops · ad-hoc, Dev · branch runs, Deploy portal
run.py               entry point for every Pipeline activity
ops/                 standing operational tasks (dev/ exists only on feature branches)
docs/                architecture, landing, operating, sources
```

## Storage

```text
zohelo-landing-prod     01_landing/  exact source files + receipts
                        05_archive/  verified raw history
zohelo-lakehouse-prod   Iceberg bronze / silver / gold (R2 Data Catalog)
                        _portal_index/  the portal's file index (current version only)
```

See the [source playbook](docs/playbook.md), [architecture](docs/architecture.md), [landing](docs/landing.md),
[operating](docs/operating.md) and [sources](docs/sources.md).
