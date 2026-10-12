# Architecture

zohelo-data is a free data platform: Cloudflare R2 storage, R2 Data Catalog (Apache
Iceberg) with R2 SQL, GitHub Actions for orchestration and an internal portal as the
entry point. No paid subscriptions; anything beyond free tiers needs an owner
decision.

## Dataflow and repository

| Stage | Repository | Storage |
|---|---|---|
| Configuration | `config/platform.yaml`, `requirements.txt`, `.python-version`, `.node-version` | — |
| Source → Landing | `ingestion/sources/<source_system>/` (`source.yaml` + code), shared `ingestion/common/`, retired code `ingestion/legacy/` | `zohelo-landing-prod/01_landing/` |
| Landing → Bronze | `landing_to_bronze/sources/<source_system>/`, shared `landing_to_bronze/common/` (lineage, archive) | Iceberg `bronze`, `05_archive/` |
| Bronze → Silver → Gold, dimensional model, semantic layer | `warehouse/` | Iceberg `silver`, `gold` |
| Data catalog and lineage | generated from `warehouse/`, shown in the portal | — |
| Serving | `portal/` (`app/` portal, `worker/` R2 read API, `publish_index.py`) | `06_control/portal_index/` |
| Orchestration | `.github/workflows/`, `run.py` | `06_control/` run records |
| Ad-hoc | `ops/` | — |

`run.py` is the single entry point: it plans a run and executes one activity.
Activities and workflows are described in [operating.md](operating.md); Landing
rules in [landing.md](landing.md); per-source routes in [sources.md](sources.md).

## Layers

- **Landing** — exact source files plus receipts. Immutable.
- **Archive** — verified copies of Landing files that Bronze has accepted.
- **Bronze** — source-faithful Iceberg tables: structural decoding only (e.g. SDMX
  dimensions, wide periods to long), source codes, flags and raw lineage kept. No
  value repair, no business selection.
- **Silver** — reusable source data: stable types, normalized identifiers and dates,
  duplicate and revision handling, explicit missing / suppressed / invalid statuses
  (never zero), canonical code mappings, provenance kept. Per-dataset rules
  (keys, types, null meanings) plus small shared macros, not one universal
  cleaning script.
- **Gold** — dimensional model: conformed dimensions (geography with levels and
  boundary vintages, period, indicator, unit, source, classifications) and facts
  at their true grain. Statistical observations stay long; facts are consolidated
  only when grain, measure, unit, frequency and population agree.
- **Semantic layer** — metric definitions on Gold.

Silver and Gold are currently pass-through copies of Bronze
(`warehouse/passthrough/`); the SQL project replaces them.

## Tools

- Python for extraction and Landing → Bronze.
- Bronze → Gold and the semantic layer: a SQL project in `warehouse/` — dbt with
  DuckDB and MetricFlow, subject to a trial proving writes of Iceberg tables to the
  R2 catalog. Its generated catalog provides lineage both ways: metric ↔ Gold ↔
  Silver ↔ Bronze table ↔ loader ↔ Landing ↔ source system.
- No orchestrator or distributed compute beyond GitHub Actions without a measured
  need.

## Security

- R2 writer credentials live only in GitHub secrets; the browser never receives
  them. The portal reads private R2 through its Worker with signed short-lived URLs
  and uses Google sign-in for identity only.
