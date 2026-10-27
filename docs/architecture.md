# Architecture

zohelo-data is a free data platform: Cloudflare R2 storage, R2 Data Catalog (Apache
Iceberg) with R2 SQL, GitHub Actions for orchestration and an internal portal as the
entry point. No paid subscriptions; anything beyond free tiers needs an owner
decision.

## Dataflow and repository

| Stage | Repository | Storage |
|---|---|---|
| Source configuration | `ingestion/sources/<source_system>.yaml` (extraction strategy, authorized activities) | — |
| Source → Landing | `ingestion/to_landing/<source_system>/`, or the shared pattern named by `extraction.kind` (`ingestion/to_landing/patterns/`) | `zohelo-landing-prod/01_landing/` |
| Landing → Bronze | `ingestion/to_bronze/<source_system>/` | Iceberg `bronze`, `05_archive/` |
| Shared ingestion code | `ingestion/common/` (R2 access, Bronze file helpers, reporting, lineage, archive, retention, scheduler) | — |
| Bronze → Silver → Gold, dimensional model, semantic layer | `warehouse/` (`warehouse.yaml`) | Iceberg `silver`, `gold` |
| Data catalog and lineage | generated from `warehouse/`, shown in the portal | — |
| Serving | `portal/` (`app/` portal, `worker/` R2 read API, `publish_index.py`, `portal.yaml`) | `_portal_index/` |
| Orchestration | `.github/workflows/`, `run.py` | run results in the workflow run (annotations, summary); no state files |
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

Today's Silver and Gold: the dbt project in `warehouse/models/` builds them for NBP,
TERYT, PRG, World Bank WDI, Eurostat and GUS BDL (see [model.md](model.md), "Built").
Sources not yet rebuilt (GUS DBW, dane.gov.pl, GLEIF, MF Biała Lista) are in Bronze only
until modelled; the pass-through copies of the retired `silver_gold.promote` (#835) are
removed with Ops `retire_tables`.

## Tools

- Python for extraction and Landing → Bronze.
- Bronze → Gold and the semantic layer: the dbt project in `warehouse/` (dbt-duckdb
  writing Iceberg tables to the R2 catalog, proven 12 October 2026; MetricFlow for
  metrics). Its generated catalog provides lineage both ways: metric ↔ Gold ↔
  Silver ↔ Bronze table ↔ loader ↔ Landing ↔ source system.
- No orchestrator or distributed compute beyond GitHub Actions without a measured
  need.

## Security

- R2 writer credentials live only in GitHub secrets; the browser never receives
  them. The portal (Cloudflare Pages) reads private R2 through its API (a Pages
  Function with R2 bindings) with signed short-lived URLs; Cloudflare Access signs
  users in (one-time PIN to the allowed emails) before anything loads.
