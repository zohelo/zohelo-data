# Source playbook

The path every source follows, source → Landing → Bronze → Archive → Silver → Gold →
semantic layer → portal. Eurostat and GUS BDL are the reference implementations; the
details live in [landing.md](landing.md), [architecture.md](architecture.md),
[operating.md](operating.md) and [sources.md](sources.md).

## 1. Research and decide extraction

- Read the provider's own documentation: bulk downloads, API, limits, licence, update
  behaviour. Record the route in `docs/sources.md`.
- Extraction order: **bulk feature → bulk workaround (owner decides) → API tested for a
  full load**. Polite access only: documented limits, registered keys are fine, no IP
  rotation, no parallel scraping, contact the provider when the limits are too tight.
- Define the **entity** (the source's own unit: dataset, table, subgroup) and the
  **version** (the source's last update of that entity).
- Write `ingestion/sources/<source>.yaml`: `extraction` (initial, production
  `snapshot|delta`, incremental policy, schedule `null`), tables per layer,
  `archive_policy`, `activities`.

## 2. Landing (`ingestion/to_landing/<source>/` or the shared pattern `ingestion/to_landing/patterns/<kind>.py`)

- `landing.inspect`: source catalogue vs Landing → current / stale / missing; no writes.
- `landing.extract`: missing first, then stale; exact source bytes, receipt after the
  file, never overwrite; resumable; time-budgeted.
- `landing.verify`: every receipt has its file (in Landing or Archive).
- Layout `01_landing/<source>/<snapshot|delta>/<data|metadata>/<entity>/<version>__<ingested>[__part-<n>].<ext>`.
  Several channels for one entity (e.g. website + API) share the entity folder; the
  receipt names the channel; the newest complete version wins.

## 3. Bronze (`ingestion/to_bronze/<source>/`)

- Source-shaped and lean: one row per source record, values as text, no per-row hashes
  or JSON copies of keys; provenance = Landing SHA-256 + version + ingestion time.
- One Parquet file per Landing file (`02_bronze/<source>/<entity table>/<entity>/<sha256>.parquet`),
  partitioned by entity, full column statistics computed client-side. The table's file
  list is the checkpoint; catalog compaction is off (#875), so file names stay the record
  of what is loaded. Rejections go to `bronze.<source>_rejected_files` with the reason.
- Big sources (Eurostat, GUS BDL; design #875) use **entity tables**
  (`ingestion/common/entity_tables.py`): the unit of work is one source entity version (a
  dataset, a subgroup with all parts of one download). Bronze and Silver each hold exactly
  one file per entity; a new version replaces the entity's partition in both, in one commit
  per table (whole files dropped from the file list, nothing rewritten row by row). Silver
  is one SQL file per source in `warehouse/entity/`, applied to each entity right after
  Bronze, with checks (grain, not-null, conflicts) that reject the entity rather than load
  it. A changed Silver SQL redoes every entity: over 10M rows that needs the owner's
  approval (`--approved <issue>`).
- DuckDB-native decoding (no per-cell Python), bounded memory, tested on the largest
  real file before the full load.
- `bronze.verify`: every newest Landing file is in Bronze or rejected with a reason, and no superseded versions remain in Bronze (one version per entity). For entity tables it reads table metadata only and fails below 99% completeness of a layer against the layer below.

## 4. Archive and retention

- `landing.archive` moves Bronze-loaded and superseded files to
  `05_archive/<source>/raw/<sha256>` (`.bin` exact bytes, `.zst` for uncompressed
  formats), verifies the SHA-256, deletes from Landing; the receipt stays.
- `archive.prune` applies `archive_policy.retention`: snapshot sources keep the newest
  version + 90 days; delta sources keep everything. First run reports only.

## 5. Silver, Gold, semantic layer (`warehouse/`, dbt)

- Silver per source: newest version per entity, typed values (casts, decimal comma,
  flags), source codes resolved to labels, channels unified.
- Gold: conformed dimensions shared by all sources (geography with levels and vintages,
  period, indicator, unit, source, classifications) and facts at their true grain.
  Design: [model.md](model.md); dimension definitions: `warehouse/dimensions/`. Map the
  source in each dimension's `used_by` before modelling it.
- Semantic layer: MetricFlow metrics on Gold. Lineage both ways through dbt.

## 6. Serving and operations

- Portal index refresh after writing runs; Ops: `platform_inventory`, `orphan_files`,
  `retry_rejected`, `restore_from_archive`.
- No control folders: state lives in receipts, tables and their file lists; run results
  in annotations and the job summary.

## Definition of done per source

1. Extraction decision recorded; config and docs updated.
2. Landing complete in the agreed layout; `landing.verify` clean.
3. Bronze complete; `bronze.verify` shows nothing unexplained; size checked against the raw.
4. Landing moved to Archive; retention report reviewed and switched to `delete`.
5. Silver and Gold models with tests; metrics where useful.
6. Portal shows the source; `platform_inventory` claims every table and folder.
