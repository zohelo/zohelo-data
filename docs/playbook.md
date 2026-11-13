# Source playbook

The one path every source in `ingestion/register.yaml` follows (owner decision O36: every
non-parked source is onboarded, scheduled and modelled through Gold):

**research → Landing → Bronze → Silver → schedule → Gold → published.**

One source (or one small family) = one pull request per step group, built by the project
agents in `.claude/agents/` (`source-researcher`, `source-builder`, `gold-modeller`,
`data-quality-reviewer`) through the stations of [flow.md](flow.md). Copy the newest
onboardings, do not invent: NBP interest rates (#2590), OECD (#2594), news feeds (#2601),
GDELT and Wikimedia (#2603), release calendars (#2607); Gold activation #2585 and #2595.
Details: [landing.md](landing.md), [operating.md](operating.md), [sources.md](sources.md),
[model.md](model.md), [published.md](published.md), [quality.md](quality.md); the backlog
stages are `.github/backlog/stages.yaml`.

## 1. Research and decide extraction (`source-researcher`)

- Read the provider's own documentation: bulk downloads, API, limits, licence, update
  behaviour. The session's proxy refuses most hosts: check reachability with a read-only Dev
  probe on a `test/<topic>` branch.
- Extraction order: **bulk feature → bulk workaround → API tested for a full load → web
  export**. A bulk workaround is Claude's decision when the terms allow it and access stays
  polite; unclear terms go to the owner (AGENTS.md "Data sources"). A web export (a browser
  session asking the site for files) needs an owner decision or a D row in
  [decisions.md](decisions.md) **per source**; today only GUS BDL has one (AGENTS.md
  "Authorizations"). Polite access only: documented limits,
  registered keys are fine, no IP rotation, no parallel scraping, never around bot protection.
- Define the **entity** (the source's own unit: dataset, table, file) and the **version**
  (the source's last update of that entity; download time when the source gives none).
- Fill every field of the register entry. **Licence:** `licence.notes` starts with `Terms:` and
  names the terms URL, the exact attribution sentence, and whether commercial reuse and
  modification are allowed. Terms not stated by the source → `licence.status: unresolved` and,
  after `Terms:`, what the pages read say (the source stays internal; the licence gate hides
  it). Never claim a licence the source does not state. `Terms:` is what lets the backlog sync
  close the review issues of an approved entry (docs/backlog.md §2).
- Outcome: register `status: approved` (route and terms known, or `unresolved` and internal),
  or `parked` with the reason as the first `research.blockers` item (never for a technical
  reason; when the data is fully in a source already loaded, that item starts with
  `Contained in:` and names it, which is what lets the backlog sync close the entry's issues).
  A new key or account, a paid tier or unclear legality is an owner question (AGENTS.md "Ask
  the owner first").

## 2. Landing (`ingestion/to_landing/<source>/` or the shared pattern `ingestion/to_landing/patterns/<kind>.py`)

- Declare the source in `ingestion/sources/<source>.yaml` (key set: operating.md "Source
  declarations"). Fixed URLs or one request per entity → `extraction.kind: bulk_file` and no
  extractor code; files derived from a seed → `patterns/seeded.py`; otherwise a small module.
- `landing.inspect`: source catalogue vs Landing → current / stale / missing; no writes.
- `landing.extract`: missing first, then stale; exact source bytes, receipt after the
  file, never overwrite; resumable; time-budgeted; unchanged bytes (same SHA-256) are skipped.
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
  of what is loaded. Rejections go to `bronze.<source>_rejected_files` with the reason;
  an unknown column, attribute or layout rejects the file, it is never guessed.
- Small sources use the shared loader `ingestion/common/bulk_bronze.py` (newest Landing copy
  of each declared file; `all_versions` for accumulating sources).
- Big sources (Eurostat, GUS BDL; design #875) use **entity tables**
  (`ingestion/common/entity_tables.py`): the unit of work is one source entity version (a
  dataset, a subgroup with all parts of one download). Bronze and Silver each hold exactly
  one file per entity; a new version replaces the entity's partition in both, in one commit
  per table (whole files dropped from the file list, nothing rewritten row by row). Silver
  is one SQL file per source in `warehouse/entity/`, applied to each entity right after
  Bronze, with checks (grain, not-null, conflicts) that reject the entity rather than load
  it. A changed Silver SQL recomputes every entity's Silver from unchanged Bronze.
- DuckDB-native decoding (no per-cell Python), bounded memory, tested on the largest
  real file before the full load.
- `bronze.verify`: every newest Landing file is in Bronze or rejected with a reason, and no superseded versions remain in Bronze (one version per entity). For entity tables it reads table metadata only and fails below 99% completeness of a layer against the layer below.

## 4. Archive and retention

- `landing.archive` moves Bronze-loaded and superseded files to
  `05_archive/<source>/raw/<sha256>` (`.bin` exact bytes, `.zst` for uncompressed
  formats), verifies the SHA-256, deletes from Landing; the receipt stays.
- `archive.prune` applies `archive_policy.retention`: snapshot sources keep the newest
  version + 90 days; delta sources keep everything. First run reports only.

## 5. Silver (`warehouse/models/silver/<source>/`, dbt; entity tables: `warehouse/entity/<table>.sql`)

- Newest version per entity, typed values (casts, decimal comma, flags), source codes
  resolved to labels, channels unified. Source codes are kept as published; conforming to
  shared dimensions happens in Gold.
- `_<source>.yml`: description with grain, history range and the trial run id; a `unique`
  test on the grain; `not_null` on every key column and on `raw_sha256`; `accepted_values`
  on frequency and other closed code lists.
- Also in the same PR, all in the source's own files ([source-registration.md](source-registration.md), D34):
  its Bronze tables as dbt sources in `warehouse/models/bronze/<source>.yml` (not `sources.yml`), the row
  ceilings of its Silver models under `full_rebuild:` in `ingestion/sources/<source>.yaml`, its route in
  `docs/sources/<source>.md`, and the register entry (`source_system`, `status: built`). Nothing to add to
  `lineage.py`, the Pipeline `source` input or `warehouse.yaml`. One row each for `source` and `licence`, in
  `warehouse/seed_rows/source/<source>.csv` and `warehouse/seed_rows/licence/<source>.csv` (the header line of
  `_shared.csv`, then your row; D36).
- News and other text feeds keep only headline, link, guid, publication time and language,
  reduced before Landing; descriptions and article text are never stored, also not in
  Landing or the Archive (O32, O33).

## 6. Schedule

- `extraction.schedule` in the source YAML: cron in UTC, run
  `full dataflow: landing → bronze → silver → gold → portal`, at a polite time after the
  source's release, with the release rhythm in a comment. No faster than the source updates.
- Proof is the scheduler's own runs (D18), never a manual dispatch: daily 3 runs in 7 days,
  weekly 3 in 21 days, monthly 2 in 62 days (D11). Production dispatches from a session are
  refused; do not try.

## 7. Gold (`gold-modeller`; `warehouse/models/gold/`, design in [model.md](model.md))

A statistical source (`kpi: true`) enters Gold through one **build stage** model
`poc_build_<source>.sql` (alias `build_<source>`) in the common observation shape; copy
`poc_build_nbp.sql` (small) or `poc_build_wdi.sql` (geography). Columns, in this order:
`source_system, block_id, block_name, measure_key, measure_name, measure_codes, unit_code,
unit_name, geography_source_code, geography_key, frequency, date_key, period_label,
breakdown_json, breakdown_label_json, sex_code, is_total, value, observation_status_key,
source_version, raw_sha256`. Then:

1. give the model the tag `gold_block` (`config(alias='build_<source>', tags=['gold_block'])`) and register
   it in the source's own macro file `warehouse/macros/sources/<source>.sql`: the block macros and
   `poc_source__<source>()` returning the model and its blocks (template in
   [source-registration.md](source-registration.md)). Do not add lines to `warehouse/macros/poc_blocks.sql`;
   `ops.repo_check` fails on a `poc_build_*` model without the tag or the registration. The Gold PR need not wait for the source's first scheduled
   run: until its Bronze/Silver tables exist the scheduled build declares the block waiting (run summary
   `waiting` and `empty_blocks`, `build_check` note `waiting: …`) and builds the facts from the other
   blocks; the first run that creates the tables builds the block and the facts with it. Only the
   scheduled build may skip a block: a filtered build (Dev trial, `poc_fact_annual+`) fails on a block
   without a table, so select the block too (`poc_build_<source>+`);
2. map its codes in `poc_geography__<source>()` in the same macro file (section "Shared dimensions" below;
   not a CTE in `poc_map_geography.sql`); when the map reads the source's own Silver table for them, read it
   through `poc_optional_ref('<model>', '<column>:<type>, …')` and add the model to the map's config meta
   `optional_parents`: a plain `ref()` there makes the map and every block wait for the source's first Bronze
   load, and the scheduled build then fails (`stalled`, #2903);
3. add the rows of the code lists, each list in the source's own file `warehouse/seed_rows/<list>/<source>.csv`
   (the header line of `_shared.csv`, then the rows; D36, never a new row in `_shared.csv`): units in `unit`
   (with `base_unit_key` and `factor`) and `unit_decimals`, the source's unit codes in `unit_map` (D33), KPIs in
   `kpi` (status `draft` until checked), one `kpi_measure` row per KPI × source measure, and for a KPI of the
   v2 map one `kpi_v2_map` row per v2 key × source (O31: a source is added next to the others, never instead
   of one);
4. list the model and the source's own metrics models in the source YAML `gold:` and give each a measured
   ceiling under `full_rebuild:` there; describe and test them in `warehouse/models/gold/_<source>.yml`.
   `warehouse/warehouse.yaml` and `_gold.yml` take only models shared by several sources.

A reference or register source (`kpi: false`) enters Gold as a dimension or entity table
keyed by official identifiers (TERYT, LEI, NIP, REGON, KRS), never matched by name
(model.md "Three kinds of dimensions").

## 8. Published

`gold.publish` writes the published layer from Gold ([published.md](published.md)), each
source as its own series (`source_key`). v2 (bi.zohelo.com, the owner's review stage) gets
every KPI with Gold series and a ready `kpi_v2_map.csv` row, draft or published (O37); v1
(zohelo.com, public) only KPIs with `status: published` in `kpi.csv`. A KPI is set to
`published` after the owner's review in bi.zohelo.com, with its `poc_metrics_<kpi>.sql`
model, a semantic model, and PL and EN name and definition. The licence gate is in
`gold.publish` (O37): no row of a source whose licence is `unresolved` or missing reaches
v1 or v2; zohelo.com's build applies the same rule again (zohelo#469).

## Quality bar per layer

| Layer | Tests | Idempotent | Incremental policy | Licence |
|---|---|---|---|---|
| Research | `python -m ops.repo_check` passes | – | `extraction.production` (`snapshot`/`delta`) and `incremental_policy` chosen | terms URL + attribution sentence in the register, or `unresolved` |
| Landing | Dev trial into `zz_dev_<topic>`, removed at the end; `landing.verify` green; `landing.inspect` missing 0 | a second `landing.extract` writes nothing (`unchanged`) | `changed_since_last` by default; `append_only` only for delta sources; `full_refresh` only when the source gives no version and the bytes are small | – |
| Bronze | `bronze.verify` green; 0 unexplained rejections; row count checked against the raw file | a second `bronze.load` appends nothing | latest version only (`bronze_latest_only: true`, #822/#845); accumulating sources say so in the YAML | – |
| Silver | grain `unique`, keys `not_null`, closed lists `accepted_values`; 0 duplicate keys and 0 null keys in the trial | a rebuild gives the same row count | `refresh.mode` judged per source (D32): `full` for a small source, `incremental` with `unit` in every layer for a big one, from its size, its update pattern and whether the source offers changed-since; `refresh.decided` cites `D32` plus the source's PR or issue. Separately, every full-rebuild dbt model has a `full_rebuild` row ceiling (#822) | `source.csv` row with `licence_key`; `licence.csv` row with the attribution text |
| Schedule | `PYTHONPATH=. python run.py schedule --check` passes | re-run of the same slot loads nothing new | scheduled runs never rebuild a big table in full | – |
| Gold | `build_check`: rows > 0 for every activated block, `rows_geography_unmatched` 0 or each code explained in the PR; `grain_check` empty; a new test that fails on prod is not merged (D29) | full rebuild from Silver gives the same series and row counts | build stage below 1 M rows with a ceiling; a fact model above 1 M rows is incremental with unique key `series_key, date_key` (D28); full-coverage tables are written per block with the grain checked on every block and the run limits of D35 instead of ceilings | measures carry the source's `licence_key` through `dim_source` |
| Published | published JSON validates against `warehouse/published/schemas/`; the newest value of each new KPI × source is compared with the source's own page in the PR | `gold.publish` rewrites the same files | – | attribution text from `licence.csv`; `unresolved` and `restricted` sources stay internal |

Every PR: `python -m ops.repo_check` and `dbt parse` pass; the body cites the Dev probe and
trial run ids with counts (files, bytes, rows, history range, duplicates, nulls) and says
what is unverified; an independent `data-quality-reviewer` run finds no blocker before
merge (O34). Row ceilings come from a measured count, about 1.5 × the trial's rows.

## What "green" and "in Gold" mean

- **Green** = Landing, Bronze and Silver are complete, scheduled and running in production:
  the first production load finished (`landing.inspect` missing 0, `bronze.verify` green),
  Silver tests pass in the last build, the schedule is merged and the scheduler's own runs
  meet the run count of section 6, big tables build incrementally, the licence is recorded,
  and no `alert` issue is open for the source. A merged PR or a green Dev trial is not green.
- **In Gold** = the source is green **and** (statistical source) its observations are in
  `gold.fact_*` through its build stage with every geography and date key matched or
  explained, its measures are in `dim_measure`, and each KPI it publishes has `kpi_measure`
  (and `kpi_v2_map`) rows; or (reference source) its Gold dimension or entity table is built,
  tested and listed in `warehouse/warehouse.yaml`. **Register status:** the build PR sets `built` (code merged, production not yet proven).
  `silver` (= green) and `gold` (= in Gold) are set only at the Verify station of
  [flow.md](flow.md), by the orchestrator, after the scheduler's own runs prove it.

## Shared dimensions: how a new source conforms

Rules of [model.md](model.md), applied in the Gold build stage. A value that fits no rule is
reported, never forced.

| Dimension | Rule |
|---|---|
| **Time** | `date_key` is a date at day grain = the **first day of the period** (2025 → 2025-01-01, 2025-Q3 → 2025-07-01, 2025-11 → 2025-11-01, a day → that day); it joins `dim_calendar`. `frequency` is a `frequency.csv` key (`A`, `S`, `Q`, `M`, `W`, `D`, `B`); `period_label` keeps the source's period text. Facts exist for A, Q, M and D: a source with S or W periods needs a new fact model first, it is not folded into another frequency. Full-coverage Gold (D35, [gold-coverage.md](gold-coverage.md)) loads S and W with their own frequency into its per-block tables. Annual values are never expanded to days. Stock dates (31 December, 1 January, mid-year) stay in the measure definition, not in `date_key`. |
| **Geography** | `geography_key` is the area, not the source's code: countries ISO 3166-1 alpha-2 (`PL`; Eurostat `EL` → `GR`, `UK` → `GB`; ISO3 and World Bank codes through `dim_geography.iso3` / `wb_code`); Polish units `TERC:<7-digit code>` (województwo `WW00000`, powiat `WWPP000`, gmina `WWPPGGT`); other regions `NUTS:<code>` (a Polish NUTS 2 that equals a voivodeship maps to its `TERC:` key, `poc_nuts_pl`); aggregates keep a scheme prefix (`EUROSTAT:EU27_2020`, `WB:WLD`); no geography → `_Z`; unmatched → `_X` with `geography_source_code` kept. Each source maps every source code in its own `poc_geography__<source>()` ([source-registration.md](source-registration.md)); map by code or an official correspondence table (GUS KTS–TERYT), by name only as a reported fallback. |
| **Units** | Values are stored as published (D33): `unit_code` and `unit_name` as the source publishes them, no conversion or rescaling in Silver or the build stage (a source in thousands stays in thousands, with its own unit). Each unit is a `unit.csv` row with `base_unit_key` and `factor` (exact decimal: base units per unit) and decimals in `unit_decimals.csv`; each source code gets a `unit_map.csv` row (`source_system`, `unit_code` → `unit_key`; a `measure_key_pattern` only where the code's meaning depends on the measure, as WDI "as named"); an unmapped code stays as published and is reported (`dim_unit.mapped`, test `metrics_unit_unmapped`). A KPI is normalised only in the KPI layer (`poc_concept_metrics`): the target unit is the `kpi_measure.csv` `unit_key`, else the KPI's; the metrics `value` is converted by pure scale (same `base_unit_key`, test `metrics_unit_base`), with `value_published` and `unit_published` beside it. Targets: counts in single units, money in the KPI's scale per currency (`mln_eur`, `mln_pln`, `mln_usd`), area in `km2`. A source that publishes a share between 0 and 1 keeps it and maps to `share` (base `pct`, factor 100; Findex); a ratio per 100 people of the reference group maps to `pct` (UN WPP old-age dependency). Currencies, index bases and price bases are never converted; multiplier and currency are part of the unit (`mln_ncu`), price base and seasonal adjustment belong to the measure. |
| **Measure and breakdowns** | `measure_key` = `<source>:<block>:<unit>[:<measure codes>]` (full-coverage Gold, D35: `…@<frequency>` at the end, [gold-coverage.md](gold-coverage.md)); everything else that splits a series goes to `breakdown_json` with source codes as published. Totals (`TOTAL`, `T`, `ogółem`) set `is_total = true`; sex goes to `sex_code` (`T`, `M`, `F`). |
| **Domain** | Every KPI has one `subgroup_key`, the key of a group or subgroup row of `warehouse/seeds/domain.csv` (12 domains: economy, finance, business, labour, population, health, education, society, housing, environment, transport, state; groups and subgroups below them, CSA codes attached). Add a group or subgroup row only when no existing one fits, in the same PR. |
| **Source and status** | `source_system` is the `source.csv` key; `observation_status_key` maps the source's flags to `dim_observation_status` (`ok` when the source has none); rows without a value are not loaded. |

## Serving and operations

- Portal index refresh after writing runs; Ops: `platform_inventory`, `orphan_files`,
  `retry_rejected`, `restore_from_archive`.
- No control folders: state lives in receipts, tables and their file lists; run results
  in annotations and the job summary.

## Definition of done per source

1. Extraction decision and licence recorded; register, config and docs updated.
2. Landing complete in the agreed layout; `landing.verify` clean.
3. Bronze complete; `bronze.verify` shows nothing unexplained; size checked against the raw.
4. Landing moved to Archive; retention report reviewed and switched to `delete` (the switch
   and any `--apply`/`--delete` Ops run only for a Task whose Epic cites an owner decision,
   O8; otherwise it is an owner action).
5. Silver models with tests; scheduled; the source is green.
6. In Gold; KPIs with metrics where the source publishes KPIs.
7. Portal shows the source; `platform_inventory` claims every table and folder.
