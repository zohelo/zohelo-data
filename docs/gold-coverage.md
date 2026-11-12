# Gold coverage: every loaded series in Gold

How every source gets into Gold in full, without an activation list (owner direction O36 and the
owner's scope update of 12 Nov 2026). It extends the Gold v1 model of [model.md](model.md): natural
string keys, series grain, sources never unioned.

**100% coverage** means: every loaded Silver series of every source is available in Gold in the
common observation shape (measure, unit, geography, calendar, breakdown, status). KPIs are the
curated layer on top: `kpi.csv`, `kpi_measure.csv`, the `metrics_*` models and the published layer
pick reviewed series out of what Gold holds; nothing here changes them. Register and reference
sources (GLEIF, MF white list, TERYT, PRG) hold entities, not observations: for them coverage means
every loaded entity is in a Gold dimension or entity table, and no observation is invented.

Today Gold is selective for every source: the three big cube sources (below) and also the small
ones (OECD: 3 indicators of a larger Silver, UNCTAD: 3 of 9 tables, WDI: the KPI-mapped series).

Status: design. Eurostat is the first source built this way (`warehouse/observations.py`); it is
not scheduled yet.

## The gap and the size (measured 12 Nov 2026)

The Gold builder's inventory of 12 Nov: 7,571 of 8,170 loaded Eurostat datasets, 2,231 of 2,254 BDL
subgroups and 4,849 of 4,873 DBW blocks have no Gold block. Blocks are activated by hand
(`poc_*_blocks()` macros, filters in `poc_src_*.py`), which does not scale to about 15,000 blocks.
Read-only Dev run 43157931560 scanned every Silver file of the three sources (all files, no
sampling):

| Source | Blocks | Silver rows | Rows with a value | Series (approx.) | Silver size |
|---|---|---|---|---|---|
| Eurostat | 8,170 datasets | 19,276 M | 6,950 M | 989 M | 8.3 GB |
| GUS BDL | 2,254 subgroups | 654 M | 458 M | 76 M | 2.8 GB |
| GUS DBW | 5,443 file stems in 11,336 files | 889 M | 889 M | not counted per block | 2.6 GB |
| Gold today (`fact_*`) | activated by hand | – | 2.1 M | 65,268 | 19 MB |

Rows without a value are not loaded (Gold v1 rule 6), so full coverage is about 8,300 M fact rows,
4,000 times today's Gold.

Eurostat rows with a value, by geography of the `geo` dimension:

| Geography | Rows |
|---|---|
| Poland (`PL`) | 113 M |
| Polish regions (`PL…`) | 83 M |
| EU, euro area and other aggregates | 239 M |
| Other EU countries | 3,210 M |
| Other countries | 515 M |
| Other regions (NUTS outside Poland) | 1,324 M |
| No `geo` dimension (air transport by airport pair, some migration tables) | 1,465 M |

By frequency: annual 4,234 M rows in 7,410 datasets, monthly 1,408 M in 364, quarterly 1,068 M in
545, weekly 226 M in 14, daily 13 M in 5, half-yearly 0.4 M in 16; 98 datasets hold more than one
frequency. 771 datasets have more than 1 M rows with a value and 133 more than 10 M; the largest
is `AVIA_TF_ACA` with 492 M (3,633 M Silver rows). Annual series are short and many: about 4.5
points per series.

BDL rows with a value by territorial level: Poland to voivodeship 12 M, to podregion 34 M, to powiat
116 M, gminas another 341 M. By period: annual 407 M, quarterly 15 M, monthly 10 M, cumulative
periods (`1-3 kwartały`, `półrocze`) 25 M. 106 subgroups have more than 1 M rows.

DBW rows by the periodicity id that ends the block id: `4` (annual) 730 M in 3,191 blocks, `282`
96 M in 570, `5` 43 M in 108, `1` (monthly) 14 M in 554, `2` 3 M in 376, `3` (quarterly) 2 M in 631,
`7` 0.06 M in 13. 99 blocks have more than 1 M rows.

**Eurostat trial** (Dev run 43195798983, 129 sampled datasets into scratch, removed afterwards):
128 built in 28 minutes on one runner with three workers, 253.8 M fact rows, 15.2 M series, 4,471
measures, about 9 M rows a minute; 0 duplicate `series_key × date_key` over the whole trial table,
0 null keys, 0 blocks rejected, every Silver row with a value loaded. One dataset (`NAIO_10_FCP_U3`,
44 M rows) ran out of its 4 GB and was not loaded; such a block is now tried again alone with the
full memory. The newest Polish values equal the curated facts (unemployment 2026-09 3.4, GDP 2025
923,507.3 M EUR, HICP 2026-09 4.0, population 2025 36,497,495). 2.5% of the rows (142 codes) have
no geography match: Polish NUTS 3 regions (`PL213`, `PL633`, …) of weekly deaths, which the Eurostat
part of `map_geography` does not map yet.

Size in Gold: 1.4 bytes per fact row and 14.7 bytes per series (362 MB and 223 MB for the trial,
against 287 MB of Silver for the same datasets). For all of Eurostat that is 17 to 24 GB (by the
ratio to Silver, and by rows and series), more than half of it the series table, because annual
series are short. At the trial's speed the first full Eurostat load is about 13 runner-hours,
spread over resumable runs.

## Decisions

### 1. Scope: every block, every geography the source publishes

No geography filter. A filter does not make the problem small: Poland with the national comparators
is still 4,160 M Eurostat rows, and Poland alone (196 M) drops the comparison that the product is
about. One rule for all blocks is also what removes the hand-picking. The cost is storage, not
build time (decision 3): see "Open decisions".

### 2. Layout: three tables per source, one file per block

| Table | One row is | Columns |
|---|---|---|
| `gold.<source>_observations` | series × period | as `gold.fact_*`: `series_key, measure_key, geography_key, date_key, frequency, value, observation_status_key, source_system, block_id, period_label, source_version, raw_sha256` |
| `gold.<source>_series` | one time series | as `gold.dim_series`, plus `source_version, raw_sha256` |
| `gold.<source>_measures` | one measure | as the activated rows of `gold.dim_measure`, plus `unit_name`, unmatched-geography and total-series point counts |

Each table is partitioned by `block_id` and holds exactly one Parquet file per block
(`04_gold/<source>/<table>/<block>/<recipe>.parquet`), the layout Silver already uses for these
sources (design #875). Reasons:

- **Per source, not one table:** sources are drilled across through the conformed keys, never
  unioned (model.md); a query for one block reads one file.
- **Not per frequency:** `frequency` is a column and part of the measure key (decision 5). A table
  per frequency would split one block over several tables and several commits.
- **Not per domain:** a block's domain is catalogue data (`dim_dataset`), and it can change without
  the data changing.
- **Dimensions are shared, not copied:** `geography_key`, `date_key`, `observation_status_key` and
  `sex_key` join the existing `dim_geography`, `dim_calendar`, `dim_observation_status` and `sex`.
  `unit_key` is the source's unit code as published, named in the measures table (`dim_unit` is
  built from the curated blocks only).

The source's build stage (`warehouse/entity/gold_<source>_build.sql`) produces the 21-column
common observation shape of `poc_build_nbp.sql`; the three projections
(`gold_observations.sql`, `gold_series.sql`, `gold_measures.sql`) are the same SQL for every source.

### 3. Incremental by block; the grain is checked on every block

A block's `<recipe>` is a digest of what its files were made from: the names of its Silver files
(they carry the Landing SHA-256 and the Silver model), the Gold SQL, and the source's rows of
`gold.map_geography`. A run compares recipes in the tables' file lists and builds only blocks whose
recipe changed. So a daily run rebuilds only the datasets with a new source version; a changed rule
or geography map rebuilds every block of that source; a block that left Silver leaves Gold. Runs
are time-budgeted and resumable: finished blocks are committed in chunks.

This is the refresh policy the three sources already declare ("a new dataset version replaces that
dataset in Bronze, Silver and Gold", `refresh: {mode: incremental, unit: …}`, #875, D32).

**Deviation from D28, proposed here.** D28 expects a Gold fact above 1 M rows to be a dbt
incremental model with unique key `series_key, date_key`. These tables keep that grain but are
written per block by `warehouse/observations.py`, outside dbt, as Silver is. Reason: a dbt merge by
key over 7,000 M rows would rewrite or scan the table on a 16 GB runner; replacing whole files per
block needs no scan and no row-level deletes. `changed.py` and the `full_rebuild` ceilings do not
apply to them (they are not dbt models). Undo: drop the three tables and the activity; the curated
facts do not read them.

**Grain.** Every block is checked before it is accepted: one row per `series_key × date_key`
(the grouping of `tests/fact_grain.sql`). `measure_key` starts with the source and the block, so a
duplicate can only occur inside one block and the per-block check covers the whole table at no
extra cost: a table-wide grain test over 7,000 M rows is never run. A block that breaks the grain
is rejected whole, recorded in `gold.observations_rejected` with the reason and removed from Gold;
it is retried when its recipe changes.

### 4. Measure keys by rule

`measure_key = <source>:<block>:<unit>[:<measure codes>]@<frequency>`.

- **Eurostat.** Measure codes are the codes of the measure-defining dimensions, in the dataset's
  dimension order: the indicator-like dimensions (`indic_*`, `na_item`, `bop_item`, `nrg_bal`,
  `stk_flow`, …; one pattern in `gold_eurostat_build.sql`) and `s_adj`, because price base and
  seasonal adjustment belong to the measure (playbook). Every other dimension is a breakdown and
  goes to `breakdown_json` with the codes as published.
- **GUS BDL.** The rule `poc_build_bdl.sql` already uses: label positions where no variable of the
  subgroup says `ogółem` name the measure; a period label becomes the frequency.
- **GUS DBW.** The rule of `poc_build_dbw.sql`: measure = indicator × presentation; the territorial
  dimension is the one whose positions match areas most often; the other dimensions are breakdowns.

The rule is code: changing it changes every recipe of the source and all its blocks are rebuilt.
Rule keys differ from today's hand-made keys where the hand list chose other measure dimensions
(`eurostat:UNE_RT_M:PC_ACT:TOTAL` by hand, `eurostat:UNE_RT_M:PC_ACT:SA@M` by rule). The curated
layer keeps its keys; nothing published is renamed.

**Totals.** `is_total` is true when every breakdown dimension of the series holds its total code:
`TOTAL`, `T` or `_T` when the block has one for that dimension, or the only code when the dimension
has one code. A dimension with several codes and none of these has no total series. Measured on 600
datasets: 182 have a total by this rule on every open dimension, and 781 of 1,430 open dimensions
have a total code; the misses are mostly `age` (`Y15-74` is the total), `nace_r2` (`B-S`) and
similar. So `is_total` is a convenience, never a filter KPIs depend on: a KPI names its codes in
`kpi_measure.csv`. Follow-up: read the root of each code list's hierarchy
(`eurostat_codes.parent_code`) as the total.

### 5. Frequency is part of the measure key

`series_key` (`measure_key|geography_key|breakdown_json`) has no frequency, and `date_key` is the
first day of the period, so the annual and the first-quarter row of one measure share a key
(lesson of 11 Nov). 98 Eurostat datasets hold several frequencies. The frequency is therefore
always the last part of the key (`@A`, `@Q`, `@M`), not only where a block needs it today: a key
must not change when a dataset gains a second frequency.

Half-yearly (16 datasets, 0.4 M rows) and weekly (14 datasets, 226 M rows) periods are loaded with
their own frequency (`S`: 1 January and 1 July; `W`: the ISO week's Monday) and are not folded into
another frequency. The playbook asks for a decision before S and W enter the `fact_*` tables; these
tables are not split by frequency, so nothing is folded, but see "Open decisions". Periods the
calendar cannot place (2 datasets, 916 rows with frequency `NAP`) are not loaded and are counted.

### 6. Labels from the Silver dictionaries

Names are resolved in the build stage from the source's own dictionaries (Eurostat:
`eurostat_dataflows`, `eurostat_dimensions`, `eurostat_codes`; BDL: `bdl_variables`; DBW:
`src_dbw_dictionary`): `measure_name` = block name, unit name and the names of the measure codes;
`breakdown_label_json` holds the names of the breakdown codes per series. Labels are stored on the
measure and the series, never on the fact. A label that changes without a new data version is
picked up at the block's next rebuild.

### 7. Quality gates

| Gate | Where | Cost |
|---|---|---|
| Grain `series_key × date_key` | every block, before upload | one pass over the block |
| Keys not null (`measure_key, geography_key, date_key, frequency, value`) | every block | same pass |
| Series and measures unique; points add up to the fact rows | every block | same pass |
| Completeness Gold against Silver (blocks holding the expected recipe, rejected, to build) | `observations.completeness`, file lists only | no data read |
| Unmatched geography (`_X`) | `points_geography_unmatched` per measure; the source code stays in the key | read of the small measures table |
| Row count against Silver | the run's summary: Silver rows and Gold rows per run | none |

dbt tests do not run on these tables. A new check that would reject blocks already in Gold is first
run as a Dev trial with the failing blocks annotated (D29).

### 8. What the portals can query

- **data.zohelo.com** reads Iceberg tables: the three tables per source are queryable as they are,
  and `gold.<source>_measures` (one row per measure, small) is the entry point; a block filter reads
  one file.
- **zohelo-bi and zohelo.com** read the published layer, which carries KPIs only. A catalogue of
  non-KPI measures for zohelo-bi is a contract change (published v2, additive): proposal
  `catalog/measures_<source>.parquet` written from `gold.<source>_measures`, and series fetched per
  block from its one file. Not built; see "Open decisions".
- **KPIs** stay curated. Once a source is fully in these tables, its `poc_build_<source>` stage can
  read the block files instead of Silver and the hand filters in `poc_src_*.py` can go; that is a
  later Task per source and changes no key.

## One adapter per source

A source needs one small rule file: its **build stage**, SQL that turns the source's Silver rows
into the 21-column common observation shape (columns and order of `poc_build_nbp.sql`), for all
rows with a value, with no block list and no sample filter. It states the source's rules and
nothing else: which codes define the measure, which are breakdowns, what the total is, how periods
and geography codes map. Everything after the build stage is shared.

Where the build stage runs follows the size rules (#822, D28, D32):

| Source size in Gold | Build stage | Runs in | Lands in |
|---|---|---|---|
| Above 1 M rows, or Silver written per entity (Eurostat, GUS BDL, GUS DBW, World Bank WDI at 9 M rows) | `warehouse/entity/gold_<source>_build.sql`, plus one entry in `SOURCES` of `warehouse/observations.py` (Silver table, how entities group into blocks, lookup tables) | per block, incremental by recipe (decision 3) | `gold.<source>_observations`, `_series`, `_measures` |
| Below 1 M rows (NBP, OECD, UNCTAD, MF, KRUS, ECB and the like) | `warehouse/models/gold/poc_build_<source>.sql`, unfiltered | the dbt build, rebuilt in full under its `full_rebuild` ceiling | `gold.fact_*`, `dim_series`, `dim_measure` through `poc_observations()` |

A small source that grows past its ceiling moves to the first row: its build SQL stays the same
apart from the Jinja refs. The curated `fact_*` tables stay below their ceilings because the big
sources add no rows to them.

## Coverage metric

Per source, one number the portal can show: **series in Gold ÷ loaded series**, where a loaded
series is a distinct series identity of the source's Silver with at least one value (Eurostat:
frequency × unit × geography × breakdown codes; BDL: unit × variable; DBW: block × presentation ×
positions; a flat Silver table: its key columns without the period).

- `gold.coverage` (one row per block a source publishes, written by each source's verify from file
  lists and catalogue metadata) gains `silver_series`, `gold_series`, `gold_rows` and `gold_status`
  (`in gold` / `rejected` / `to build` / `not modelled`). Per-block sources fill them from the
  block's build (the counts are a by-product of the pass that writes the files); dbt-built sources
  from `build_check`.
- `gold.build_check` gains a per-source summary row: blocks loaded, blocks in Gold, loaded series,
  series in Gold, coverage %, rows not loaded with the reason (no value, period not placeable,
  block rejected).
- 100% is reached when every loaded block is `in gold` and series in Gold equals loaded series; a
  rejected block or an unplaced period keeps the source below 100% and names why. Unmatched
  geography (`_X`) does not lower coverage (the series is in Gold with its source code) and is
  reported next to it.

## Rollout

The three big sources first: they hold by far the largest gap (about 8,300 M rows not in Gold).

1. Eurostat: build stage, driver and projections (Task #2792), Dev trial on a sample.
2. Pipeline activity `gold.observations` after `silver_gold.build` in the source's full dataflow,
   completeness in the source's verify, the `gold.coverage` columns above, the tables in
   `warehouse.yaml` `gold`, the portal catalogue. The activity is authorized in
   `ingestion/sources/eurostat.yaml` only after the storage decision.
3. The first production load runs smallest blocks first within each run's budget, so most datasets
   land in the first runs and the 133 largest last.
4. GUS BDL, then GUS DBW: one build stage each; the driver and the projections are shared. DBW needs
   its periodicity ids (`282`, `5`, `2`, `7`) mapped to frequencies from the dictionary first.
5. World Bank WDI (9.0 M Silver rows, block = indicator): the fourth per-block source.

Then every remaining source, one Task and one PR each, with this checklist:

1. Count the source's Silver: tables, rows with a value, loaded series (the metric's denominator).
2. Choose the row of the adapter table by the measured size; record it in the source YAML `refresh`.
3. Write the build stage for **all** Silver tables and series of the source: remove block lists and
   sample filters; one measure rule per table; periods other than A, S, Q, M, W, D reported, not folded.
4. Map every geography code in `poc_map_geography.sql`; list unmatched codes with the reason.
5. Units: one `unit.csv` row per unit a KPI uses; other units stay as published.
6. Dev trial into `zz_dev_<topic>`: grain by `series_key × date_key` empty, coverage 100% or each
   missing series explained, row counts against Silver, ceilings from the measured rows.
7. Existing `kpi_measure.csv` patterns still match exactly the series they matched before (compare
   `concept_measures` before and after); published KPIs keep their keys and their first source.
8. Register sources: every loaded entity in its Gold dimension or entity table, keyed by the
   official identifier; the metric counts entities.

Known sources and their row of the adapter table (Silver row counts from `warehouse.yaml`
`full_rebuild`; sources still in open PRs are sized by their own Task):

| Source | Silver today | Gold today | Path |
|---|---|---|---|
| World Bank WDI | `wdi_observations` 9.0 M rows, 1,498 indicators | 26 indicators | per block |
| OECD | `oecd_cli` 241 k rows | 3 indicators | dbt, unfiltered |
| NBP | `nbp_fx_rates` 428 k, `nbp_gold_fixings` 3.5 k, `nbp_policy_rates` 390 | tables A and C, gold; rates in #2737 | dbt, unfiltered |
| UNCTAD, ECB, UN WPP, MF debt and budget, KRUS, NBP balance of payments, World Bank WGI/Findex/ICP | in open PRs | selections | sized in step 1 |
| GLEIF, MF white list, TERYT, PRG | entity tables | partial (#2739, #2740) | entity coverage, step 8 |

## Open decisions

| # | Question | Proposal | Who |
|---|---|---|---|
| 1 | Storage: Silver of the three sources is 13.7 GB, already above R2's free 10 GB. Gold in full adds about twice that (Eurostat 17 to 24 GB measured by the trial; BDL and DBW about 11 GB by the same ratio). | Accept the storage for full coverage; otherwise Poland and national comparators only (4,160 M Eurostat rows) or Poland only (196 M) | owner |
| 2 | S and W periods in Gold | Load them with their own frequency in these tables; `fact_semiannual` of #2781 stays the curated answer for KPIs | architect |
| 3 | D28 deviation (per-block files instead of a dbt incremental model) | As decision 3 | architect |
| 4 | Non-KPI measures in zohelo-bi (published v2 catalogue) | Additive `catalog/measures_<source>.parquet`; decide after Eurostat is loaded | owner |
| 5 | BDL gmina level (341 M of 458 M rows) | In scope, loaded last | owner, with 1 |
| 6 | The series table is larger than the fact (short annual series, labels repeated per series) | Before the production load, move the breakdown labels to one small table per block (dimension, code, name) and measure again | architect |
| 7 | Eurostat codes of Polish NUTS 3 regions are unmatched (`_X`) | Map them in the Eurostat part of `poc_map_geography.sql` to the podregion keys DBW already uses | architect |
