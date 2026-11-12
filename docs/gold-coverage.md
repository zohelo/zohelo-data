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
`7` 0.06 M in 13. 99 blocks have more than 1 M rows. (The
probe counts 5,443 distinct file stems where the inventory counts 4,873 blocks; the difference is to
be reconciled in the DBW Task before its build stage is written.)

**Eurostat trials** (samples built into scratch and removed afterwards; Dev runs 43195798983 with 129
datasets, 43251674524 with 71, 43276680619 with 50 on the code of #2831 as reviewed). The last: 50
datasets in 10 minutes on one runner, 220.2 M fact rows, 14.1 M series, 4,261 measures. Over the
runs 6 to 22 M fact rows a minute. In all of them: 0 duplicate `series_key × date_key` over the whole
trial table, 0 null keys, 0 blocks rejected, every Silver row with a value loaded in the blocks
built. The newest Polish values equal the curated facts (unemployment 2026-09 3.4, GDP 2025
923,507.3 M EUR, HICP 2026-09 4.0, population 2025 36,497,495). 2.6 to 2.9% of the rows have no
geography match: mostly Polish NUTS 3 regions (`PL213`, `PL633`, …) of weekly deaths, which the
Eurostat part of `map_geography` does not map yet.
The largest dataset, `AVIA_TF_ACA`
(492 M rows with a value, 3,633 M Silver rows), failed on temp space in runs 43244012890 and
43251674524 and was built in run 43268232691 after the build was split into steps (#2831): 26 minutes
alone, 0 duplicates, every row with a value loaded.

Size in Gold: 1.2 to 1.3 bytes per fact row and 12.9 bytes per series (259 MB and 182 MB in the last
trial). For all of Eurostat that is about 22 GB: 9 GB of facts and 13 GB of series, because annual
series are short. At the trials' speed the first full Eurostat load is 5 to 19 runner-hours, spread
over resumable runs.

## Decisions

### 1. Scope: every block, every geography the source publishes

No geography filter. A filter does not make the problem small: Poland with the national comparators
is still 4,160 M Eurostat rows, and Poland alone (196 M) drops the comparison that the product is
about. One rule for all blocks is also what removes the hand-picking. The cost is storage, not
build time (decision 3): see "Open decisions".

### 2. Layout: four tables per source, one file per block

| Table | One row is | Columns |
|---|---|---|
| `gold.<source>_observations` | series × period | as `gold.fact_*`: `series_key, measure_key, geography_key, date_key, frequency, value, observation_status_key, source_system, block_id, period_label, source_version, raw_sha256` |
| `gold.<source>_series` | one time series | as `gold.dim_series`, plus `source_version, raw_sha256` |
| `gold.<source>_measures` | one measure | as the activated rows of `gold.dim_measure`, plus `unit_name`, unmatched-geography and total-series point counts |
| `gold.<source>_geography` | block × source geography code | the area the code was mapped to (`_X` unmatched, `_Z` none) and its points |

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
  `unit_key` is the source's unit code as published, named in the measures table.
- **Units follow D33** (#2835): facts keep the published value and the published unit; nothing is
  converted or rescaled in these tables. The conformed unit of a source code is looked up in
  `unit_map.csv` (`source_system`, `unit_code`, optional `measure_key_pattern`) through `dim_unit`,
  and a value is rescaled only in the KPI layer, to the KPI's `unit_key`. A code without a
  `unit_map` row stays valid here (unmapped, the source's own name). A `measure_key_pattern` meant
  for rule keys has to allow the `@<frequency>` ending.

The source's build stage (`warehouse/entity/gold_<source>_build.sql`) produces the 21-column
common observation shape of `poc_build_nbp.sql`; the four projections (`gold_observations.sql`,
`gold_series.sql`, `gold_measures.sql`, `gold_geography.sql`) are the same SQL for every source.

### 3. Incremental by block; the grain is checked on every block

A block's `<recipe>` is a digest of what its files were made from: the names of its Silver files
(they carry the Landing SHA-256 and the Silver model), the Gold SQL, and the mapping of the geography
codes **that block holds**. A run compares recipes in the tables' file lists and builds only blocks
whose recipe changed:

| What changed | What is rebuilt | Cost for Eurostat |
|---|---|---|
| A dataset's source version | that dataset | the daily load: see below |
| A row of `gold.map_geography` (a new code, a code newly matched) | the blocks holding that code (`gold.<source>_geography` lists each block's codes and areas; it is read only when the map changed) | the Polish NUTS 3 mapping of open decision 7 touches the regional datasets only |
| The Gold SQL of the source (a rule) | every block of the source | 5 to 19 runner-hours, over several resumable runs; Gold stays readable meanwhile, block by block |
| A block left Silver | removed from Gold, within the removal cap (decision D34) | none |

Runs are time-budgeted and resumable: finished blocks are committed in chunks. The daily load is the
datasets Eurostat updated that day. By the landed versions in `gold.coverage` (Dev run 43276680619;
two releases a day): 9 Nov 34 datasets with 98 M Silver rows, 10 Nov 88 with 643 M, 11 Nov 215 with
1,005 M; on 6 Nov 95 datasets with 6,808 M Silver rows, a day when the largest tables were updated.
At the measured 40 to 120 M Silver rows a minute a usual day is 5 to 30 minutes of one runner and a
heavy day 1 to 3 hours. (Older days are undercounted: a dataset shows only its newest version.)

This is the refresh policy the three sources already declare ("a new dataset version replaces that
dataset in Bronze, Silver and Gold", `refresh: {mode: incremental, unit: …}`, #875, D32).

**Decision D34 (docs/decisions.md), an exception to D28.** D28 expects a Gold fact above 1 M rows
to be a dbt incremental model with unique key `series_key, date_key`. These tables keep that grain
but are written per block by `warehouse/observations.py`, outside dbt, as Silver is. Reason: a dbt
merge by key over 7,000 M rows would rewrite or scan the table on a 16 GB runner; replacing whole
files per block needs no scan and no row-level deletes. `changed.py` and the `full_rebuild` row
ceilings (#822) are for dbt models and do not apply; D34 puts these limits in their place:

- **Run budget:** a run stops starting blocks at its time budget (the Pipeline's `budget-minutes`); a
  block built alone starts only when its estimate fits the time left, otherwise it waits and is
  listed. Memory per block is bounded (4 GB in parallel, 10 GB alone), whatever the block's size.
- **Removal cap:** one run removes at most 5% of the blocks Gold holds (at least 10); above that it
  removes nothing and fails, so a partial Silver listing cannot empty Gold. An empty listing stops
  the run.
- **No partial block, no silent loss:** a block is accepted whole or rejected with a recorded reason;
  a rejected new version keeps the version Gold already holds.

Undo: drop the tables of the source and the activity; the curated facts do not read them.

**Grain.** Every block is checked before it is accepted: one row per `series_key × date_key`
(the grouping of `tests/fact_grain.sql`). `measure_key` starts with the source and the block, so a
duplicate can only occur inside one block and the per-block check covers the whole table at no
extra cost: a table-wide grain test over 7,000 M rows is never run. A block that breaks the grain
is rejected whole and recorded in `gold.observations_rejected` with the reason; Gold keeps the
block's previous version, if it has one. It is retried when its recipe changes.

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

**Suppressed and missing cells** (lesson of 11 Nov: a flagged zero is not a value). The rule-based
build stages carry the rules the curated stages already apply:

- Eurostat: `:` is no value and the row is not loaded; a value with flag `c` is loaded with status
  `confidential`.
- GUS BDL: an empty value is not loaded; a cell with attribute `x` ("filling the item is impossible
  or pointless") is not loaded even when it carries 0; two different published values for one key
  reject the subgroup in Silver already.
- GUS DBW: an empty value is not loaded; a 0 with a missing-value reason (`id_brak_wartosci` other
  than "value present") is not loaded; a value with a confidentiality id (`id_tajnosci`) is loaded
  with status `confidential`.

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

Names are resolved from the source's own dictionaries (Eurostat: `eurostat_dataflows`,
`eurostat_dimensions`, `eurostat_codes`; BDL: `bdl_variables`; DBW: `src_dbw_dictionary`).
`measure_name` = block name, unit name and the names of the measure codes, stored once per measure.
Labels of breakdown codes are **not** stored per row or per series: Eurostat has about 989 M series,
and building the label per series was what exhausted memory on the series-heavy datasets (#2831).
`breakdown_label_json` stays empty for these sources, as in today's `poc_build_eurostat`;
the names are one small table per block (`gold.<source>_codes`: dimension, code, name, total code),
a follow-up Task. A name that changes without a new data version is picked up at the block's next
rebuild.

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

- **data.zohelo.com** reads Iceberg tables: the tables of a source are queryable as they are,
  and `gold.<source>_measures` (one row per measure, small) is the entry point; a block filter reads
  one file.
- **zohelo-bi and zohelo.com** read the published layer, which carries KPIs only. A catalogue of
  non-KPI measures for zohelo-bi is a contract change (published v2, additive): proposal
  `catalog/measures_<source>.parquet` written from `gold.<source>_measures`, and series fetched per
  block from its one file. Not built; see "Open decisions".
- **KPIs** stay curated, with their hand-made measure keys. Rule keys and hand keys differ
  (decision 4), and published v1 files carry `measure_key`, so the curated build stages keep deriving
  their own keys. Once a source is fully in these tables, its `poc_build_<source>` stage may read the
  block files instead of Silver (the hand filters in `poc_src_*.py` can go) while still writing the
  hand keys; moving `kpi_measure.csv` patterns to rule keys is a separate decision per source, taken
  with checklist step 7.

## One adapter per source

A source needs one small rule file: its **build stage**, SQL that turns the source's Silver rows
into the 21-column common observation shape (columns and order of `poc_build_nbp.sql`), for all
rows with a value, with no block list and no sample filter. It states the source's rules and
nothing else: which codes define the measure, which are breakdowns, what the total is, how periods
and geography codes map. Everything after the build stage is shared.

Where the build stage runs: **full coverage of every statistical source goes through the per-block
path** (decision 3, D34), big or small. The curated `fact_*` tables cannot take unfiltered sources:

| Curated table | Rows (12 Nov, run 43157931560) | Ceiling (`warehouse.yaml`) | Headroom |
|---|---|---|---|
| `fact_annual` | 1,279,175 | 1,500,000 (exception #2294, D28) | 220,825 |
| `fact_quarterly` | 141,495 | 250,000 | 108,505 |
| `fact_monthly` | 325,478 | 300,000 | none: already above |
| `fact_daily` | 372,538 | 600,000 | 227,462 |

One unfiltered small source already exceeds that: `unctad_services_trade` alone is 808 k Silver rows,
`oecd_cli` 241 k monthly rows, `nbp_fx_rates` 428 k daily rows. So the `fact_*` tables stay what they
are, the reviewed KPI selections under their #822 ceilings, and no source is "unfiltered" into them.

| Source's Silver | Block | Adapter |
|---|---|---|
| Written per entity (Eurostat, GUS BDL, GUS DBW) | the entity, or a group of entities (DBW: the files of one block) | `warehouse/entity/gold_<source>_*.sql` and one entry in `SOURCES` of `warehouse/observations.py` (Silver table, how entities group into blocks, lookup tables) |
| A dbt table (World Bank WDI 9.0 M rows, UNCTAD, OECD, NBP, MF, KRUS, ECB and the like) | the Silver table (its file list is the recipe), so the source is rebuilt in Gold when its Silver table was rebuilt | the same two things; the driver's support for an unpartitioned Silver table is part of the first such source's Task |

Both land in `gold.<source>_observations`, `_series`, `_measures` and `_geography`. A table-sized
block is bounded by the same run limits (D34); a Silver table too large for one block (above about
500 M rows, the largest block built so far) is split by an entity column first.

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
- **Explained exclusions.** Some rows cannot be placed by rule and are not loaded: a row without a
  value (Gold v1 rule 6), and a period the calendar cannot place (Eurostat frequency `NAP`: 2 datasets,
  916 rows). Each is counted per block with its reason. Coverage is
  **series in Gold ÷ (loaded series − series excluded by a stated rule)**, and the excluded series
  are shown next to it with the reason. A series that only has rows without a value was never a
  loaded series. So 100% is reachable, and it never hides a loss: anything not in Gold is either in
  the denominator or listed as an exclusion.
- 100% is reached when every loaded block is `in gold` and series in Gold equals the denominator; a
  rejected block or a block waiting for a longer run keeps the source below 100% and names why.
  Unmatched geography (`_X`) does not lower coverage (the series is in Gold with its source code)
  and is reported next to it, per code, from `gold.<source>_geography`.

## Rollout

The three big sources first: they hold by far the largest gap (about 8,300 M rows not in Gold).

1. Eurostat: build stage, driver and projections (Task #2792), Dev trial on a sample.
2. Pipeline activity `gold.observations` after `silver_gold.build` in the source's full dataflow,
   completeness in the source's verify, the `gold.coverage` columns above, the portal catalogue.
   Before anything is scheduled the new tables are listed in `warehouse.yaml` `gold` and in
   `NOT_DBT_GOLD` of `ops/repo_check.py`, or `ops/retire_tables.py` would treat them as leftovers. The activity is authorized in
   `ingestion/sources/eurostat.yaml` only after the storage decision.
3. The first production load runs smallest blocks first within each run's budget, so most datasets
   land in the first runs and the 133 largest last.
4. GUS BDL, then GUS DBW: one build stage each; the driver and the projections are shared. DBW needs
   its periodicity ids (`282`, `5`, `2`, `7`) mapped to frequencies from the dictionary first.
5. World Bank WDI (9.0 M Silver rows): the first source whose Silver is a dbt table.

Then every remaining source, one Task and one PR each, with this checklist:

1. Count the source's Silver: tables, rows with a value, loaded series (the metric's denominator).
2. Choose the block (the Silver table, or an entity column when the table is too large for one
   block); record it in the source YAML `refresh`.
3. Write the rule-based build stage for **all** Silver tables and series of the source, without
   block lists or sample filters (the curated `poc_build_<source>` stays for the KPIs); one measure rule per table; periods other than A, S, Q, M, W, D reported, not folded.
4. Map every geography code in `poc_map_geography.sql`; list unmatched codes with the reason.
5. Units: one `unit.csv` row per unit a KPI uses; other units stay as published.
6. Dev trial into `zz_dev_<topic>`: grain by `series_key × date_key` empty, coverage 100% or each
   missing series explained, row counts against Silver, build time and size.
7. Existing `kpi_measure.csv` patterns still match exactly the series they matched before (compare
   `concept_measures` before and after); published KPIs keep their keys and their first source.
8. Register sources: every loaded entity in its Gold dimension or entity table, keyed by the
   official identifier; the metric counts entities.

Known sources (Silver row counts from `warehouse.yaml`
`full_rebuild`; sources still in open PRs are sized by their own Task):

| Source | Silver today | Gold today | Path |
|---|---|---|---|
| World Bank WDI | `wdi_observations` 9.0 M rows, 1,498 indicators | 26 indicators | per block; block = Silver table |
| OECD | `oecd_cli` 241 k rows | 3 indicators | per block; block = Silver table |
| NBP | `nbp_fx_rates` 428 k, `nbp_gold_fixings` 3.5 k, `nbp_policy_rates` 390 | tables A and C, gold; rates in #2737 | per block; block = Silver table |
| UNCTAD, ECB, UN WPP, MF debt and budget, KRUS, NBP balance of payments, World Bank WGI/Findex/ICP | in open PRs | selections | sized in step 1 |
| GLEIF, MF white list, TERYT, PRG | entity tables | partial (#2739, #2740) | entity coverage, step 8 |

## Open decisions

| # | Question | Proposal | Who |
|---|---|---|---|
| 1 | Storage: Silver of the three sources is 13.7 GB, already above R2's free 10 GB. Gold in full adds about 36 GB (Eurostat about 22 GB from the trials; BDL and DBW about 14 GB by the same ratio to Silver, not measured). | Accept the storage for full coverage; otherwise Poland and national comparators only (4,160 M Eurostat rows) or Poland only (196 M) | owner |
| 2 | S and W periods in Gold | Load them with their own frequency in these tables; `fact_semiannual` of #2781 stays the curated answer for KPIs | architect |
| 3 | D28 deviation (per-block files instead of a dbt incremental model) | Decided: D34 | architect, 12 Nov |
| 4 | Non-KPI measures in zohelo-bi (published v2 catalogue) | Additive `catalog/measures_<source>.parquet`; decide after Eurostat is loaded | owner |
| 5 | BDL gmina level (341 M of 458 M rows) | In scope, loaded last | owner, with 1 |
| 6 | Names of breakdown codes | Not per series (decision 6); build `gold.<source>_codes` per block before the portal shows these tables | architect |
| 7 | Eurostat codes of Polish NUTS 3 regions are unmatched (`_X`) | Map them in the Eurostat part of `poc_map_geography.sql` to the podregion keys DBW already uses | architect |
| 8 | The series table is more than half of the bytes (989 M short series for Eurostat) | Keep it for the first load; then decide whether series attributes move onto the fact for annual blocks | architect |
