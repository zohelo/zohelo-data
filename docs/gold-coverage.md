# Gold coverage: every loaded series in Gold

How every source gets into Gold in full, without an activation list (owner direction O36). It
extends the Gold v1 model of [model.md](model.md): natural string keys, series grain, sources
drilled across through conformed dimensions and never unioned. The storage layout and the run limits are
decision D35 in [decisions.md](decisions.md); the scope is O43; units follow D33.

**100% coverage** means: every loaded Silver series of every source is available in Gold in the
common observation shape (measure, unit, geography, calendar, breakdown, status). KPIs are the
curated layer on top: `kpi.csv`, `kpi_measure.csv`, the `poc_build_<source>` stages, the `fact_*`
tables, the `metrics_*` models and the published layer pick reviewed series; nothing here changes
them. Register and reference sources (GLEIF, MF white list, TERYT, PRG) hold entities, not
observations: for them coverage means every loaded entity is in a Gold dimension or entity table,
keyed by its official identifier, and no observation is invented.

## Sizes the design rests on

Hand activation (`poc_*_blocks()` macros, filters in `poc_src_*.py`) does not scale to the about
15,000 blocks of the three big cube sources. In round numbers (rows with a value, the rows Gold
loads):

| Source | Blocks | Rows with a value | Series | Silver size |
|---|---|---|---|---|
| Eurostat | about 8,200 datasets | about 7,000 M | about 1,000 M | about 8 GB |
| GUS BDL | about 2,250 subgroups | about 460 M (340 M of it gminas) | about 76 M | about 3 GB |
| GUS DBW | about 5,000 blocks | about 890 M | not counted | about 3 GB |

Eurostat by geography: Poland and its regions about 200 M rows; with the EU aggregates and all
countries about 4,200 M; every geography about 7,000 M. Full Eurostat in Gold is about 22 GB, more
than half of it the series table (annual series are short). The curated `fact_*` tables hold about
2 M rows.

## Decisions

### 1. Scope: every block, every geography the source publishes (decided, O43)

Every loaded series of every source, all geographies, goes to Gold; the storage this needs is
accepted. No geography filter: one rule for all blocks is what removes the hand-picking.

**Load order** (O43): Poland and its regions first, then countries and EU aggregates, then the rest.
A block is built whole, so the order is an order of blocks, decided from the source's structure
metadata (a dataset's geography code list), not from its data: first the blocks of Polish sources
and the blocks that hold Polish regions, then the blocks that hold Poland, other countries or the EU
aggregates, last the blocks with neither (other countries' regions only, airport pairs). Within a
group, smaller blocks first.

### 2. One path for every statistical source: per block

Full coverage of every statistical source, big or small, is built per block (D35). The curated
`fact_*` tables stay the reviewed KPI selections under their #822 ceilings; no source is loaded
into them unfiltered. One path keeps one set of rules (whole-block accept or reject, the grain
checked per block, the run limits of D35) instead of a size threshold that a source crosses as it
grows.

A **block** is the unit of rebuild: a Silver entity (Eurostat dataset, BDL subgroup), a group of
entities (DBW: the files of one block), or, for a source whose Silver is a dbt table, the table
itself; a table too large for one block is split by an entity column first.

### 3. Layout: four tables per source, one file per block

| Table | One row is | Columns |
|---|---|---|
| `gold.<source>_observations` | series × period | as `gold.fact_*`: `series_key, measure_key, geography_key, date_key, frequency, value, observation_status_key, source_system, block_id, period_label, source_version, raw_sha256` |
| `gold.<source>_series` | one time series | as `gold.dim_series`, plus `source_version, raw_sha256` |
| `gold.<source>_measures` | one measure | as the activated rows of `gold.dim_measure`, plus `unit_name`, unmatched-geography and total-series point counts |
| `gold.<source>_geography` | block × source geography code | the area the code was mapped to (`_X` unmatched, `_Z` none) and its points |

Each table is partitioned by `block_id` and holds exactly one Parquet file per block
(`04_gold/<source>/<table>/<block>/<recipe>.parquet`), the layout Silver uses for the big sources
(design #875).

**Reads are the cost, not storage** (R2 bills read requests, Class B, far above stored bytes). A run
decides what to build from Iceberg metadata only (file lists and table properties), never by
listing or opening data files; it reads a block's Silver and Gold files only when it rebuilds that
block, reads the source's small lookups once per run, and reads `gold.<source>_geography` only when
the geography map changed. One file per block and table keeps a rebuild or a block query at one
object per table. A source may have a block's Silver files downloaded once per rebuild
(`local_input`, GUS BDL), so the steps of its build stage do not each read them from storage.

- **Per source, not one table:** sources are drilled across, never unioned; a query for one block
  reads one file.
- **Not per frequency:** `frequency` is a column and part of the measure key (decision 6).
- **Not per domain:** a block's domain is catalogue data (`dim_dataset`) and changes without the data.
- **Dimensions are shared, not copied:** `geography_key`, `date_key`, `observation_status_key` and
  `sex_key` join `dim_geography`, `dim_calendar`, `dim_observation_status` and `sex`.
- **Units (D33):** facts keep the published value and unit; nothing is converted in these tables.
  The conformed unit of a source code comes from `unit_map.csv` through `dim_unit`; a value is
  rescaled only in the KPI layer. A `measure_key_pattern` meant for rule keys allows the
  `@<frequency>` ending.

A source's build stage turns its Silver rows into the 21-column common observation shape of
`poc_build_nbp.sql`; four projections, the same SQL for every source, make the four tables
(`warehouse/entity/gold_*.sql` and the builder `warehouse/observations.py`, added with #2831).

### 4. Incremental by block; the grain is checked on every block

A block's recipe is a digest of what its files were made from: the names of its Silver files (they
carry the Landing SHA-256 and the Silver model), the Gold SQL of the source, and the mapping of the
geography codes **that block holds**. A run builds only blocks whose recipe changed:

| What changed | What is rebuilt |
|---|---|
| A block's source version | that block (the daily load) |
| A row of `gold.map_geography` | the blocks holding that code, found in `gold.<source>_geography`, which is read only when the map changed |
| The Gold SQL of the source (a rule) | every block of the source, over several resumable runs; Gold stays readable block by block meanwhile |
| A block left Silver | it is removed from Gold, within the removal cap of D35 |

This is the refresh policy the big sources already declare (`refresh: {mode: incremental, unit: …}`,
#875, D32). The limits that replace the #822 row ceilings for these tables (run budget, memory per
block, removal cap, whole-block accept or reject) are D35.

**Grain.** Every block is checked before it is accepted: one row per `series_key × date_key`, the
grouping of `tests/fact_grain.sql`. `measure_key` starts with the source and the block, so a
duplicate can only occur inside one block, and the per-block check covers the whole table: no
table-wide grain test runs. A block that breaks the grain is rejected whole and recorded in
`gold.observations_rejected` with the reason; Gold keeps the block's previous version, if it has
one. It is retried when its recipe changes.

### 5. Measure keys by rule

`measure_key = <source>:<block>:<unit>[:<measure codes>]@<frequency>`.

- **Eurostat.** Measure codes are the codes of the measure-defining dimensions, in the dataset's
  dimension order: the indicator-like dimensions (`indic_*`, `na_item`, `bop_item`, `nrg_bal`,
  `stk_flow`, …; one pattern in the build stage) and `s_adj`, because seasonal adjustment belongs to
  the measure. Every other dimension is a breakdown, codes as published in `breakdown_json`.
- **GUS BDL.** The rule of `poc_build_bdl.sql`, read from the subgroup's own Silver file
  (`gold_gus_bdl_labels.sql`): label positions where no variable of the subgroup says `ogółem` name
  the measure; a first label that names a month, a quarter or `rok` becomes the frequency and the
  date. A cumulative period (`1-3 kwartały`, `styczeń-maj`, `półrocze`) cannot be placed and is
  counted as not loaded. The curated stage's hand-made exceptions (measure positions of two
  subgroups, chained price bases, year and level filters) are not part of the rule.
- **GUS DBW.** The rule of `poc_build_dbw.sql`: measure = indicator × presentation; the territorial
  dimension is the one whose positions match areas most often; the other dimensions are breakdowns.

The rule is code: changing it changes every recipe of the source. Rule keys differ from the curated
hand-made keys (`eurostat:UNE_RT_M:PC_ACT:TOTAL` curated, `eurostat:UNE_RT_M:PC_ACT:SA@M` by rule).
The curated build stages keep writing their hand keys, so nothing published is renamed. Moving a
`kpi_measure.csv` pattern to a rule key is a separate decision per KPI (rule 7 of the adapter rules).

**Totals.** `is_total` is true when every breakdown dimension of the series holds its total code:
`TOTAL`, `T` or `_T` when the block has one for that dimension, or the only code when the dimension
has one code. On a sample about half of the open Eurostat dimensions have such a code; the misses
are mostly ranges that are the total in effect (`age` `Y15-74`, `nace_r2` `B-S`). So `is_total` is a
convenience, never a filter a KPI depends on: a KPI names its codes in `kpi_measure.csv`.

**Cells that are not plain observed values** (lesson of 11 Nov: a flagged zero is not a value). The
rule-based build stages apply the rules the curated stages already apply:

- Eurostat: `:` is no value and the row is not loaded. A value flagged `c` is loaded with status
  `confidential`; a value flagged `f` with status `forecast`. A row keeps its status in every table
  it reaches; a reader that wants observed values only filters on `observation_status_key`, and a
  forecast is never presented as an observation.
- GUS BDL: an empty value is not loaded; a 0 under a no-value sign is not loaded (#2852: website
  signs `x`, `X`, `n`; API attribute ids 4, 14, 50, 91), a number other than 0 under such a sign is;
  two different published values for one key reject the subgroup in Silver already. A value for a
  year after the year of the subgroup's version, and every row of a subgroup named a projection
  (`prognoza`), has status `forecast`.
- GUS DBW: an empty value is not loaded; a 0 with a missing-value reason (`id_brak_wartosci` other
  than "value present") is not loaded; a value with a confidentiality id (`id_tajnosci`) is loaded
  with status `confidential`.

**GUS BDL geography by code.** A BDL unit id carries the TERYT code of its area, so Poland, a
voivodeship, a powiat and a gmina are mapped by that code through the map's rows of the 7-digit
TERYT codes (`gold_gus_bdl_map.sql`, run once per run; the build stage and the builder's check of
a changed map read its result). The rows `gold.map_geography` holds for these ids are matched by
name and are not used here: they leave four in ten gmina ids unmatched and give some gminas the
area of a namesake. A code TERYT no longer has stays unmatched; it is never matched by name.

**One area, one unit per block (GUS BDL).** BDL publishes one area under several units: a region
(level 3) that is a whole voivodeship, the website's TERYT code beside the unit id, another unit
under a borrowed id. Two units under one `geography_key` would repeat `series_key × date_key`, and
choosing between their rows would be a silent pick. `gold_gus_bdl_units.sql` therefore gives each
area one unit by rule (the unit named as its id says; a voivodeship before the region that stands
in for it; the unit id before the TERYT code; a tie gives it to none). Every other unit keeps all
its rows under `_X` with the source code `<code>#<area>`, which names the area it maps to; the
builder reads that form when the map changes, so giving regions their own keys later rebuilds only
the blocks that hold one.

### 6. Frequency is part of the measure key; S and W have their own frequency

`series_key` (`measure_key|geography_key|breakdown_json`) has no frequency, and `date_key` is the
first day of the period, so the annual and the first-quarter row of one measure would share a key
(lesson of 11 Nov). The frequency is therefore always the last part of the key (`@A`, `@Q`, `@M`),
not only where a block needs it: a key must not change when a dataset gains a second frequency.

Half-yearly and weekly periods are loaded with their own frequency (`S`: 1 January and 1 July; `W`:
the ISO week's Monday) and never folded into another frequency (D35). The playbook's rule that S and
W need a new fact model before they enter `fact_*` is unchanged for the curated facts. Periods the
calendar cannot place (Eurostat frequency `NAP`) are not loaded and are counted as explained
exclusions (decision 10).

### 7. Labels from the Silver dictionaries

Names come from the source's own dictionaries (Eurostat: `eurostat_dataflows`, `eurostat_dimensions`,
`eurostat_codes`; BDL: `bdl_variables`; DBW: `src_dbw_dictionary`). `measure_name` = block name, unit
name and the names of the measure codes, stored once per measure. Names of breakdown codes are
**not** stored per row or per series (about 1,000 M Eurostat series): `breakdown_label_json` stays
empty for these sources, as in the curated `poc_build_eurostat`, and the names belong in one small table
per block (open decision 2). A name that changes without a new data version is picked up at the
block's next rebuild.

### 8. Quality gates

| Gate | Where |
|---|---|
| Grain `series_key × date_key` | every block, before upload |
| Keys not null (`measure_key, geography_key, date_key, frequency, value`) | every block, same pass |
| Series and measures unique; points add up to the fact rows | every block, same pass |
| One source code maps to one area | every block (`gold.<source>_geography`) |
| Completeness Gold against Silver (blocks holding the expected recipe, rejected, to build) | the builder's check, file lists only |
| Unmatched geography (`_X`) | per block and code in `gold.<source>_geography`; the source code stays in the key |
| Rows against Silver | each run's summary: Silver rows, Gold rows, rows not loaded with the reason |

dbt tests do not run on these tables. A new check that would reject blocks already in Gold is first
run as a Dev trial with the failing blocks annotated (D29).

### 9. What the portals can query

- **data.zohelo.com** reads Iceberg tables: the tables of a source are queryable as they are;
  `gold.<source>_measures` is the entry point and a block filter reads one file.
- **zohelo-bi** reads the published layer, which carries KPIs only (O37). A catalogue of non-KPI
  measures is a contract change (open decision 1).
- **KPIs** stay curated. Once a source is fully in these tables, its `poc_build_<source>` stage may
  read the block files instead of Silver, still writing the hand keys.

### 10. Coverage metric

Per source, one number the portal can show:

**coverage = series in Gold ÷ (loaded series − series excluded by a stated rule)**

- A **loaded series** is a distinct series identity of the source's Silver with at least one value
  (Eurostat: frequency × unit × geography × breakdown codes; BDL: unit × variable; DBW: block ×
  presentation × positions; a flat Silver table: its key columns without the period). A series with
  only rows without a value was never loaded.
- An **explained exclusion** is a series not loaded by a stated rule: a period the calendar cannot
  place, a cell rule of decision 5. Each is counted per block with its reason and shown next to the
  number. So 100% is reachable and never hides a loss: anything not in Gold is either in the
  denominator or listed as an exclusion.
- 100% is reached when every loaded block is in Gold and the series in Gold equal the denominator. A
  rejected block, or one waiting for a run long enough to build it, keeps the source below 100% and
  says why. Unmatched geography (`_X`) does not lower coverage (the series is in Gold with its
  source code) and is reported next to it.
- `gold.coverage` (one row per block a source publishes) gains `silver_series`, `gold_series`,
  `gold_rows`, `excluded_series` and `gold_status` (`in gold` / `rejected` / `to build` /
  `not modelled`), filled from each block's build; `gold.build_check` gains a per-source summary row.

## Rules for a source's adapter

A source needs one small rule file: its build stage, SQL that turns all its Silver rows with a value
into the common observation shape, with no block list and no sample filter, plus one entry in the
builder (Silver table, how entities group into blocks, lookup tables). It states the source's rules
and nothing else. Everything after the build stage is shared.

1. The denominator is counted first: Silver tables, rows with a value, loaded series.
2. The block is the Silver entity, a group of entities, or the Silver table; it is recorded in the
   source YAML `refresh`.
3. The build stage covers **all** Silver tables and series of the source; one measure rule per
   table; periods other than A, S, Q, M, W, D are excluded and counted, never folded. The curated
   `poc_build_<source>` stays for the KPIs.
4. Every geography code is mapped by code in the source's `poc_geography__<source>()`
   ([source-registration.md](source-registration.md), D34; an official correspondence table where
   codes differ; names only as a reported fallback). When it reads the source's own Silver table, it
   reads it through `poc_optional_ref` as [playbook.md](playbook.md) Gold step 2 says: a plain `ref()`
   makes the map and every block wait for the source's first load (#2903). The source's per-block Gold
   tables are declared in its own source YAML `gold:` (D34).
5. Units follow D33: each unit is a `unit.csv` row with `base_unit_key` and `factor`, each source
   code a `unit_map.csv` row; an unmapped code stays as published and is reported.
6. A Dev trial into `zz_dev_<topic>` shows the grain empty, coverage 100% or each missing series
   explained, rows against Silver, build time and size.
7. Existing `kpi_measure.csv` patterns match exactly the series they matched before; published KPIs
   keep their keys and their first source.
8. A register source puts every loaded entity in its Gold dimension or entity table, keyed by the
   official identifier; the metric counts entities.

## Open decisions

| # | Question | Proposal | Who |
|---|---|---|---|
| 1 | Non-KPI measures in zohelo-bi | An additive published v2 catalogue, `catalog/measures_<source>.parquet` from `gold.<source>_measures`; decide once one source is fully loaded | owner |
| 2 | Names of breakdown codes | One small table per block (`gold.<source>_codes`: dimension, code, name, total code) before the portal shows these tables | architect |
| 3 | Eurostat codes of Polish NUTS 3 regions are unmatched (`_X`) | Map them to the podregion keys DBW already uses (Eurostat's codes are the `eurostat` CTE of `poc_map_geography.sql`, registered before D34) | architect |
| 4 | The series table is more than half of the bytes | Keep it for the first load; then decide whether series attributes move onto the fact for annual blocks | architect |
