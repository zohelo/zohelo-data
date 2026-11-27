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
| GUS DBW | 4,903 blocks | about 890 M | not counted (trial sample: one series per 5 rows) | about 2.6 GB |

GUS DBW in Gold: a 52-block trial (52 M rows, #2983) wrote about 7.0 MB per million rows to the observations
and 3.5 MB to the series table, about 9 GB for all of DBW at that rate (the sample is dominated by one gmina
block, so this is an upper estimate).

Eurostat by geography: Poland and its regions about 200 M rows; with the EU aggregates and all
countries about 4,200 M; every geography about 7,000 M. Full Eurostat in Gold is about 22 GB, more
than half of it the series table (annual series are short). The curated `fact_*` tables hold about
2 M rows.

## Decisions

### 1. Scope: every block, every geography the source publishes (decided, O43)

Every loaded series of every source, all geographies, goes to Gold; the storage this needs is
accepted. No geography filter: one rule for all blocks is what removes the hand-picking.

**Load order** (O43): Poland and its regions first, then countries and EU aggregates, then the rest.
A block is built whole, so the order is an order of blocks: first the blocks of Polish sources
and the blocks that hold Polish regions, then the blocks that hold Poland, other countries or the EU
aggregates, last the blocks with neither (other countries' regions only, airport pairs). Within a
group, smaller blocks first, and a group is finished before the next one starts.

GUS BDL is a Polish source, so its three groups are territorial levels: subgroups down to
voivodeships and subregions, then down to powiats, then down to gminas and statistical localities. The group is the lowest
level the subgroup's variables have (`silver.bdl_variables`, read once per run with the lookups);
a subgroup without variable metadata is built with the last group. Nothing else is read for it.

How a block's group is found (#2974): a block whose structure has no geography dimension is in the
last group, from the structure metadata alone. For the other blocks the structure does not help:
the structure messages Eurostat publishes name one `GEO` code list for every dataset, not the codes a
dataset holds. So the builder reads the geography column of the Silver files of the blocks it is about
to build (one column, once per run, at most a tenth of the run's time; nothing is read for a block
that needs no build) and looks each code up in the geography map. A block not read in that time is
ordered with the countries. The order is a preference, never a filter: every block is loaded.

### 2. One path for every statistical source: per block

Full coverage of every statistical source, big or small, is built per block (D35). The curated
`fact_*` tables stay the reviewed KPI selections under their #822 ceilings; no source is loaded
into them unfiltered. One path keeps one set of rules (whole-block accept or reject, the grain
checked per block, the run limits of D35) instead of a size threshold that a source crosses as it
grows.

A **block** is the unit of rebuild: a Silver entity (Eurostat dataset, BDL subgroup), a group of
entities (DBW: the files of one block), or, for a source whose Silver is a dbt table, the table
itself; a table too large for one block is split by an entity column first.

**A source whose Silver is dbt tables** (#2998, #2943; World Bank WDI, OECD, NBP, BIS, ILOSTAT, UNDP HDR,
UNdata, UNECE, FAOSTAT, World Bank IDS, EEA, ECB, UNCTAD, World Bank Findex, ICP and WGI, NBP balance of
payments, UN WPP, UNESCO UIS, World Bank EdStats and GEM, OWID and WHO so far) declares them in the
builder (`tables` of its `SOURCES` entry): each table is one block, named after the table, with its own
build stage, because two tables of one source rarely share a shape (UNCTAD's nine tables do, one macro
builds them all, so they share one stage). The block's Silver files are the
table's current data files, taken from the table's file list, so the plan still reads metadata only.
dbt writes new files whenever it rebuilds a table, so the block is rebuilt in Gold whenever the changed-only
build rebuilt its Silver table, also when the rows came out the same (a rebuild after a change of shared
code); for tables of this size that costs one read of the table. A declared table that is missing or holds
no file stops the run: that is a fault of the build, never a block that left the source, and nothing is
removed from Gold for it (with a handful of blocks the removal cap of D35 would not protect them). A model
build of another run replaces a dbt table by drop and rename, so a table not found is read again every
30 s for up to 9 minutes first, as `gold.publish` does (#2834). The
load order needs no read for these sources. Whole tables as blocks hold up to the size one worker builds
alone (WDI: about 9 M rows); a larger table gets an entity column and per-entity Silver files first.

### 3. Layout: four tables per source, one file per block

| Table | One row is | Columns |
|---|---|---|
| `gold.<source>_observations` | series × period | as `gold.fact_*`: `series_key, measure_key, geography_key, date_key, frequency, value, observation_status_key, source_system, block_id, period_label, source_version, raw_sha256` |
| `gold.<source>_series` | one time series | as `gold.dim_series`, plus `source_version, raw_sha256` |
| `gold.<source>_measures` | one measure | as the activated rows of `gold.dim_measure`, plus `unit_name`, unmatched-geography and total-series point counts |
| `gold.<source>_geography` | block × source geography code | the area the code was mapped to (`_X` unmatched, `_Z` none) and its points |

Each table is partitioned by `block_id` and holds exactly one Parquet file per block
(`04_gold/<source>/<table>/<block>/<recipe>.<content>.parquet`), the layout Silver uses for the big
sources (design #875). `<recipe>` names what the file was made from (decision 4); `<content>` is a
digest of the file's own bytes. **A Gold file is never replaced:** an upload writes a new key or finds
the same bytes under the key and keeps them; other bytes under an existing key stop the run, because a
table may point to that object. The four tables are committed one after the other (observations last)
and a block counts as built only when all four hold one recipe, so a run that stops between the commits
rebuilds the block next time under new keys or the same bytes. A superseded file stays as an
unreferenced object until Ops `orphan_files` removes it.

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
- **GUS DBW.** `gus_dbw:<block>:<presentation id>[:<cycle>]@<frequency>`: measure = indicator ×
  presentation, as `poc_build_dbw.sql`. The frequency comes from the period's own name (`id_okres`:
  "kind of data" annual, half-yearly, quarterly, monthly), not from the block id; `<cycle>` appears only
  where a monthly figure is taken once a quarter or once a year. The territorial dimension is the
  dimension slot with the most positions named like Polish areas in the block's own dictionary; the
  other dimensions are breakdowns, DBW's dimension and position ids as published. Detail:
  [sources/gus_dbw.md](sources/gus_dbw.md).
- **World Bank WDI.** `world_bank_wdi:wdi_observations:_Z:<indicator code>@A`: an indicator code is one
  measure whole (WDI's codes for a sex or an age band are its own indicators), so no series has a
  breakdown. The unit is not in the key (`_Z`): WDI states it in the indicator's name and in a metadata
  field that is filled or corrected without the indicator changing. The unit code stays the metadata
  field as published, else `as named`.
- **OECD.** `oecd:oecd_cli:<UNIT_MEASURE>:<MEASURE>:<ADJUSTMENT>:<TRANSFORMATION>:<METHODOLOGY>@<FREQ>`:
  every dimension of the dataflow other than the area and the frequency defines the measure.
- **NBP.** `nbp:nbp_fx_rates:PLN:<table>:<mid|bid|ask>@D` with the currency as the breakdown;
  `nbp:nbp_gold_fixings:PLN/g:price@D`. No area (`_Z`).
- **BIS.** `bis:bis_observations:<UNIT_MEASURE>[*10^<UNIT_MULT>]:<selection>:<series codes>@<FREQ>`: the
  series key as published without FREQ and the area (REF_AREA, or BORROWERS_CTY in the credit dataflows);
  every other dimension defines the measure. The BIS aggregates (4T, 5R, 5A, G2, XW) stay `_X` with their code.
- **ILOSTAT.** `ilostat:ilostat_observations:<unit>:<indicator>@<FREQ>` (unit `PC` for the `_RT` indicators);
  sex, `classif1`, `classif2` and the survey (`source`) are breakdowns as published, so two surveys of one
  area and period are two series, never one chosen value. ILO aggregates (`X01` …) stay `_X`.
- **UNDP HDR.** `undp_hdr:undp_hdr_indices:_Z:<indicator>@A`: an indicator (column name without the year) is
  one measure whole (`hdi_f`, `gii_rank` are the HDR's own indicators); unit `as named`. HDR groups and
  regions (`ZZA` … `ZZJ`) stay `_X`.
- **UNdata.** `undata:undata_syb_values:<unit>:<table>:<series>@A`: the Yearbook's series text is the measure
  code, its last brackets the unit as published (`millions of US dollars`), else `as named`. M49 codes are
  mapped by rule in `gold_undata_map.sql` (a country's M49 code is its ISO 3166-1 numeric code, through
  alpha-3 to the country; the curated map's rows win); M49 regions stay `_X`.
- **UNECE.** `unece:unece_observations:_Z:<table id>[:<codes>]@<freq>`: the value codes of every variable other
  than the area define the measure; frequency from the period (`1990`, `2025Q2`, `2025M07`); unit `as named`
  (the value labels). The UNECE groupings (CIS, CWG, EAEU, ECE, NA, ST7) stay `_X`.
- **FAOSTAT.** `faostat:faostat_observations:<unit>:<domain>:<item>.<element>.<FAO source>@A`: domain, item,
  element and FAO's data source (`_Z` where the domain has none) define the measure, as in the curated stage.
  Areas by FAO area code through the curated map (the EU27 members, 5707 European Union, 5000 World); every
  other FAO area (other countries, FAO regions) stays `_X` with its code until a rule map by M49 exists.
- **World Bank IDS.** `world_bank_ids:ids_observations:_Z:<series code>@A`, as WDI (unit `as named`); the
  counterpart area (creditor) is the breakdown as published (`WLD` = all creditors, `is_total`).
- **EEA.** `eea:eea_ghg_inventory:<unit>:<format>:<pollutant>:<sector code>@A`: reporting format, pollutant and
  CRF sector as published define the measure. The aggregate EUA stays `_X`.
- **ECB.** `ecb:ecb_observations:<UNIT>[*10^<UNIT_MULT>]:<dataflow>:<series codes>@<FREQ>`: the series key as
  published without FREQ and REF_AREA, as for BIS; B (business days) is loaded as `D`; FM, EST and BSI without
  REF_AREA are the euro area (U2) and EXR has no area (`_Z`), as in the curated stage.
- **UNCTAD.** `unctad:<table>:<measure>[:<dimension codes>]@<freq>`, one block per Silver table: the measure
  as UNCTAD names it is the unit (`US$ at current prices in millions`, a percentage, an index), and every
  dimension other than the economy (flow, direction, category, commodity, port, vessel type) defines the
  measure, codes joined by `.` in the table's column order; frequency from the period (`2025`, `2006Q01`,
  `1995M01`). Economies through the curated map (EU27 members, 3412 European Union (2020 …), 0000 World);
  the other UNCTAD economies and groups stay `_X` with their code; commodity prices and the port liner
  shipping connectivity index have no economy (`_Z`; the port is a dimension of the measure).
- **World Bank Findex.** `world_bank_findex:findex_observations:_Z:<indicator>@A`: an indicator code is one
  measure whole; the population group is the breakdown as published (`{"group": …, "group_value": …}`, group
  `all` = is_total), so the shares for women and for men are two series. Unit: a share of the population group,
  0-1, as published (the curated code "share of population age 15+" holds for group `all` only).
- **World Bank ICP.** `world_bank_icp:icp_observations:<classification>:<series>@A`: the classification (CN,
  PPPGlob, PCAP.PP, PX.WL …) is measure and unit at once, the series the expenditure category, as in the curated
  stage; no breakdowns.
- **World Bank WGI.** `world_bank_wgi:wgi_observations:_Z:<indicator code>@A`, as WDI: every `GOV_WGI_<dimension>.<x>`
  code (estimate, score and its bounds, standard error, number of sources) is one measure; unit the metadata
  field as published, else `as named`.
- **NBP balance of payments.** `nbp_bop:nbp_bop_observations:mln_<currency>:<workbook>:<item code>@<M|Q>`: workbook
  (bop_m, bop_q, iip_q), currency sheet (PLN, EUR, USD; never converted) and Silver's item code define the measure;
  no breakdowns. Poland only (`PL`).
- **UN WPP.** `un_wpp:un_wpp_demographic_indicators:<unit>:<indicator>@A`: an indicator column (TPopulation1July,
  LExFemale …) is one measure whole, as WDI's codes (WPP publishes the values by sex as their own indicators); the
  unit per indicator as WPP's definitions state it (thousands of persons, births or deaths, per 1,000 population,
  per 1,000 live births, years …). Locations by ISO two-letter code, else LocID, through the curated map (countries,
  the EU (EU: 27) group, World); the UN's other groups stay `_X` with their code. The age-group table is left out:
  its columns are sums and ratios Silver computes from the published five-year groups (which stay in Bronze), not
  series WPP publishes.
- **UNESCO UIS.** `unesco_uis:unesco_uis_observations:<unit>:<base indicator>@A`, as the curated stage: GERD as % of GDP
  (`pct_gdp`) and tertiary attainment 25+ (`pct`); the `.F` and `.M` indicators are the sex breakdown of their base
  indicator (`{"sex": "T|F|M"}`). Countries by ISO alpha-3; the UIS, SDG, MDG, World Bank and UNESCO groupings stay `_X`
  with their code.
- **World Bank EdStats.** `world_bank_edstats:edstats_pisa:score:<base indicator>@A`: one measure per PISA subject, the
  `.FE` and `.MA` indicators its sex breakdown, as the curated stage. The OECD average (`OED`) stays `_X`.
- **World Bank GEM.** `world_bank_gem:gem_observations:<unit>:<series code>@<A|Q|M>`: a series code is one measure
  whole and each frequency GEM publishes it at is a measure of its own (the curated stage keeps one); units as the
  curated stage names them per series.
- **OWID.** `owid:owid_observations:<unit>:<chart>@A`: a chart's one value column is one measure, units as the curated
  stage. An entity without a code (OWID's continents, Maddison regions, states before German and Italian
  unification) keeps its published name as the source code; it and OWID's other `OWID_…` codes stay `_X`.
- **WHO.** `who:who_observations:years:<indicator code>@A`: life expectancy (WHOSIS_000001) and healthy life
  expectancy (WHOSIS_000002) at birth, the sexes their breakdown (`{"sex": "T|F|M"}`); the point estimate only, the
  bounds stay in Silver, as the curated stage. Areas `<type>:<M49 code>`: COUNTRY and the two territory types by
  their M49 code through ISO 3166-1 numeric → alpha-3 (`gold_who_map.sql`, the list `gold_iso3166_numeric.sql` that
  UNdata's map reads too); WHO regions and World Bank income groups stay `_X` with their code, GLOBAL:1 is the World.

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
  `confidential`; a value flagged `f` with status `forecast`; every row of a projection dataset
  (`PROJ_*`) is `forecast`, whatever its flag. A row keeps its status in every table
  it reaches; a reader that wants observed values only filters on `observation_status_key`, and a
  forecast is never presented as an observation. An exact zero (`0`, `0.0`, `0.000`, with or without
  a flag) is a value Eurostat publishes, most of all in the airport-pair and stock datasets, and is
  loaded as published: no layer fills a cell, and the bulk file, Silver and Gold hold the same number
  of zeros per dataset (#2973). A reader that wants no zeros filters on `value`.
- GUS BDL: an empty value is not loaded; a 0 under a no-value sign is not loaded (#2852: website
  signs `x`, `X`, `n`; API attribute ids 4, 14, 50, 91), a number other than 0 under such a sign is;
  two different published values for one key reject the subgroup in Silver already. A value for a
  year after the year of the subgroup's version, and every row of a subgroup named a projection
  (`prognoza`), has status `forecast`. An exact 0 without such a sign is a zero BDL publishes and is
  loaded as published, as Eurostat's (#2973): the API gives it attribute 0 (`/attributes`: name "0",
  no symbol), apart from the no-value signs and from 15 `-` (no information after a change of units
  or variables; no Silver row carries it), and the website export writes an empty cell empty and this
  zero as `0`, both within one variable and unit (`/attributes` as read in Dev run 43275462132; counts in
  Dev runs 45829968475 and 45833898232, #1178).
  In the subgroups of gminas and statistical localities most rows are such zeros (P2179 97%, P2035
  89%, P2017 84% of the values); a reader that wants no zeros filters on `value`.
- GUS DBW: an empty value is not loaded; a 0 with a no-value sign (`id_brak_wartosci` other than 253
  "Nie dotyczy") or under confidentiality (`id_tajnosci` other than 43 "Dane jawne") is not loaded; a
  non-zero number under confidentiality or with sign 38 (figure withheld) is loaded as `confidential`,
  one with another no-value sign (42, 725) as `flagged`; `id_flaga` gives `provisional`, `estimated`,
  `break`, `low_reliability` and `forecast` (30 "Prognoza"), none, 0 and the final, revised, full
  population, not applicable and informative-note flags are `ok`. These ids are every id in DBW's Silver
  (Dev run 45745873785); the run summary counts the rows left out by reason
  (`gold_gus_dbw_not_loaded.sql`).
- World Bank WDI: an empty cell is not loaded; a year after the year of the archive's version is a
  projection and is not loaded. OECD: a period without a number is not loaded; `OBS_STATUS` becomes the
  status (`A` or none ok, `P`, `E`, `B`, `D`, `U`, `N`; any other code `flagged`, never silently ok), and
  `F` (forecast) is not loaded. NBP: a rate a table does not publish (bid and ask in A and B, mid in C)
  is no row; table B is published weekly and its days in between are not filled.
- BIS: `NaN` (OBS_STATUS M) is no value; OBS_STATUS A/none ok, B, P, E; any other code `flagged`. ILOSTAT,
  UNECE: a period that had not ended on the day of the file's version is a projection or an incomplete
  period and is not loaded (the ILO aggregates carry years ahead). UNECE: a 0 in a series whose value labels
  name no rate, growth, change, per cent or balance (an index, a level, an exchange rate) is a value below
  the printed precision or a cell without a value and is not loaded, as in the curated stage; a rate of 0 is
  kept. UNdata: a row footnoted as a projection is not loaded; footnotes give `definition_differs` or
  `estimated` as in the curated stage. UNDP HDR: no flags, `ok`. UNCTAD: a measure without a number, a value that carries
  a missing-value reason and a value footnoted as a forecast or projection are not loaded; footnotes give
  `estimated` (estimate, nowcast), `break` and `provisional` (provisional, preliminary), other footnotes are
  notes and the value is `ok`, as in the curated stage.
- World Bank Findex, ICP and WGI: a cell without a number (empty, `NA`, `..`: no value in Silver) is not loaded,
  nor a value that is not a finite number, nor a year after the year of the version (a projection; none of the
  three publishes one). Findex and WGI publish no flags (`ok`). ICP surveys the benchmark years 2017 and 2021;
  every other year of ICP 2021 is filled between and around them and has status `estimated`, never `ok`.
- NBP balance of payments: a cell without a number or
  outside the period grid is not loaded (it fails Silver's tests); the newest period of each workbook is
  `provisional` (NBP revises it with the next release), every earlier one `ok`, as in the curated stage. UN WPP:
  the projections (years from 2024 in the 2024 revision) are not loaded, nor an empty cell; no flags (`ok`).
- UNESCO UIS: magnitude `NA` (not applicable; the API sends 0 in the value field) is not loaded; magnitude `NIL` (nil
  or negligible) is loaded as published with status `flagged`; qualifiers `UIS_EST` and `NAT_EST` give `estimated`
  as in the curated stage, any other qualifier `flagged`. World Bank EdStats: no flags today; a flag gives `flagged`.
  World Bank GEM: a period that had not ended on the day of the file's version is not loaded (the annual rows of the
  current year are an average or a sum of the months so far); no flags (`ok`). OWID: years before 1800 (Maddison
  from year 1, V-Dem from 1789) are not loaded, nor an exact 0 in the consumption-based CO2 chart (a missing value,
  the curated stage's rule); no flags (`ok`).
- WHO: a row without a value, a row not in state `PUBLISHED`, a period that is not a year and a year after the year
  of the file's version (a projection; none today) are not loaded; no flags (`ok`).
- FAOSTAT: a cell flagged `M` (missing value) is not loaded whatever number it carries; a projection (a year
  after the year of the file's version) is not loaded; flags E and I `estimated`, B `break`, P `provisional`, A
  and X `ok`, any other flag `flagged`. World Bank IDS: a year after the last actual year (the newest year of
  total external debt stocks, as in the curated stage) is a projection of scheduled debt service and is not
  loaded. EEA: a row with a notation key and no number is not loaded, nor a 0 published with a notation key; a
  number published with a notation key is `flagged`. ECB: OBS_STATUS `F` (forecast) is not loaded; A/none ok, P,
  E, B, D (`definition_differs`), any other code `flagged`; a period Silver cannot place (weekly,
  half-yearly) is not loaded.

**GUS BDL geography by code.** A BDL unit id carries the TERYT code of its area, so Poland, a
voivodeship, a powiat and a gmina are mapped by that code through the map's rows of the 7-digit
TERYT codes (`gold_gus_bdl_map.sql`, run once per run; the build stage and the builder's check of
a changed map read its result). The rows `gold.map_geography` holds for these ids are matched by
name and are not used here: they leave four in ten gmina ids unmatched and give some gminas the
area of a namesake. A code TERYT no longer has stays unmatched; it is never matched by name.

**One area, several units (GUS BDL).** BDL publishes one area under several units: a region
(level 3) that is a whole voivodeship, a gmina before and after it got town rights, the TERYT code
of an older export beside the unit id, another unit under a borrowed id. Two rows under one
`geography_key` would repeat `series_key × date_key`, and choosing between them would be a silent
pick. `gold_gus_bdl_units.sql` orders the units of an area by rule (the area's own unit before a
region that stands in for it, the current TERYT code before a changed one, the unit id before the
TERYT code), and `gold_gus_bdl_cells.sql` gives each cell (series × year) to the first unit that
has it. The same cell of a later unit stays in Gold under `_X` with the source code
`<code>#<area>`, whatever its value, so no row is dropped and no value is chosen; so does every
row of a unit whose name is not the name of its id (a borrowed id) and of units the rule cannot
order. The builder reads that form when the map changes, so giving regions their own keys later
rebuilds only the blocks that hold one.

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
`eurostat_codes`; BDL: `bdl_variables`; DBW: the block's files of `bronze.dbw_dictionary_items`). `measure_name` = block name, unit
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
  Not built yet.
- Until then every run of `gold.observations` ends with the annotation `<source> gold coverage`, made
  from the tables' file lists only: blocks in Gold over blocks in Silver, series in Gold (the rows of
  the series files) over loaded series, and the share of Silver rows whose block is in Gold. A block in
  Gold is accepted whole, so its loaded series are its Gold series. The loaded series of a block that
  waits or is rejected are not known before its first build: they are estimated from its Silver rows at
  the ratio of the blocks in Gold, and `loaded_series_estimated` says how many of the denominator are
  such an estimate (0 = the number is exact). Excluded series are not counted yet.

### 11. Running in production

The Pipeline activity `gold.observations` (authorized per source in its `activities:`) runs the
builder for the source; [operating.md](operating.md) has the run rules. It is the last step of the
platform's weekly run (`silver/gold: build models, then gold observations`, Sunday 09:41 UTC, O46; one job per
source, after the model build, the publish and the index), so a new block version reaches Gold by the next Sunday
(#3133: source runs end with Bronze or Silver), and it is the source's own run `gold: observations` for a first load or a catch-up.

- **Budget and resuming.** A run builds within its time budget (300 minutes of a 350-minute job),
  commits finished blocks every 200 blocks or 10 minutes, and ends green with a warning when blocks
  are left; the next run plans from the tables' file lists and continues. Failed blocks and a refused
  removal end the run red.
- **The first load is started by hand.** A run without a filter only continues: when Gold holds no
  block of the source it builds nothing and says so. A filter starts it: `*` for every block, or
  block ids and globs for some. Scheduled runs have no filter, so merging the authorization starts
  nothing.

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
   makes the map and every block wait for the source's first load (#2903), and the map lists that
   table in its config meta `optional_parents`. The source's per-block Gold tables and
   `gold.observations_rejected` are declared in its own source YAML `gold:` (D34); `repo_check` fails
   when a source authorized for `gold.observations` does not declare them.
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
| 3 | Eurostat codes of Polish NUTS 3 regions: mapped by name (#2974) | `poc_geography__eurostat()` maps a `PLxxx` code to the podregion (`BDL:<id>`) of the same name in the voivodeship of its NUTS 2 parent, because no loaded table holds the NUTS–KTS correspondence. Decide whether to load GUS's correspondence table and map by code | architect |
| 4 | The series table is more than half of the bytes | Keep it for the first load; then decide whether series attributes move onto the fact for annual blocks | architect |
| 5 | GUS DBW cumulative periods ("dane narastające": January to the month, first quarter to the quarter), three-year data and years that are not calendar years are not loaded; blocks that hold nothing else are rejected as empty | A fact kind with a period start and end (year to date) before they are loaded; never folded into A, Q or M | architect |
| 6 | GUS DBW names powiaty, gminas and countries without a code: they are `_X` with `<NAME> #<position id>` kept | A correspondence of DBW position ids to TERYT from GUS; names only where one name is one unit | architect |
