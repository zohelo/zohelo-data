# Published layer contract (v1)

Small files under `06_published/v1/` that zohelo-bi and zohelo.com read. This document is the
contract both sides build against; `warehouse/publish.py` (`gold.publish`) is the only writer
(decision D5). Types: string, number, integer, boolean, array, object, date. Timestamps are
strings in ISO 8601 UTC.

## Rules

- Prefix `06_published/v1/` in bucket `STORAGE_LAKEHOUSE_BUCKET_NAME`; rebuilt whole by
  `gold.publish` after each Gold build; no history.
- UTF-8; timestamps ISO 8601 UTC; additive fields are allowed in v1; any rename or removal needs
  v2 alongside v1.
- Keys: `geography_key` as in Gold (`'PL'`, `'TERC:<code>'`, `'NUTS:<code>'`); `kpi_key` =
  `concept_key` (e.g. `unemployment_rate`); `unit_key` from `warehouse/seeds/unit.csv`.
- Formats: keys match `^[a-z0-9_]+$`, domain keys dot-separated segments of that form, timestamps
  `YYYY-MM-DDTHH:MM[:SS[.f]]Z`; the schemas declare them as `pattern` (D24).
- All fields below are required unless marked optional.
- gold.publish checks every JSON file against its schema in warehouse/published/schemas/ before writing it and fails
  on a problem.
- The data portal serves these files read-only, after sign-in, at `/api/published/v1/<path>` (portal/worker/src/index.js).

## catalog/catalog.json

| field | type | required | meaning |
|---|---|---|---|
| version | integer | yes | contract version, `1` |
| generated_at | string | yes | when the file was written (ISO 8601 UTC) |
| kpis | array | yes | one entry per KPI |
| kpis[].kpi_key | string | yes | KPI key (= concept_key) |
| kpis[].name_pl | string | yes | Polish name |
| kpis[].name_en | string | yes | English name |
| kpis[].unit_key | string | yes | unit, from `warehouse/seeds/unit.csv` |
| kpis[].frequency_default | string | yes | default frequency of the KPI |
| kpis[].subgroup_key | string | yes | taxonomy subgroup the KPI belongs to |
| kpis[].domain_path | array | yes | keys domain → group → subgroup |
| kpis[].source_keys | array | yes | sources that provide the KPI |
| kpis[].geo_levels | array | yes | geography levels with data |
| kpis[].first_period | string | yes | first period with data |
| kpis[].last_period | string | yes | last period with data |
| kpis[].dimensions | array | yes | extra dimensions of the KPI |
| kpis[].dimensions[].dimension_key | string | yes | dimension key |
| kpis[].dimensions[].member_count | integer | yes | number of members |
| kpis[].dimensions[].levels | array | yes | levels of the dimension |
| kpis[].loaded_at | string | yes | when the KPI's data was last loaded (ISO 8601 UTC) |
| kpis[].status | string | yes | KPI status |
| domains | array | yes | the taxonomy; each entry has the columns of `warehouse/seeds/domain.csv` |
| units | array | yes | the units; each entry has the columns of `warehouse/seeds/unit.csv` |
| sources | array | yes | one entry per source |
| sources[].source_key | string | yes | source key |
| sources[].name_pl | string | yes | Polish name |
| sources[].name_en | string | yes | English name |
| sources[].licence_key | string | yes | licence of the source |
| sources[].register_id | string | yes | id in the source register |
| licences | array | yes | one entry per licence |
| licences[].licence_key | string | yes | licence key |
| licences[].name | string | yes | licence name |
| licences[].url | string | yes | licence text URL |
| licences[].attribution_text | string | yes | attribution to show with the data |
| licences[].commercial_ok | boolean | yes | commercial use allowed |

`warehouse/publish.py` writes it (`kpi_entry`, `catalog_json`) with one `kpis` entry per published KPI that has a
`gold.metrics_<kpi_key>` table, computed from that KPI's series.parquet rows: `source_keys` and `geo_levels`
(non-geographic `_Z` rows count as `country`) as they occur, `first_period`/`last_period` from the rows of
`frequency_default` (all rows when there are none), `dimensions` from the `dim_*` columns (`member_count` without
`'_T'`; `levels` empty until dimension levels exist), `domain_path` by walking `domain.csv` parents of
`subgroup_key`. `loaded_at` is the newest `gold.coverage` `checked_at` of the KPI's sources (the `generated_at` when
none) until `gold.freshness` exists. `domains`, `units`, `sources` and `licences` are the seeds with booleans and
`sort` typed and an empty `parent_key` as null.

## catalog/<table>.parquet

One Parquet file per Gold catalogue table, with that table's columns.

| field | type | required | meaning |
|---|---|---|---|
| dim_kpi | object | yes | `catalog/dim_kpi.parquet`: the KPI dimension |
| kpi_measure | object | yes | `catalog/kpi_measure.parquet`: KPI ↔ measure mapping |
| kpi_dimension | object | yes | `catalog/kpi_dimension.parquet`: KPI ↔ dimension mapping |
| dim_domain | object | yes | `catalog/dim_domain.parquet`: the taxonomy |
| dim_unit | object | yes | `catalog/dim_unit.parquet`: the units |
| dim_source | object | yes | `catalog/dim_source.parquet`: the sources |
| dim_licence | object | yes | `catalog/dim_licence.parquet`: the licences |
| freshness | object | yes | `catalog/freshness.parquet`: load freshness per source and KPI |

`warehouse/publish.py` `catalog_tables()` writes each file from its Gold table: dim_kpi ← `gold.kpi`, kpi_measure ←
`gold.kpi_measure`, kpi_dimension ← `gold.kpi_dimension`, dim_domain ← `gold.domain`, dim_unit ← `gold.unit`,
dim_source ← `gold.dim_source`, dim_licence ← `gold.licence`, freshness ← `gold.freshness`; a file whose table is
not built yet is left out and reported as `missing` in the `gold.publish` annotation.

## manifest.json

| field | type | required | meaning |
|---|---|---|---|
| version | integer | yes | contract version, `1` |
| generated_at | string | yes | when the file was written (ISO 8601 UTC) |
| metrics | array | yes | one entry per metric |
| metrics[].kpi_key | string | yes | KPI key |
| metrics[].table | string | yes | Gold table, `gold.<name>` |
| metrics[].unit_key | string | yes | unit |
| metrics[].frequency | string | yes | frequency of the table |
| metrics[].time_column | string | yes | column holding the period |
| metrics[].value_column | string | yes | column holding the value |
| metrics[].source_column | string | yes | column holding the source |
| metrics[].dimensions | array | yes | dimension columns of the metric |
| metrics[].dimensions[].dimension_key | string | yes | dimension key |
| metrics[].dimensions[].column | string | yes | column in the table |
| metrics[].default_filters | object | optional | column → value applied by default |

`warehouse/catalog.py` `manifest()` builds this object from the dbt semantic models and `kpi.csv`; a published KPI without a semantic model is left out. `gold.publish` (`warehouse/publish.py` `manifest_json`) writes it after a fresh `dbt parse` (the job starts without `target/`).

## kpi/<kpi_key>/latest.json

| field | type | required | meaning |
|---|---|---|---|
| kpi_key | string | yes | KPI key |
| unit_key | string | yes | unit |
| generated_at | string | yes | when the file was written (ISO 8601 UTC) |
| values | array | yes | one entry per source × geography at levels country and voivodeship |
| values[].source_key | string | yes | source |
| values[].geography_key | string | yes | geography |
| values[].geography_level | string | yes | `country` or `voivodeship` |
| values[].period | string | yes | latest period |
| values[].value | number | yes | value in that period |
| values[].unit_key | string | optional | unit of this value (key in `catalog/unit`); always written, may differ from the KPI's `unit_key` when sources publish the measure in different currencies or scales |
| values[].previous_period | string | optional | the period before |
| values[].previous_value | number | optional | value in the period before |
| values[].flags | array | yes | status flags of the value |
| values[].checked_at | string | yes | when the source was last checked (ISO 8601 UTC) |

`warehouse/publish.py` writes it from `gold.metrics_<kpi_key>`: only country and voivodeship rows with a value
(and not geography `_X`) count; each row gets the priority of its source's measure in `kpi_measure.csv` (999 when
none matches); per source and geography one series is kept, the one with the finest frequency, then the best
priority, then the lowest measure key; its newest period gives `period` and `value`, the period before gives
`previous_period` and `previous_value` (left out when there is none); `unit_key` is the chosen series' unit from
`kpi_measure.csv` `unit_key`, or the KPI's unit when that is empty (#2148). `fx_rate` is skipped: it has one value per
currency and latest.json has no currency field.
gold.publish reads each file back and fails when a value is not in Gold.

## kpi/<kpi_key>/series.parquet

| field | type | required | meaning |
|---|---|---|---|
| kpi_key | string | yes | KPI key |
| source_key | string | yes | source |
| measure_key | string | yes | measure |
| geography_key | string | yes | geography |
| geography_level | string | yes | geography level |
| period | string | yes | `'2025'`, `'2025-03'` or `'2025-03-14'` |
| period_start | date | yes | first day of the period |
| frequency | string | yes | frequency |
| value | number | yes | value (Parquet double) |
| unit_key | string | yes | unit of this row (key in `catalog/unit`): its measure's `kpi_measure.csv` `unit_key`, or the KPI's unit when that is empty (#2449) |
| flags | string | yes | status flags |
| dim_<dimension_key> | string | yes | one column per extra dimension; `'_T'` = total |

`warehouse/publish.py` `kpi_series()` writes it from `gold.metrics_<kpi_key>` for every published KPI (`fx_rate`
included): rows with a value, not geography `_X`, either not geographic (geography `_Z`, e.g. exchange rates) or at
a level the contract names (Gold `wojewodztwo` → `voivodeship`, `makroregion` → `nuts1`, `region` → `nuts2`;
`country`, `powiat`, `gmina`, `nuts1`, `nuts2` as they are). Every breakdown key of the kept rows that takes more
than one value (a row without the key counts as `'_T'`) becomes a column `dim_<key>` (lower case, other characters
→ `_`); a key with one value is not a dimension.

## geo/<level>.geojson

`level` ∈ `country`, `voivodeship`, `powiat`, `gmina`, `nuts1`, `nuts2`. A GeoJSON FeatureCollection
with simplified coordinates; each Feature has these properties:

| field | type | required | meaning |
|---|---|---|---|
| geography_key | string | yes | geography, as in Gold |
| name_pl | string | yes | Polish name |
| name_en | string | yes | English name |
| level | string | yes | the file's level |
| parent_key | string | optional for country | geography_key of the parent unit |

`warehouse/publish.py` `geo()` writes `country`, `voivodeship`, `powiat` and `gmina`, from `silver.prg_units` (PRG)
and the names and parents in `gold.dim_geography`, simplified at 0.005° with coordinates rounded to 4 decimals;
`nuts1` and `nuts2` follow later.

## feed/changes.json and feed/calendar.ics

`changes.json`:

| field | type | required | meaning |
|---|---|---|---|
| generated_at | string | yes | when the file was written (ISO 8601 UTC) |
| items | array | yes | changes of the last 30 days |
| items[].at | string | yes | when the change was loaded (ISO 8601 UTC) |
| items[].source_key | string | yes | source |
| items[].dataset_key | string | yes | dataset |
| items[].kpi_keys | array | yes | KPIs the change affects |
| items[].title_pl | string | yes | Polish title |
| items[].title_en | string | yes | English title |
| items[].kind | string | yes | `'data_update'` |

`calendar.ics`: iCalendar, one VEVENT per `release_calendar` row (SUMMARY = `title_pl`,
DTSTART = `release_at`, URL); zero events until that table exists.
gold.publish writes one event per row of warehouse/seeds/release_calendar.csv (UID `<release_at>-<source_key>-<kpi_key>@zohelo.com`, PRODID `-//zohelo//gold.publish//EN`).

## status.json

| field | type | required | meaning |
|---|---|---|---|
| version | integer | yes | contract version, `1` |
| generated_at | string | yes | when the file was written (ISO 8601 UTC) |
| sources | array | yes | one entry per row of `warehouse/seeds/source.csv` |
| sources[].source_key | string | yes | source |
| sources[].name_pl | string | yes | Polish name |
| sources[].name_en | string | yes | English name |
| sources[].last_loaded_at | string or null | yes | newest `gold.coverage` `checked_at` of the source (as catalog.json `loaded_at`); null when none |
| sources[].next_expected_at | string or null | yes | next run from `python run.py schedule --next`; null without a schedule |
| sources[].lag_days | integer or null | yes | whole days from `last_loaded_at` to `generated_at`; null without `last_loaded_at` |

## Size limits

| file | limit |
|---|---|
| catalog/catalog.json | ≤ 2 MB gzip |
| kpi/<kpi_key>/latest.json | ≤ 100 kB |
| feed/changes.json | ≤ 200 kB |
| status.json | ≤ 50 kB |
| kpi/<kpi_key>/series.parquet | ≤ 20 MB |
| geo/<level>.geojson | ≤ 2 MB (gmina ≤ 8 MB) |

# Published layer contract v2 (for zohelo-bi)

Owner decisions O27/O28 (2026-11-10): the zohelo-bi design template becomes zohelo-bi and the first real slice
(Polska w skrócie, the economy and labour dashboards) is the next goal. v2 is what that product needs. It is
written **alongside** v1 under `06_published/v2/`; v1 stays unchanged for zohelo.com until zohelo.com chooses to
move. `gold.publish` writes both. zohelo-bi reads v2 only.

Status (2026-11-11): built. `warehouse/publish_v2.py` writes it after the v1 files when `published_v2: true` in
`warehouse/warehouse.yaml` (`false` stops it; v1 is not affected). Schemas in `warehouse/published/schemas/v2/`,
fixtures in `warehouse/published/fixtures/v2/` (from synthetic Gold rows; `ops.repo_check` checks them against the
schemas and the cross-file rules of `published_v2_problems`). Seeds: `kpi_v2_map.csv` (the v2 keys),
`kpi.csv` (`definition_pl` beside `definition_en`), `geography_v2.csv`, `unit_decimals.csv`, `kpi_member.csv`.

## What changes against v1, and why

| Need of zohelo-bi | v1 | v2 |
|---|---|---|
| The KPI keys of the dashboards | concept keys (`inflation`, `unemployment_rate`) | the product keys of zohelo-bi (`hicp_annual_rate`, `unemployment_lfs`); v1 keeps its keys |
| Compare Poland with EU countries and the EU | only country and voivodeship of Poland | every EU country and the EU-27 aggregate where the source has them |
| Breakdowns with names (sex, age, NACE, COICOP…) | dimension key and member count only | dimensions with their members, names and order |
| Never sum an average, never index a rate | nothing | `aggregation` per KPI |
| Green or red for a change | nothing | `polarity` per KPI |
| Year-on-year in KPI cards, release dates | previous period only | `year_ago`, `next_release` |
| Readable without Parquet in the browser | series as Parquet | series as compact JSON per KPI |

## Rules

- Prefix `06_published/v2/`; rebuilt whole by `gold.publish` after each Gold build; no history; UTF-8; ISO 8601 UTC.
- A KPI is in v2 only when its concept has `status = published` in `warehouse/seeds/kpi.csv`; its v2 key is the
  `v2_key` of the concept's rows in `warehouse/seeds/kpi_v2_map.csv` (one place for the mapping, no `v2_key` column
  in `kpi.csv`). A concept maps to one v2 key at most; a v2 key comes from exactly one concept. Each ready source of
  the map is its own series (O31), restricted to the map row's `measure_key_pattern`.
- Keys: `kpi_key` (v2 key) `^[a-z0-9_]+$`. **Geography keys**: countries ISO 3166-1 alpha-2 (`PL`, `DE`, `CZ`;
  Greece `EL` as Eurostat writes it), the EU aggregate `EU27` (Eurostat `EU27_2020`), Polish voivodeships
  ISO 3166-2 (`PL-02` dolnośląskie … `PL-32` zachodniopomorskie, the TERC code after the hyphen). No NUTS codes
  in v2 keys: NUTS `PL22` is Śląskie while TERC `22` is Pomorskie, so a bare code is ambiguous.
  `warehouse/seeds/geography_v2.csv` lists every v2 geography (Poland, the EU-27 aggregate, the other 26 member
  states, the 16 voivodeships) with the Gold keys it stands for (Gold `GR` → `EL`, `TERC:02` → `PL-02`,
  `EUROSTAT:EU27_2020` and World Bank `WB:EUU` → `EU27`); Gold rows of other areas are not in v2. A non-geographic
  Gold row (`_Z`, the NBP rates) belongs to the one geography its map row names (`PL`).
- Periods: `2025` (annual), `2025-Q3` (quarterly), `2025-09` (monthly). Daily data is published as the monthly
  mean in v2 (exchange rates); the daily series stays in v1.
- Members: `_T` is the total of a dimension and never appears as a member; a series without a dimension key is
  the total. Gold metrics hold total series, so a breakdown key of `breakdown_json` is a dimension only where it
  splits the rows of one measure (the NBP currency); keys are named as in v1 (`dimension_alias.csv`, else lower case).
  Where `kpi_member.csv` lists members of a dimension for a v2 key, only those reach v2 (`eurpln` = currency `EUR`);
  a dimension left with one member is not a dimension.
- Units: every series and every latest value carries its own `unit_key` (the measure's `kpi_measure.csv` unit, else
  the KPI's), because sources publish one KPI in different units (GDP in mln EUR, mln PLN and US$; GUS indices
  `index_py_100` beside rates in %).
- Flags: Gold observation status → letter: provisional `p`, estimated `e`, break `b`, forecast `f`, low reliability
  `u`, definition differs `d`, not significant `n`, confidential `c`; other statuses carry no flag.
- Every JSON file is checked against its schema in `warehouse/published/schemas/v2/` before it is written; the
  run fails on a problem (as v1).

## catalog.json

| field | type | meaning |
|---|---|---|
| version | integer | `2` |
| generated_at | string | when written |
| kpis[] | array | one entry per v2 KPI |
| kpis[].kpi_key | string | v2 key |
| kpis[].name_pl, name_en | string | names |
| kpis[].definition_pl, definition_en | string | one or two sentences: what is measured, how, coverage |
| kpis[].unit_key | string | unit (see `units`) |
| kpis[].frequency | string | `M`, `Q` or `A`: the frequency the product shows |
| kpis[].domain_key, group_key | string | domain and group of `domain.csv` (the 12 domains of zohelo-bi) |
| kpis[].source_keys | array | sources, the preferred first |
| kpis[].geographies | array | geography keys with data, `PL` first |
| kpis[].dimension_keys | array | breakdowns besides geography and source |
| kpis[].first_period, last_period | string | range of the shown frequency |
| kpis[].aggregation | string | `sum` (counts and money: parts add up), `mean` (averages: wages, prices per m²), `rate` (shares and rates in % or ‰), `index` (an index level), `none` (ratios such as per-capita values, nothing to combine) |
| kpis[].polarity | string | `up` (a rise is good), `down` (a fall is good), `neutral`; set only where most readers agree (docs/model.md) |
| kpis[].loaded_at | string | newest load of its sources |
| kpis[].next_release | string or null | next date in `release_calendar.csv` for the KPI (`YYYY-MM-DD`) |
| dimensions[] | array | every dimension used by a v2 KPI |
| dimensions[].dimension_key, name_pl, name_en | string | |
| dimensions[].members[] | array | `{ key, name_pl, name_en, sort, parent_key? }`, total excluded |
| geographies[] | array | `{ key, name_pl, name_en, level: country \| voivodeship \| aggregate, eu: boolean }` for every key used |
| units[] | array | `unit.csv` plus `decimals` (integer: the precision to show) |
| domains, sources, licences | array | as v1 |

Written from the seeds: names and `unit_key` from `kpi.csv`, definitions from its `definition_pl`/`definition_en`
(required for a published concept with a v2 key), `frequency`, `aggregation` and `polarity` from `kpi_v2_map.csv`
(one value per v2 key), `domain_key`/`group_key` by walking `domain.csv` up from the concept's `subgroup_key`,
`decimals` from `unit_decimals.csv`, member names and order from `kpi_member.csv` (else `sex.csv` for sex, else the
member key), geographies from `geography_v2.csv` (`eu`: the area is in the EU). `source_keys` are the sources with
data, preferred first (best `kpi_measure.csv` priority of the concept, then map order). `first_period`/`last_period`
are the ends of the KPI's `series.json` periods; a v2 KPI without any row at its frequency is left out and reported.

## latest.json (one file for all KPIs)

| field | type | meaning |
|---|---|---|
| version, generated_at | | as above |
| values[] | array | one entry per KPI × geography (totals only, no breakdowns) |
| values[].kpi_key, geography | string | |
| values[].source_key | string | the source shown (preferred source with data for that geography) |
| values[].unit_key | string | unit of the value (see Units above) |
| values[].period, value | string, number | newest period and value |
| values[].previous_period, previous_value | string, number or null | the period before with a value |
| values[].year_ago_value | number or null | same period a year earlier (monthly and quarterly KPIs) |
| values[].flags | array | source status flags (`p` provisional, `e` estimated, `b` break) |
| values[].checked_at | string | when the source was last checked |

## kpi/<kpi_key>/series.json

Columnar to stay small; the reader filters by geography, source and members.

| field | type | meaning |
|---|---|---|
| kpi_key, frequency, unit_key | string | |
| periods | array | every period of the KPI, sorted |
| series[] | array | one entry per geography × source × member combination |
| series[].geography, source_key, unit_key | string | |
| series[].members | object | `{ dimension_key: member_key }`; `{}` = totals |
| series[].values | array | one number or null per entry of `periods` |
| series[].flags | object | optional, `{ period: "p" }` only where a flag exists |

Size: ≤ 1 MB gzipped per KPI. A KPI whose full breakdown would exceed it publishes only the members listed for it
in `kpi_member.csv` (the curated members a reader needs: e.g. the NACE sections, not every class); `gold.publish`
fails on a file over the limit.

Only rows of the KPI's shown frequency are in the file (daily rows as the monthly mean, 6 decimals, when it is
monthly), so a source that has the KPI only at another frequency (e.g. World Bank annual rates of a quarterly KPI)
has no series in v2 yet. Per geography × source × members one measure is kept: the best `kpi_measure.csv` priority,
then the lowest measure key. Two different Gold values for one period of a series fail the run (equal duplicates
count once).

## calendar.json

`{ version, generated_at, releases: [{ date, source_key, kpi_keys, title_pl, title_en, url }] }` from
`release_calendar.csv` (every row; `kpi_keys` = the v2 key of its concept when that KPI is in v2) and the
`silver.releases` rows that publish a v2 KPI (`source_key` = publisher, the one title in both fields), dates today and
later; rows of one date, source, title and URL are merged; `url` may be null. `catalog.json` `next_release` is the
KPI's first date here. `feed/calendar.ics` (v1) stays.

## topics.json

What is being talked about, ranked per period (owner decision O32; method in [topics.md](topics.md)). Written by
`gold.publish` from `gold.topic_rank` once the owner has switched topics on for the portals (`topics.publish: true`
in `warehouse.yaml`); until then the file is not written.

| field | type | meaning |
|---|---|---|
| version, generated_at | | as above |
| periods | object | keys `day`, `week`, `month`, `quarter`, `year` |
| periods.<p>.window | object | `{ from, to }` dates of the window |
| periods.<p>.topics[] | array | at most 10, ranked |
| …topics[].topic_key, name_pl, name_en | string | |
| …topics[].rank, heat | integer, number | rank in the period, heat score (2 decimals) |
| …topics[].trend | string | `up`, `down`, `flat` against the previous window |
| …topics[].kpi_keys | array | v2 KPI keys linked to the topic, `main` ones first; may be empty |
| …topics[].headlines[] | array | at most 5, newest first: `{ title, url, outlet, lang, published_at }` |
| …topics[].next_release | object or null | `{ date, source_key, title_pl, title_en }` of the next release of a main KPI (`source_key` = the publisher's key, e.g. `eurostat`, `gus`, `ecb`, or the seed row's source; official calendars publish the title in one language, given in both fields) |
| …topics[].signals | object | `{ news, volume, attention }`: the z-scores behind the heat, for "why is this hot" |

Size: ≤ 150 kB gzipped. Headlines are links with the outlet's name; no article text is ever in the file. Schema
`warehouse/published/schemas/v2/topics.schema.json`, fixture `warehouse/published/fixtures/v2/topics.json`. Until
`topics.publish` is true the same document goes to the internal review copy `06_published/v1/review/topics.json`
(not part of this contract; [topics.md](topics.md), "Internal review").

## status.json

As v1 (the same document, copied to v2 so zohelo-bi reads one prefix; schema `schemas/v2/status.schema.json` is a copy
of v1's).

## Size limits

catalog.json ≤ 1 MB gzip · latest.json ≤ 300 kB gzip · series.json ≤ 1 MB gzip each · calendar.json ≤ 50 kB ·
topics.json ≤ 150 kB gzip.

## First slice: coverage

`warehouse/seeds/kpi_v2_map.csv` maps the 78 v2 keys of the slice (Polska w skrócie first, then economy and labour)
to concepts of `kpi.csv` and source measures, **one row per v2 key and source** (owner decision O31, 10 Nov 2026:
every KPI is taken from every source that publishes it, each source its own series; a missing source is added,
never substituted). Columns: `concept_key` (one concept per v2 key; empty when none yet), `source_key`, `dataset`
(`;` separates several), `measure_key_pattern` (as in `kpi_measure.csv`), `frequency` (what the product shows) and
`frequency_available` (what the measures have), `geographies` (`PL`, `EU` member states, `EU27`, `VOIV`
voivodeships), `dimensions`, `aggregation`, `polarity`, `status` and `note`:

- `ready`: the measures are in activated blocks and the concept maps them in `kpi_measure.csv` (one row per source
  measure, units per measure where they differ, e.g. GUS indices `index_py_100` beside Eurostat rates); a
  `metrics_<concept>` table after the next Gold build. New concepts start as `draft` and become `published` once the
  build checks pass.
- `needs-block`: in Silver (codes checked or block named) but not activated: a member filter, a half-yearly fact, a
  derivation or a row-size decision is missing; `note` says which.
- `needs-source`: not ingested; `note` names the register id (NBP balance of payments PLG-002, investment
  position PLG-003). NBP official rates (`nbp_interest_rates`, PLG-006) and the OECD CLI (`oecd`, GL-003) reached
  Silver on 10 Nov (#2590, #2594) and are `needs-block` until their Gold build stages exist.

v2 keys by their best source status:

| | ready | needs-block | needs-source | total |
|---|---|---|---|---|
| Polska w skrócie | 28 | 3 | 0 | 31 |
| all 78 keys | 63 | 14 | 1 | 78 |

Rows by source (141 rows; 31 keys have two or more ready sources):

| source | ready | needs-block | needs-source | rows |
|---|---|---|---|---|
| eurostat | 51 | 7 | 0 | 58 |
| gus_bdl | 17 | 8 | 0 | 25 |
| gus_dbw | 17 | 8 | 0 | 25 |
| world_bank_wdi | 23 | 1 | 0 | 24 |
| nbp | 1 | 0 | 6 | 7 |
| nbp_interest_rates | 0 | 1 | 0 | 1 |
| oecd | 0 | 1 | 0 | 1 |

Gold has a quarterly fact (`fact_quarterly`), and the BDL and DBW builds place monthly and quarterly periods, so
LFS rates, the current account, NAMQ, job vacancies and the GUS short-term series keep their frequency. Polska w
skrócie still needs `nbp_reference_rate` (NBP PLG-006: in Silver, Gold build stage missing), `price_m2_secondary` (the secondary market is a breakdown
member of BDL P3788/P3794 and DBW 309_485: a member filter, `kpi_member.csv`) and `broadband_households` (the
ingested broadband datasets end in 2021). GUS indices (`… = 100`) are published as the source gives them, with the
measure's unit; the rate is the value minus 100. The v2 key of a concept is read from this map (see Rules).

## Who checks what

- zohelo-data: schemas, `gold.publish` reads back every number of `latest.json` and `series.json` (values, previous
  and year-ago values; monthly means recomputed from the daily rows) against Gold (as v1) and fails on a difference;
  `ops.repo_check` checks the fixtures in `warehouse/published/fixtures/v2/` (schemas, `published_v2_problems`) and
  the v2 seeds (`published_v2_seeds`).
- zohelo-bi: its live provider maps v2 one to one onto its data interface (`apps/portal/src/data/types.ts`) and
  tests against the same fixtures (copied, not re-invented).
- The first set of v2 KPIs is the slice in `.github/backlog/epics/published-v2.yaml`.
