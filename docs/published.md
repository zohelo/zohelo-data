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
- All fields below are required unless marked optional.

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

`warehouse/catalog.py` `manifest()` builds this object from the dbt semantic models and `kpi.csv`; a published KPI without a semantic model is left out.

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
| values[].previous_period | string | optional | the period before |
| values[].previous_value | number | optional | value in the period before |
| values[].flags | array | yes | status flags of the value |
| values[].checked_at | string | yes | when the source was last checked (ISO 8601 UTC) |

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
| unit_key | string | yes | unit |
| flags | string | yes | status flags |
| dim_<dimension_key> | string | yes | one column per extra dimension; `'_T'` = total |

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

## Size limits

| file | limit |
|---|---|
| catalog/catalog.json | ≤ 2 MB gzip |
| kpi/<kpi_key>/latest.json | ≤ 100 kB |
| feed/changes.json | ≤ 200 kB |
| kpi/<kpi_key>/series.parquet | ≤ 20 MB |
| geo/<level>.geojson | ≤ 2 MB (gmina ≤ 8 MB) |
