# Published layer contract v1 — field lists (architect, 27 Oct 2026)

Source for Task `data/published/published-v1/contract/doc`: docs/published.md presents each list below as a
field table (name, type, required, meaning). Decision D5: unit_key everywhere; geo levels country,
voivodeship, powiat, gmina, nuts1, nuts2; warehouse/publish.py is the only writer of these files.

## Rules

- Prefix `06_published/v1/` in bucket STORAGE_LAKEHOUSE_BUCKET_NAME; rebuilt whole by gold.publish after each Gold build; no history.
- UTF-8; timestamps ISO 8601 UTC; additive fields are allowed in v1; any rename or removal needs v2 alongside v1.
- Keys: geography_key as in Gold ('PL', 'TERC:<code>', 'NUTS:<code>'); kpi_key = concept_key (e.g. unemployment_rate); unit_key from warehouse/seeds/unit.csv.
- All fields below are required unless marked (optional).

## catalog/catalog.json

Top level: version (int, 1), generated_at, kpis[], domains[], units[], sources[], licences[].
- kpis[]: kpi_key, name_pl, name_en, unit_key, frequency_default, subgroup_key, domain_path (array of keys domain → group → subgroup), source_keys[], geo_levels[], first_period, last_period, dimensions[] (dimension_key, member_count, levels[]), loaded_at, status.
- domains[]: the columns of warehouse/seeds/domain.csv. units[]: the columns of warehouse/seeds/unit.csv.
- sources[]: source_key, name_pl, name_en, licence_key, register_id. licences[]: licence_key, name, url, attribution_text, commercial_ok.

## catalog/<table>.parquet

One file per Gold catalogue table with that table's columns: dim_kpi, kpi_measure, kpi_dimension, dim_domain, dim_unit, dim_source, dim_licence, freshness.

## manifest.json

Top level: version (1), generated_at, metrics[].
- metrics[]: kpi_key, table ('gold.<name>'), unit_key, frequency, time_column, value_column, source_column, dimensions[] (dimension_key, column), default_filters (object column → value; optional).

## kpi/<kpi_key>/latest.json

Top level: kpi_key, unit_key, generated_at, values[] — one entry per source × geography at levels country and voivodeship.
- values[]: source_key, geography_key, geography_level, period, value (number), previous_period (optional), previous_value (optional), flags[], checked_at.

## kpi/<kpi_key>/series.parquet

Columns: kpi_key, source_key, measure_key, geography_key, geography_level, period (string: '2025', '2025-03', '2025-03-14'), period_start (date), frequency, value (double), unit_key, flags (string), plus one string column dim_<dimension_key> per extra dimension ('_T' = total).

## geo/<level>.geojson

level ∈ country, voivodeship, powiat, gmina, nuts1, nuts2. FeatureCollection; Feature properties geography_key, name_pl, name_en, level, parent_key (optional for country). Coordinates simplified.

## feed/changes.json and feed/calendar.ics

changes.json top level: generated_at, items[] (last 30 days). items[]: at, source_key, dataset_key, kpi_keys[], title_pl, title_en, kind ('data_update').
calendar.ics: iCalendar, one VEVENT per release_calendar row (summary = title_pl, DTSTART = release_at, URL); zero events until that table exists.

## Size limits

catalog.json ≤ 2 MB gzip · latest.json ≤ 20 kB · changes.json ≤ 200 kB · series.parquet ≤ 20 MB · geo files ≤ 2 MB (gmina ≤ 8 MB).
