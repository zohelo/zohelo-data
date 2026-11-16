-- Published contract v2: v2 KPI key and source to concept and measures (O31).
-- A code list kept as rows in warehouse/seed_rows/kpi_v2_map/*.csv, one file per source that added rows
-- (docs/source-registration.md, D36); every file has the same header. Read as text, typed here.
{{ config(meta={'seed_rows': 'kpi_v2_map'}) }}

select
    v2_key,
    dashboards,
    frequency,
    concept_key,
    source_key,
    dataset,
    measure_key_pattern,
    frequency_available,
    geographies,
    dimensions,
    aggregation,
    polarity,
    status,
    note
from read_csv(
    '{{ invocation_args_dict["project_dir"] }}/seed_rows/kpi_v2_map/*.csv',
    header = true, all_varchar = true, union_by_name = true, delim = ',', quote = '"', escape = '"'
)
