-- Which source measures feed each KPI concept, with priority.
-- A code list kept as rows in warehouse/seed_rows/kpi_measure/*.csv, one file per source that added rows
-- (docs/source-registration.md, D36); every file has the same header. Read as text, typed here.
{{ config(meta={'seed_rows': 'kpi_measure'}) }}

select
    kpi_key,
    source_system,
    measure_key_pattern,
    definition,
    cast(priority as integer) as priority,
    unit_key
from read_csv(
    '{{ invocation_args_dict["project_dir"] }}/seed_rows/kpi_measure/*.csv',
    header = true, all_varchar = true, union_by_name = true, delim = ',', quote = '"', escape = '"'
)
