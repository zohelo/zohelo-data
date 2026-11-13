-- KPI concepts: names, unit, domain subgroup, default frequency, status, definitions.
-- A code list kept as rows in warehouse/seed_rows/kpi/*.csv, one file per source that added rows
-- (docs/source-registration.md, D36); every file has the same header. Read as text, typed here.
{{ config(meta={'seed_rows': 'kpi'}) }}

select
    kpi_key,
    name_en,
    name_pl,
    unit_key,
    subgroup_key,
    frequency_default,
    status,
    definition_en,
    definition_pl
from read_csv(
    '{{ invocation_args_dict["project_dir"] }}/seed_rows/kpi/*.csv',
    header = true, all_varchar = true, union_by_name = true, delim = ',', quote = '"', escape = '"'
)
