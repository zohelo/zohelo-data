-- Source unit codes to units (D33).
-- A code list kept as rows in warehouse/seed_rows/unit_map/*.csv, one file per source that added rows
-- (docs/source-registration.md, D36); every file has the same header. Read as text, typed here.
{{ config(meta={'seed_rows': 'unit_map'}) }}

select
    source_system,
    unit_code,
    measure_key_pattern,
    unit_key
from read_csv(
    '{{ invocation_args_dict["project_dir"] }}/seed_rows/unit_map/*.csv',
    header = true, all_varchar = true, union_by_name = true, delim = ',', quote = '"', escape = '"'
)
