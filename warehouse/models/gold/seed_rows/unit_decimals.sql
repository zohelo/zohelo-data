-- Decimals shown per unit (published contract v2).
-- A code list kept as rows in warehouse/seed_rows/unit_decimals/*.csv, one file per source that added rows
-- (docs/source-registration.md, D36); every file has the same header. Read as text, typed here.
{{ config(meta={'seed_rows': 'unit_decimals'}) }}

select
    unit_key,
    cast(decimals as integer) as decimals
from read_csv(
    '{{ invocation_args_dict["project_dir"] }}/seed_rows/unit_decimals/*.csv',
    header = true, all_varchar = true, union_by_name = true, delim = ',', quote = '"', escape = '"'
)
