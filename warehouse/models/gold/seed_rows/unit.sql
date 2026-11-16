-- Units with base unit and factor (D33).
-- A code list kept as rows in warehouse/seed_rows/unit/*.csv, one file per source that added rows
-- (docs/source-registration.md, D36); every file has the same header. Read as text, typed here.
{{ config(meta={'seed_rows': 'unit'}) }}

select
    unit_key,
    symbol,
    name_en,
    name_pl,
    cast(is_rate as boolean) as is_rate,
    base_unit_key,
    cast(factor as decimal(38, 9)) as factor
from read_csv(
    '{{ invocation_args_dict["project_dir"] }}/seed_rows/unit/*.csv',
    header = true, all_varchar = true, union_by_name = true, delim = ',', quote = '"', escape = '"'
)
