-- Sources (`source_key` is the source_system id): register entry, names, publisher, family, licence.
-- A code list kept as rows in warehouse/seed_rows/source/*.csv, one file per source that added rows
-- (docs/source-registration.md, D36); every file has the same header. Read as text, typed here.
{{ config(meta={'seed_rows': 'source'}) }}

select
    source_key,
    register_id,
    name_pl,
    name_en,
    publisher,
    family,
    block_kind,
    provider_type,
    licence_key,
    url
from read_csv(
    '{{ invocation_args_dict["project_dir"] }}/seed_rows/source/*.csv',
    header = true, all_varchar = true, union_by_name = true, delim = ',', quote = '"', escape = '"'
)
