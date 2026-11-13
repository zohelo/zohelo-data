-- Licences and attribution texts of the sources (licence gate, O37).
-- A code list kept as rows in warehouse/seed_rows/licence/*.csv, one file per source that added rows
-- (docs/source-registration.md, D36); every file has the same header. Read as text, typed here.
{{ config(meta={'seed_rows': 'licence'}) }}

select
    licence_key,
    name,
    url,
    attribution_text,
    cast(commercial_ok as boolean) as commercial_ok,
    cast(share_alike as boolean) as share_alike
from read_csv(
    '{{ invocation_args_dict["project_dir"] }}/seed_rows/licence/*.csv',
    header = true, all_varchar = true, union_by_name = true, delim = ',', quote = '"', escape = '"'
)
