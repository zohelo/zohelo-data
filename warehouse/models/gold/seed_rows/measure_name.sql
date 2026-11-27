-- Bilingual measure names (O47), written by batch-labels.yml step collect (ops/measure_names.py) and reviewed in
-- its pull request; unclear = true where the source's label was too opaque to name with confidence.
-- A code list kept as rows in warehouse/seed_rows/measure_name/*.csv, one file per source (docs/source-registration.md,
-- D36); _shared.csv holds the header only. Read as text, typed here. Nothing joins it yet (the dim_measure change
-- is a later pull request).
{{ config(meta={'seed_rows': 'measure_name'}) }}

select
    measure_key,
    name_pl,
    name_en,
    cast(unclear as boolean) as unclear
from read_csv(
    '{{ invocation_args_dict["project_dir"] }}/seed_rows/measure_name/*.csv',
    header = true, all_varchar = true, union_by_name = true, delim = ',', quote = '"', escape = '"'
)
