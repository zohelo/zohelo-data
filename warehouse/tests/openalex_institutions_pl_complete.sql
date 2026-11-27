-- openalex: OpenAlex placed 1,352 institutions in Poland on 26 Nov 2026 (Dev probe 45816100460). A list cut short
-- (fewer than 1,200), an id that is not an OpenAlex institution id, or an institution whose parents name itself fails
-- here. One test per Silver model (it reads only this model).
select
    'institutions in Poland' as problem,
    count(*) as found
from {{ ref('openalex_institutions_pl') }}
having count(*) < 1200
union all
select
    'not an OpenAlex institution id: ' || institution_id as problem,
    1 as found
from {{ ref('openalex_institutions_pl') }}
where not regexp_full_match(institution_id, 'I[0-9]+')
union all
select
    'parent list names the institution: ' || institution_id as problem,
    1 as found
from {{ ref('openalex_institutions_pl') }}
where list_contains(from_json(parent_ids_json, '["VARCHAR"]'), institution_id)
