{# UNHCR Refugee Statistics API, population endpoint (bronze.unhcr_population): people under UNHCR's mandate by
   country of asylum (coa), country of origin (coo) and year, the newest Landing copy of each selection (entity
   population_by_asylum: every country of asylum, origins summed, coo = '-'; poland_by_origin: Poland by every
   country of origin). Grain: entity × year × coa × coo (UNHCR's own codes; the ISO alpha-3 codes are kept beside
   them). Stocks at the end of the year, persons, as published. "-" (no figure for that population type) becomes
   null, never 0; any other text that is not a whole number fails the build (no silent zeros). A duplicated row
   fails the unique test. tests/unhcr_selection.sql checks each entity holds only its declared selection. #}
{% set measures = ['refugees', 'asylum_seekers', 'returned_refugees', 'idps', 'returned_idps', 'stateless', 'ooc', 'oip', 'hst'] %}
with newest as (
    select *
    from {{ source('unhcr', 'unhcr_population') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    entity,
    cast(year as integer) as year,
    trim(coa) as coa,
    trim(coa_iso) as coa_iso,
    trim(coa_name) as coa_name,
    trim(coo) as coo,
    trim(coo_iso) as coo_iso,
    trim(coo_name) as coo_name,
    {%- for m in measures %}
    case
        when trim({{ m }}) = '-' then null
        when regexp_full_match(trim({{ m }}), '[0-9]+') then cast(trim({{ m }}) as bigint)
        else error('unhcr_population: {{ m }} is not a whole number: ' || coalesce({{ m }}, 'null'))
    end as {{ m }},
    {%- endfor %}
    raw_sha256,
    source_version,
    ingested_at,
    source_row_number
from newest
