{# GUGiK PRNG (register PL-ENV-002): the gminas each named object lies in, wholly or partly, as the register publishes
   them (silver.prng_objects.gmina_codes, TERYT gmina codes separated by ';'). Grain: prng_id × gmina_code. With the
   powiat and voivodeship codes of the gmina code (its first 4 and 2 digits) and the code's position in the list. An
   object without a gmina code (e.g. an area of the sea) has no row. A code that is not 7 digits fails the build; a
   code listed twice for one object fails the unique test. #}
with lists as (
    select
        entity,
        prng_id,
        string_split(gmina_codes, ';') as code_list,
        source_version,
        raw_sha256
    from {{ ref('prng_objects') }}
    where gmina_codes is not null
),

codes as (
    select
        entity,
        prng_id,
        trim(unnest(code_list)) as gmina_code,
        unnest(range(1, len(code_list) + 1)) as position,  -- unnests of equal length are zipped
        source_version,
        raw_sha256
    from lists
)

select
    entity,
    prng_id,
    case
        when regexp_full_match(gmina_code, '[0-9]{7}') then gmina_code
        else error('prng_object_gminas: gmina code is not 7 digits: "' || gmina_code || '" (prng_id ' || prng_id || ')')
    end as gmina_code,
    left(gmina_code, 4) as powiat_code,
    left(gmina_code, 2) as voivodeship_code,
    cast(position as integer) as position,
    source_version,
    raw_sha256
from codes
