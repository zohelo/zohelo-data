{# GUGiK PRNG (register PL-ENV-002): the gmina of each located point of a named object, as the register publishes it
   (silver.prng_objects.gmina_codes, TERYT gmina codes separated by ';'). The register gives one code per point of the
   object's representation: one for a main point ('punkt główny'), two for a linear object (its start point, then its
   end point), so a river whose two ends lie in one gmina lists that code twice (10,615 objects in the export of
   12 Nov 2026, Dev run 43806029951). Grain: prng_id × gmina_code, with the points that lie in that gmina
   (point_roles: 'main', 'start', 'end', in that order) and the powiat and voivodeship codes of the gmina code (its
   first 4 and 2 digits). These are the gminas of the published points, not every gmina the object crosses. An object
   without a gmina code (e.g. an area of the sea) has no row. A code that is not 7 digits, or a number of codes that
   does not match the representation kind, fails the build. #}
with lists as (
    select
        entity,
        prng_id,
        representation_kind,
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
        representation_kind,
        len(code_list) as code_count,
        trim(unnest(code_list)) as gmina_code,
        unnest(range(1, len(code_list) + 1)) as point_position,  -- unnests of equal length are zipped
        source_version,
        raw_sha256
    from lists
),

points as (
    select
        entity,
        prng_id,
        case
            when regexp_full_match(gmina_code, '[0-9]{7}') then gmina_code
            else error(
                'prng_object_gminas: gmina code is not 7 digits: "' || gmina_code || '" (prng_id ' || prng_id || ')'
            )
        end as gmina_code,
        point_position,
        case
            when representation_kind = 'punkt główny' and code_count = 1 then 'main'
            when
                representation_kind = 'punkt początkowy obiektu; punkt końcowy obiektu'
                and code_count = 2
                then case point_position when 1 then 'start' else 'end' end
            else error(
                'prng_object_gminas: ' || code_count || ' gmina codes for representation "'
                || coalesce(representation_kind, 'null') || '" (prng_id ' || prng_id || ')'
            )
        end as point_role,
        source_version,
        raw_sha256
    from codes
)

select
    entity,
    prng_id,
    gmina_code,
    left(gmina_code, 4) as powiat_code,
    left(gmina_code, 2) as voivodeship_code,
    list(point_role order by point_position) as point_roles,
    source_version,
    raw_sha256
from points
group by entity, prng_id, gmina_code, source_version, raw_sha256
