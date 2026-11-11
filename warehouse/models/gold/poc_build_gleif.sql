{{ config(alias='build_gleif') }}
{# Gold build stage (#2740): GLEIF LEI counts, an entity snapshot of the newest golden copy
   (silver.gleif_legal_entities, latest-only: one date, the golden copy's publication day). Measure =
   LEI records by registration status (measure_key gleif:lei2:entities:<ISSUED|LAPSED|RETIRED|…|_T>);
   breakdowns entity_status, legal_form (ISO 20275 ELF code as published) and entity_category, each
   broken down alone with the others at _T (no cross-tabulation, so series stay few); _U = not stated.
   Geography: the country of the legal jurisdiction (US-DE → US) and, for entities under Polish
   jurisdiction with a Polish legal address, the voivodeship of that address (ISO 3166-2 PL-<TERC>,
   legacy letter codes included), both through map_geography; voivodeships do not add up to Poland
   when the region is missing (about 5 %). #}
with entities as (
    select
        substr(legal_jurisdiction, 1, 2) as country_code,
        case
            when legal_jurisdiction = 'PL' and legal_address_country = 'PL' then legal_address_region
        end as region_code,
        coalesce(registration_status, '_U') as registration_status,
        coalesce(entity_status, '_U') as entity_status,
        coalesce(legal_form_code, '_U') as legal_form,
        coalesce(entity_category, '_U') as entity_category,
        source_version,
        raw_sha256
    from {{ source('silver_entities', 'gleif_legal_entities') }}
    where legal_jurisdiction is not null
),

located as (
    select
        country_code as geo_code,
        registration_status,
        entity_status,
        legal_form,
        entity_category
    from entities
    union all
    select
        region_code as geo_code,
        registration_status,
        entity_status,
        legal_form,
        entity_category
    from entities
    where region_code is not null
),

{% set dims = ['registration_status', 'entity_status', 'legal_form', 'entity_category'] %}
{% set cuts = [[], ['entity_status'], ['legal_form'], ['entity_category']] %}
counted as (  -- each breakdown alone (others _T), with and without the registration status
    {% for with_status in [false, true] %}{% for cut in cuts %}
        {% set kept = (['registration_status'] if with_status else []) + cut %}
        {% if not loop.first or with_status %}union all{% endif %}
        select
            geo_code,
            {% for d in dims %}
                {{ d if d in kept else "'_T'" }} as {{ d }},
            {% endfor %}
            count(*) as entities
        from located
        group by geo_code{% for d in kept %}, {{ d }}{% endfor %}
    {% endfor %}{% endfor %}
),

snapshot as (
    select
        max(source_version) as source_version,
        any_value(raw_sha256) as raw_sha256
    from entities
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'gleif'
)

select
    'gleif' as source_system,
    'lei2' as block_id,
    'GLEIF LEI records (golden copy)' as block_name,
    'gleif:lei2:entities:' || c.registration_status as measure_key,
    'Legal entities with an LEI, registration status ' || c.registration_status as measure_name,
    c.registration_status as measure_codes,
    'entities' as unit_code,
    'legal entities' as unit_name,
    c.geo_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'D' as frequency,
    cast(strptime(s.source_version, '%Y%m%dT%H%M%SZ') as date) as date_key,
    cast(cast(strptime(s.source_version, '%Y%m%dT%H%M%SZ') as date) as varchar) as period_label,
    cast(json_object(
        'entity_category', c.entity_category, 'entity_status', c.entity_status, 'legal_form', c.legal_form
    ) as varchar) as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    c.entity_category = '_T' and c.entity_status = '_T' and c.legal_form = '_T' as is_total,
    cast(c.entities as double) as value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from counted as c
cross join snapshot as s
left join geo_map as g on c.geo_code = g.source_code
