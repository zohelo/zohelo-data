{{ config(alias='geo_unit_wikidata') }}
{# Gold entity table (reference source, playbook section 7): Wikidata (CC0) attributes of Polish voivodeships,
   powiats and gminas, one row per TERC unit Wikidata holds, keyed like dim_geography ('TERC:<code>'); joined by
   the official code, never by name. Values as Wikidata publishes them, chosen in silver.wikidata_terc_attributes
   (preferred rank, latest point in time): population with its point in time, area in its unit (km2 or ha),
   coordinates of the item (not a computed centroid: gold.geo_unit has PRG's), official website, Wikipedia
   articles pl/en. population_candidates > 1 means other population statements were set aside; qid_candidates > 1 means
   other items carry the same code and this one is the only preferred-rank one. #}
with a as (select * from {{ ref('wikidata_terc_attributes') }})

select
    'TERC:' || terc_code as geography_key,
    terc_code,
    any_value(unit_level) as level_name,
    any_value(qid) as wikidata_qid,
    max(candidates) filter (where property = 'qid') as qid_candidates,
    max(value_text) filter (where property = 'label_pl') as label_pl,
    max(value_text) filter (where property = 'label_en') as label_en,
    max(value_number) filter (where property = 'population') as population,
    max(point_in_time) filter (where property = 'population') as population_date,
    max(candidates) filter (where property = 'population') as population_candidates,
    max(value_number) filter (where property = 'area') as area,
    max(
        case unit_qid when 'Q712226' then 'km2' when 'Q35852' then 'ha' else unit_qid end
    ) filter (where property = 'area') as area_unit,
    max(lon) filter (where property = 'coordinates') as lon,
    max(lat) filter (where property = 'coordinates') as lat,
    max(value_text) filter (where property = 'website') as website,
    max(value_text) filter (where property = 'wikipedia_pl') as wikipedia_pl,
    max(value_text) filter (where property = 'wikipedia_en') as wikipedia_en,
    max(source_version) as source_version
from a
group by terc_code
