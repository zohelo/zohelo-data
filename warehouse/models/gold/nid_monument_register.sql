{{ config(alias='nid_monument_register') }}
{# Gold entity table (register source, playbook section 7): NID register of immovable monuments, register book A
   (register PLS-026), one row per register entry = register_key (list:number:date of the decision, Silver
   nid_monuments_decisions). Attributes as the lists print them: the register number and the decision date, the
   objects the entry covers (object names in list order; numbered items, "jw." items and parts of an ensemble), the
   powiat (geography_key of the powiat when all its objects lie in one, else the voivodeship; powiat_count says
   how many), the localities, the date of the list. No street address, no owner. Joined by the area key, never by
   name; the powiat comes from the page header of the list through poc_map_geography (source nid_monuments). #}
with d as (select * from {{ ref('nid_monuments_decisions') }}),

i as (select * from {{ ref('nid_monuments_items') }}),

geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'nid_monuments'
),

located as (
    select
        d.register_key,
        d.list_code,
        d.woj_code,
        d.register_number,
        d.decision_date,
        d.decision_date_text,
        d.register_role,
        i.item_number,
        i.object_name,
        i.locality_text,
        i.list_state_date,
        coalesce(g.geography_key, '_X') as powiat_key,
        d.source_version,
        d.raw_sha256
    from d
    inner join i on d.list_code = i.list_code and d.item_number = i.item_number
    left join geo as g on g.source_code = {{ poc_nid_monuments_area_code('i.woj_code', 'i.powiat_text') }}
)

select
    register_key,
    list_code,
    'TERC:' || woj_code as voivodeship_key,
    case when count(distinct powiat_key) = 1 then any_value(powiat_key) else 'TERC:' || woj_code end
        as geography_key,
    count(distinct powiat_key) as powiat_count,
    register_number,
    decision_date,
    string_agg(distinct decision_date_text, '; ' order by decision_date_text) as decision_date_text,
    count(distinct item_number) as objects,
    count(distinct item_number) filter (where register_role = 'numbered') as numbered_objects,
    string_agg(object_name, '; ' order by item_number) as object_names,
    count(distinct locality_text) as locality_count,
    string_agg(distinct locality_text, '; ' order by locality_text) as localities,
    max(list_state_date) as list_state_date,
    max(source_version) as source_version,
    max(raw_sha256) as raw_sha256
from located
group by register_key, list_code, woj_code, register_number, decision_date
