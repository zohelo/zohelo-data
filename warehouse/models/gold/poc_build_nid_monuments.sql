{{ config(alias='build_nid_monuments', tags=['gold_block']) }}
{# Gold build stage: NID register of immovable monuments, register book A (register PLS-026), counts of the newest
   lists (Silver nid_monuments_items and nid_monuments_decisions): an entity snapshot, one date = the date the lists
   state ("Stan na 30.06.2026"), frequency D as other register snapshots (GLEIF). Block 'register', two measures:
   - nid_monuments:register:monuments:objects - objects in the register: list items with a decision of their own,
     "jw." items and the parts of an ensemble (register_role numbered, same_as_above, component); items without a
     decision (unnumbered) are left out;
   - nid_monuments:register:list_entries:decisions - register entries: distinct register_key (number and date of
     the decision); an entry whose objects lie in two powiats counts in each, once in the voivodeship and Poland.
   Areas: powiat (by name from the page header, macros/sources/nid_monuments.sql), voivodeship, Poland; the Warsaw
   list counts in Warsaw (powiat) and mazowieckie. #}
with items as (
    select * from {{ ref('nid_monuments_items') }}
    where register_role in ('numbered', 'same_as_above', 'component')
),

decisions as (
    select * from {{ ref('nid_monuments_decisions') }}
),

geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'nid_monuments'
),

areas as (  -- each object in its powiat, its voivodeship and Poland
    select
        list_code,
        item_number,
        {{ poc_nid_monuments_area_code('woj_code', 'powiat_text') }} as area_code
    from items
    union all
    select
        list_code,
        item_number,
        'woj:' || woj_code as area_code
    from items
    union all
    select
        list_code,
        item_number,
        'PL' as area_code
    from items
),

counted as (
    select
        a.area_code,
        'monuments' as unit_code,
        'objects' as measure_code,
        count(*) as value
    from areas as a
    group by a.area_code
    union all
    select
        a.area_code,
        'list_entries' as unit_code,
        'decisions' as measure_code,
        count(distinct d.register_key) as value
    from areas as a
    inner join decisions as d on a.list_code = d.list_code and a.item_number = d.item_number
    group by a.area_code
),

snapshot as (
    select
        max(list_state_date) as state_date,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from {{ ref('nid_monuments_items') }}
)

select
    'nid_monuments' as source_system,
    'register' as block_id,
    'NID register of immovable monuments (register book A)' as block_name,
    'nid_monuments:register:' || c.unit_code || ':' || c.measure_code as measure_key,
    case c.measure_code
        when 'objects' then 'Immovable monuments in the register (objects listed)'
        when 'decisions' then 'Entries in the register of immovable monuments (decisions)'
    end as measure_name,
    c.measure_code as measure_codes,
    c.unit_code,
    case c.unit_code when 'monuments' then 'monuments' when 'list_entries' then 'register entries' end as unit_name,
    c.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'D' as frequency,
    s.state_date as date_key,
    cast(s.state_date as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(c.value as double) as value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from counted as c
cross join snapshot as s
left join geo as g on c.area_code = g.source_code
