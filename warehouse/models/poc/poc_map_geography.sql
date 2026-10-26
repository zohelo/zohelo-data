{{ config(alias='map_geography') }}
{# Gold PoC: every source's geography code → the area it means in dim_geography
   (design draft 2: map_geography__<source>, merged into one table). Unmatched codes map to
   '_X' and are counted by the build check. #}
with geo as (select * from {{ ref('poc_dim_geography') }}),

nuts_pl as ({{ poc_nuts_pl() }}),

eurostat_codes as (
    select code from {{ ref('eurostat_codes') }}
    where codelist_id = 'GEO'
    group by code
),

eurostat as (
    select
        'eurostat' as source_system,
        c.code as source_code,
        coalesce(
            case when c.code in ('EL', 'UK') then (case c.code when 'EL' then 'GR' else 'GB' end) end,
            case when n.woj_code is not null and n.level_name = 'wojewodztwo' then 'TERC:' || n.woj_code end,
            case when n.nuts_code is not null then 'NUTS:' || n.nuts_code end,
            g_country.geography_key,
            g_nuts.geography_key,
            g_agg.geography_key,
            '_X'
        ) as geography_key
    from eurostat_codes as c
    left join nuts_pl as n on c.code = n.nuts_code
    left join geo as g_country on c.code = g_country.geography_key and g_country.level_name = 'country'
    left join geo as g_nuts on g_nuts.geography_key = 'NUTS:' || c.code
    left join geo as g_agg on g_agg.geography_key = 'EUROSTAT:' || c.code
),

-- BDL: the web export keys rows by a 7-digit zero-padded TERYT code (0000000 Polska, WW00000
-- województwo, WWPP000 powiat, WWPPGGT gmina); the API by a 12-character unit id (bdl_units),
-- mapped here by level and name (v1: the GUS KTS–TERYT correspondence table).
terc_woj as (
    select
        terc_code,
        upper(name_pl) as name_u
    from {{ ref('teryt_terc') }}
    where unit_level = 'wojewodztwo'
),

terc_pow as (
    select
        terc_code,
        woj_code,
        lower(name_pl) as name_l,
        unit_type_pl
    from {{ ref('teryt_terc') }}
    where unit_level = 'powiat'
),

terc_gmi as (
    select
        terc_code,
        woj_code,
        pow_code,
        lower(name_pl) as name_l,
        gmina_type_code
    from {{ ref('teryt_terc') }}
    where unit_level = 'gmina'
),

bdl_units as (select * from {{ ref('bdl_units') }}),

bdl as (
    select
        'gus_bdl' as source_system,
        u.unit_id as source_code,
        case
            when u.level = 0 then 'PL'
            when u.level = 1 then coalesce('NUTS:' || n1.nuts_code, '_X')
            when u.level = 2 then coalesce('TERC:' || w.terc_code, '_X')
            when u.level = 3 and upper(u.name) like '%WARSZAWSKI STO%' then 'NUTS:PL91'
            when u.level = 3 and upper(u.name) like '%MAZOWIECKI REGIONALNY%' then 'NUTS:PL92'
            when u.level = 3 then coalesce('TERC:' || w3.terc_code, '_X')
            when u.level = 4 then 'BDL:' || u.unit_id
            when u.level = 5 then coalesce('TERC:' || p.terc_code, '_X')
            when u.level = 6 then coalesce('TERC:' || g.terc_code, '_X')
            else '_X'
        end as geography_key
    from bdl_units as u
    left join nuts_pl as n1 on n1.level_name = 'makroregion' and upper(n1.name_pl) = upper(u.name)
    left join terc_woj as w on w.name_u = upper(u.name) and u.level = 2
    left join terc_woj as w3 on w3.name_u = upper(regexp_replace(u.name, '^(?i)region ', '')) and u.level = 3
    left join terc_pow as p
        on
            u.level = 5
            and p.woj_code = substr(u.unit_id, 3, 2)
            and p.name_l = lower(regexp_replace(u.name, '^(?i)powiat (m\.\s*st\.\s*|m\.\s*)?', ''))
    left join terc_gmi as g
        on
            u.level = 6
            and g.woj_code = substr(u.unit_id, 3, 2)
            and g.name_l
            = lower(
                regexp_replace(u.name, '\s*\((miasto|obszar wiejski|gmina (miejska|wiejska|miejsko-wiejska))\)$', '')
            )
            and g.gmina_type_code = substr(u.unit_id, 12, 1)
),

bdl_terc as (  -- the 7-digit codes of the web export
    select distinct
        'gus_bdl' as source_system,
        o.unit_code as source_code,
        case
            when o.unit_code = '0000000' then 'PL'
            when regexp_matches(o.unit_code, '^[0-9]{2}00000$') then 'TERC:' || substr(o.unit_code, 1, 2)
            when regexp_matches(o.unit_code, '^[0-9]{4}000$') then 'TERC:' || substr(o.unit_code, 1, 4)
            when regexp_matches(o.unit_code, '^[0-9]{7}$') then 'TERC:' || o.unit_code
            else '_X'
        end as geography_key
    from {{ source('silver_entities', 'bdl_observations') }} as o
    where o.unit_code_scheme = 'TERC' and o.subgroup_id in (select b.subgroup_id from ({{ poc_bdl_blocks() }}) as b)
),

-- DBW cross-section positions are territory names; matched by name (upper) to Polish areas.
dbw_names as (
    select distinct upper(trim(opis)) as source_code
    from {{ source('gus_dbw', 'dbw_dictionary_items') }}
    where
        lower(nazwa_kolumny) like 'id_pozycja_%'
        and regexp_replace(entity_id, '_[0-9]{4}_pl$', '') in (select b.block_id from ({{ poc_dbw_blocks() }}) as b)
),

dbw as (
    select
        'gus_dbw' as source_system,
        d.source_code,
        coalesce(
            case when d.source_code = 'POLSKA' then 'PL' end,
            g_woj.geography_key,
            g_nuts.geography_key,
            g_pod.geography_key,
            '_X'
        ) as geography_key
    from dbw_names as d
    left join geo as g_woj
        on g_woj.level_name = 'wojewodztwo' and upper(g_woj.name_pl) = d.source_code
    left join geo as g_nuts
        on
            g_nuts.level_name in ('makroregion', 'region')
            and upper(g_nuts.name_pl) = regexp_replace(d.source_code, '^(MAKROREGION|REGION) ', '')
    left join geo as g_pod
        on
            g_pod.level_name = 'podregion'
            and regexp_replace(upper(g_pod.name_pl), '^PODREGION ', '')
            = regexp_replace(d.source_code, '^PODREGION ', '')
),

wdi as (
    select
        'world_bank_wdi' as source_system,
        w.economy_code as source_code,
        coalesce(g.geography_key, g_agg.geography_key, '_X') as geography_key
    from {{ ref('wdi_countries') }} as w
    left join geo as g on g.level_name = 'country' and w.iso2_code = g.iso2
    left join geo as g_agg on g_agg.geography_key = 'WB:' || w.economy_code
)

select
    source_system,
    source_code,
    geography_key,
    geography_key <> '_X' as matched
from (
    select * from eurostat
    union all
    select * from bdl
    union all
    select * from bdl_terc
    union all
    select * from dbw
    union all
    select * from wdi
) as mapped
qualify row_number() over (partition by source_system, source_code order by geography_key) = 1
