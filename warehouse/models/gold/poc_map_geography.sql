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
            when u.level = 1
                then
                    case
                        when upper(u.name) like '%MAZOWIECKIE%' then 'TERC:14'
                        when upper(u.name) like '%POŁUDNIOWO-ZACHODNI%' then 'NUTS:PL5'
                        when upper(u.name) like '%PÓŁNOCNO-ZACHODNI%' then 'NUTS:PL4'
                        when upper(u.name) like '%POŁUDNIOWY%' then 'NUTS:PL2'
                        when upper(u.name) like '%PÓŁNOCNY%' then 'NUTS:PL6'
                        when upper(u.name) like '%CENTRALNY%' then 'NUTS:PL7'
                        when upper(u.name) like '%WSCHODNI%' then 'NUTS:PL8'
                        else '_X'
                    end
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

type_digits as (select unnest(['1', '2', '3']) as digit),

bdl_terc as (  -- the 7-digit zero-padded codes of the web export, generated from TERYT
    select
        'gus_bdl' as source_system,
        rpad(terc_code, 7, '0') as source_code,
        'TERC:' || terc_code as geography_key
    from {{ ref('teryt_terc') }}
    union all
    select
        'gus_bdl' as source_system,
        '0000000' as source_code,
        'PL' as geography_key
    union all
    -- a gmina whose type digit changed since (2 → 3 when a village got town rights): the current unit
    select
        'gus_bdl' as source_system,
        substr(g.terc_code, 1, 6) || t.digit as source_code,
        'TERC:' || g.terc_code as geography_key
    from {{ ref('teryt_terc') }} as g
    cross join type_digits as t
    where g.unit_level = 'gmina' and g.gmina_type_code in ('1', '2', '3') and t.digit <> g.gmina_type_code
),

-- DBW cross-section positions are territory names; matched by name (upper) to Polish areas.
dbw_names as (  -- position labels of the activated DBW blocks (extracted dictionaries)
    select distinct upper(trim(opis)) as source_code
    from {{ ref('poc_src_dbw_dictionary') }}
    where lower(nazwa_kolumny) like 'id_pozycja_%'
),

dbw as (
    select
        'gus_dbw' as source_system,
        d.source_code,
        coalesce(
            case when d.source_code = 'POLSKA' then 'PL' end,
            case when d.source_code like 'MAKROREGION WOJEW%MAZOWIECKIE' then 'TERC:14' end,
            g_woj.geography_key,
            g_nuts.geography_key,
            g_pod.geography_key,
            '_X'
        ) as geography_key
    from dbw_names as d
    left join geo as g_woj
        on g_woj.level_name = 'wojewodztwo' and upper(g_woj.name_pl) = regexp_replace(d.source_code, '^REGION ', '')
    left join geo as g_nuts
        on
            g_nuts.level_name in ('makroregion', 'region')
            and regexp_replace(upper(g_nuts.name_pl), '^(MAKROREGION|REGION) ', '')
            = regexp_replace(d.source_code, '^(MAKROREGION|REGION) ', '')
    left join geo as g_pod
        on
            g_pod.level_name = 'podregion'
            and regexp_replace(regexp_replace(upper(g_pod.name_pl), '^PODREGION ', ''), '^(M\. ST\. |M\. |MIASTO )', '')
            = regexp_replace(regexp_replace(d.source_code, '^PODREGION ', ''), '^(M\. ST\. |M\. |MIASTO )', '')
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
    m.source_system,
    m.source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    g.geography_key is not null as matched
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
) as m
left join geo as g on m.geography_key = g.geography_key and g.geography_key <> '_X'
where m.source_code is not null
qualify row_number() over (partition by m.source_system, m.source_code order by g.geography_key nulls last) = 1
