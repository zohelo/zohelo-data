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

-- MRPiPS registered unemployment: the rate workbook's WOJ. and POW. columns as one 4-digit code (WWPP; WW00 the
-- voivodeship, 0000 Poland), generated from TERYT like the BDL codes.
mrpips as (
    select
        'mrpips_unemployment' as source_system,
        rpad(terc_code, 4, '0') as source_code,
        'TERC:' || terc_code as geography_key
    from {{ ref('teryt_terc') }}
    where unit_level in ('wojewodztwo', 'powiat')
    union all
    select
        'mrpips_unemployment' as source_system,
        '0000' as source_code,
        'PL' as geography_key
),

wdi as (
    select
        'world_bank_wdi' as source_system,
        w.economy_code as source_code,
        coalesce(g.geography_key, g_agg.geography_key, '_X') as geography_key
    from {{ ref('wdi_countries') }} as w
    left join geo as g on g.level_name = 'country' and w.iso2_code = g.iso2
    left join geo as g_agg on g_agg.geography_key = 'WB:' || w.economy_code
),

-- OECD (#2738): countries by ISO 3166 alpha-3 (dim_geography iso3); EU27_2020 is the Eurostat aggregate, the
-- aggregates of macro poc_oecd_areas are OECD:<code> members. Built from Gold's own lists, not from
-- silver.oecd_cli, so the dimensions never wait for the OECD source; codes outside them are unmatched (_X).
oecd as (
    select
        'oecd' as source_system,
        g.iso3 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 is not null
    union all
    select
        'oecd' as source_system,
        'EU27_2020' as source_code,
        'EUROSTAT:EU27_2020' as geography_key
    union all
    select
        'oecd' as source_system,
        replace(g.geography_key, 'OECD:', '') as source_code,
        g.geography_key
    from geo as g
    where g.geography_key like 'OECD:%'
),

-- GLEIF (#2740): countries of the legal jurisdiction by ISO 3166-1 alpha-2; Polish legal-address regions by
-- ISO 3166-2, PL-<TERC> since 2018 and the legacy letter codes still found in the golden copy.
gleif as (
    select
        'gleif' as source_system,
        g.iso2 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso2 is not null
    union all
    select
        'gleif' as source_system,
        'PL-' || terc_code as source_code,
        'TERC:' || terc_code as geography_key
    from {{ ref('teryt_terc') }}
    where unit_level = 'wojewodztwo'
    union all
    select
        'gleif' as source_system,
        'PL-' || letters as source_code,
        'TERC:' || terc_code as geography_key
    from (
        values
        ('DS', '02'), ('KP', '04'), ('LU', '06'), ('LB', '08'), ('LD', '10'), ('MA', '12'), ('MZ', '14'),
        ('OP', '16'), ('PK', '18'), ('PD', '20'), ('PM', '22'), ('SL', '24'), ('SK', '26'), ('WN', '28'),
        ('WP', '30'), ('ZP', '32')
    ) as t (letters, terc_code)
),

-- TERYT TERC codes and the PRG unit codes (JPT_KOD_JE: TERC, '0' the country) are TERC keys (#2739).
teryt_prg as (
    select
        'gus_teryt' as source_system,
        terc_code as source_code,
        'TERC:' || terc_code as geography_key
    from {{ ref('teryt_terc') }}
    union all
    select distinct
        'gugik_prg' as source_system,
        teryt_code as source_code,
        case when unit_level = 'kraj' then 'PL' else 'TERC:' || teryt_code end as geography_key
    from {{ ref('prg_units') }}
    where unit_level in ('kraj', 'wojewodztwo', 'powiat', 'gmina')
),

-- World Bank WGI, Findex and ICP (#2736): World Bank economy codes like WDI's, so each gets WDI's mapping (iso2 or
-- WB:<code>), built from wdi_countries only so that this shared model never waits for their Silver tables. A code
-- WDI does not know (a few territories, ICP benchmark groups) is not listed and the build stage maps it to _X.
wb_family as (
    select
        s.source_system,
        w.source_code,
        w.geography_key
    from wdi as w
    cross join (values ('world_bank_wgi'), ('world_bank_findex'), ('world_bank_icp')) as s (source_system)
),

-- UNCTAD: economies by UNCTAD code (M49; France 250 in the FDI, merchandise and services tables, 251 only in port
-- calls) from poc_unctad_areas: the EU27 members, 3412 European Union (2020 …) = EU27_2020, 0000 World. Other
-- economies have no ISO code in the tables (_X); the build stage builds only the listed areas, and
-- tests/unctad_areas_present.sql fails when a listed code is missing from one of the three built tables.
unctad_areas as ({{ poc_unctad_areas() }}),

unctad as (
    select
        'unctad' as source_system,
        c.economy as source_code,
        coalesce(g.geography_key, '_X') as geography_key
    from (
        select distinct economy from {{ ref('unctad_fdi_flows_stock') }}
        union
        select distinct economy from {{ ref('unctad_merchandise_trade') }}
        union
        select distinct economy from {{ ref('unctad_services_trade') }}
    ) as c
    left join unctad_areas as a on c.economy = a.economy
    left join geo as g on a.candidate_key = g.geography_key
    where c.economy is not null
),

-- WHO data.who.int: areas by M49 code (unpadded) and type; countries through poc_m49_eu27 (the EU27 members),
-- World (GLOBAL 1) = WB:WLD. WHO regions and income groups have no Gold key (_X); the build stage builds only the
-- mapped areas. source_code = <type>:<code>, as region and country codes share one number space.
who_m49 as ({{ poc_m49_eu27() }}),

who as (
    select
        'who' as source_system,
        a.geo_type || ':' || a.geo_code_m49 as source_code,
        coalesce(g.geography_key, case when a.geo_type = 'GLOBAL' and a.geo_code_m49 = '1' then 'WB:WLD' end, '_X')
            as geography_key
    from (
        select distinct
            geo_type,
            geo_code_m49
        from {{ ref('who_observations') }}
    ) as a
    left join who_m49 as m on a.geo_type = 'COUNTRY' and lpad(a.geo_code_m49, 3, '0') = m.m49
    left join geo as g on g.level_name = 'country' and m.iso2 = g.geography_key
),

-- UNESCO UIS: countries by ISO 3166-1 alpha-3 (dim_geography.iso3); regional units ('SDG: ...', 'WB: ...') have no
-- Gold key (_X), and the build stage builds only the EU27 members.
unesco_uis as (
    select
        'unesco_uis' as source_system,
        a.geo_unit as source_code,
        coalesce(g.geography_key, '_X') as geography_key
    from (
        select distinct
            geo_unit,
            is_country_code
        from {{ ref('unesco_uis_observations') }}
    ) as a
    left join geo as g on a.is_country_code and g.level_name = 'country' and a.geo_unit = g.iso3
),

-- FAOSTAT: FAO area codes; countries through their UN M49 code (poc_m49_eu27: the EU27 members), FAO aggregates
-- 5707 European Union (27) = EU27_2020 (current members, fixed composition) and 5000 World. Other areas have no
-- Gold key here (_X); the build stage builds only the mapped ones.
faostat_m49 as ({{ poc_m49_eu27() }}),

faostat as (
    select
        'faostat' as source_system,
        cast(a.area_code as varchar) as source_code,
        coalesce(
            g.geography_key,
            case a.area_code when 5707 then 'EUROSTAT:EU27_2020' when 5000 then 'WB:WLD' end,
            '_X'
        ) as geography_key
    from (
        select distinct
            area_code,
            area_code_m49
        from {{ ref('faostat_observations') }}
    ) as a
    left join faostat_m49 as m on a.area_code_m49 = m.m49 and a.area_code < 5000
    left join geo as g on g.level_name = 'country' and m.iso2 = g.geography_key
),

-- ECB: ISO two-letter countries (GR, GB) and the aggregates of poc_ecb_areas (U2, I8, I9, B6, B0, D0); static
-- codes, so Gold does not read ECB Silver here. Countries come from dim_geography's country keys.
ecb_areas as ({{ poc_ecb_areas() }}),

ecb as (
    select
        'ecb' as source_system,
        area_code as source_code,
        candidate_key as geography_key
    from ecb_areas
    union all
    select
        'ecb' as source_system,
        geography_key as source_code,
        geography_key
    from geo
    where level_name = 'country' and length(geography_key) = 2
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
    select * from mrpips
    union all
    select * from wdi
    union all
    select * from oecd
    union all
    select * from gleif
    union all
    select * from teryt_prg
    union all
    select * from wb_family
    union all
    select * from unctad
    union all
    select * from who
    union all
    select * from unesco_uis
    union all
    select * from faostat
    union all
    select * from ecb
) as m
left join geo as g on m.geography_key = g.geography_key and g.geography_key <> '_X'
where m.source_code is not null
qualify row_number() over (partition by m.source_system, m.source_code order by g.geography_key nulls last) = 1
