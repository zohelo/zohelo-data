{# Conformed geography (warehouse/dimensions/geography.yaml), v1: the Polish TERYT schemes.
   Scheme TERC: Polska (L0) → województwo (L1) → powiat (L2) → gmina (L3, RODZ 1–3) →
   część gminy (L4: town and rural area of an urban-rural gmina, RODZ 4/5; districts of
   Warszawa and delegations of Kraków, Łódź, Poznań, Wrocław, RODZ 8/9);
   scheme SIMC: localities (L5) under their gmina or gmina part. Vintage = TERYT catalogue
   state date. Area (hectares) of voivodeships, powiats and gminas comes from PRG
   (silver.prg_units).
   Scheme WB: World Bank economies (level 3) and aggregates (is_aggregate) from WDI, with
   ISO 2-alpha code, region, income group and lending category; vintage = WDI release.
   Schemes are stacked with UNION ALL BY NAME, so each brings only its own attributes.
   Further schemes (M49, NUTS/LAU, KTS/BDL, Eurostat GEO) and the cross-scheme bridge follow. #}
with terc as (select * from {{ ref('teryt_terc') }}),

prg as (
    -- a unit drawn as several features is summed, so the join never duplicates members
    select
        case when unit_level = 'kraj' then 'PL' else teryt_code end as prg_code,
        sum(area_ha) as area_ha
    from {{ ref('prg_units') }}
    where unit_level in ('kraj', 'wojewodztwo', 'powiat', 'gmina')
    group by 1
),

simc as (select * from {{ ref('teryt_simc') }}),

wdi as (select * from {{ ref('wdi_countries') }}),

vintage as (select max(catalogue_date) as d from terc),

members as (
    select
        'TERC' as scheme,
        'PL' as code,
        0 as level,
        'kraj' as level_name,
        'Polska' as name_pl,
        null as parent_code,
        null as l1_code,
        null as l2_code,
        null as l3_code,
        null as l4_code,
        null as teryt_type,
        (select d from vintage) as vintage
    union all
    select
        'TERC',
        terc_code,
        case
            when unit_level = 'wojewodztwo' then 1 when unit_level = 'powiat' then 2
            when gmina_type_code in ('4', '5', '8', '9') then 4 else 3
        end,
        case
            when unit_level <> 'gmina' then unit_level
            when gmina_type_code in ('4', '5', '8', '9') then 'czesc_gminy' else 'gmina'
        end,
        name_pl,
        case
            when unit_level = 'wojewodztwo' then 'PL'
            when unit_level = 'powiat' then woj_code
            when gmina_type_code in ('4', '5') then woj_code || pow_code || gmi_code || '3'
            when gmina_type_code in ('8', '9') then woj_code || pow_code || '011'
            else woj_code || pow_code
        end,
        woj_code,
        case when unit_level in ('powiat', 'gmina') then woj_code || pow_code end,
        case
            when unit_level <> 'gmina' then null
            when gmina_type_code in ('4', '5') then woj_code || pow_code || gmi_code || '3'
            when gmina_type_code in ('8', '9') then woj_code || pow_code || '011'
            else terc_code
        end,
        case when gmina_type_code in ('4', '5', '8', '9') then terc_code end,
        coalesce(cast(gmina_type_code as varchar), unit_type_pl),
        catalogue_date
    from terc
    union all
    select
        'SIMC',
        s.simc_code,
        5,
        'miejscowosc',
        s.name_pl,
        s.gmina_terc_code,
        substr(s.gmina_terc_code, 1, 2),
        substr(s.gmina_terc_code, 1, 4),
        case
            when substr(s.gmina_terc_code, 7, 1) in ('4', '5') then substr(s.gmina_terc_code, 1, 6) || '3'
            when substr(s.gmina_terc_code, 7, 1) in ('8', '9') then substr(s.gmina_terc_code, 1, 4) || '011'
            else s.gmina_terc_code
        end,
        case when substr(s.gmina_terc_code, 7, 1) in ('4', '5', '8', '9') then s.gmina_terc_code end,
        s.locality_type_code,
        s.catalogue_date
    from simc as s
),

teryt_members as (
    select
        members.scheme || ':' || members.code || ':' || strftime(members.vintage, '%Y%m%d') as geography_key,
        members.scheme,
        members.code,
        members.vintage,
        members.vintage as valid_from,
        null::date as valid_to,
        level,
        level_name,
        name_pl,
        parent_code,
        'PL' as l0_code,
        l1_code,
        l2_code,
        l3_code,
        l4_code,
        teryt_type,
        prg.area_ha,
        round(prg.area_ha / 100, 2) as area_km2,
        false as is_aggregate,
        'member' as member_type
    from members
    left join prg on members.scheme = 'TERC' and prg.prg_code = members.code
),

wb_members as (
    select
        'WB:' || economy_code || ':' || source_version[1:8] as geography_key,
        'WB' as scheme,
        economy_code as code,
        cast(strptime(source_version[1:8], '%Y%m%d') as date) as vintage,
        cast(strptime(source_version[1:8], '%Y%m%d') as date) as valid_from,
        null::date as valid_to,
        case when is_aggregate then null else 3 end as level,
        case when is_aggregate then 'aggregate' else 'economy' end as level_name,
        short_name as name_en,
        long_name as long_name_en,
        iso2_code,
        region as wb_region,
        income_group as wb_income_group,
        lending_category as wb_lending_category,
        is_aggregate,
        'member' as member_type
    from wdi
),

special as (
    select * from (
        values
        ('_T', '_T', '_T', 'wszystkie obszary (all areas)', 'all areas', true, 'all_areas'),
        ('_Z', '_Z', '_Z', 'nie dotyczy (not applicable)', 'not applicable', false, 'not_applicable'),
        ('_X', '_X', '_X', 'nieznany (unknown)', 'unknown', false, 'unknown')
    ) as t (geography_key, scheme, code, name_pl, name_en, is_aggregate, member_type)
)

select * from teryt_members
union all by name
select * from wb_members
union all by name
select * from special
