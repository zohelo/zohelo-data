{# Conformed geography (warehouse/dimensions/geography.yaml), v1: the Polish TERYT schemes.
   Scheme TERC: Polska (L0) → województwo (L1) → powiat (L2) → gmina (L3);
   scheme SIMC: localities (L4) under their gmina. Vintage = TERYT catalogue state date.
   Area (hectares) of voivodeships, powiats and gminas comes from PRG (silver.prg_units).
   Further schemes (ISO/M49, NUTS/LAU, KTS/BDL, World Bank, Eurostat GEO) and the
   cross-scheme bridge follow as their sources are modelled. #}
with terc as (select * from {{ ref('teryt_terc') }}),
prg as (
    -- a unit drawn as several features is summed, so the join never duplicates members
    select case when unit_level = 'kraj' then 'PL' else teryt_code end as prg_code, sum(area_ha) as area_ha
    from {{ ref('prg_units') }}
    where unit_level in ('kraj', 'wojewodztwo', 'powiat', 'gmina')
    group by 1
),
simc as (select * from {{ ref('teryt_simc') }}),
vintage as (select max(catalogue_date) as d from terc),
members as (
    select 'TERC' as scheme, 'PL' as code, 0 as level, 'kraj' as level_name, 'Polska' as name_pl,
           null as parent_code, null as l1_code, null as l2_code, null as l3_code, null as teryt_type,
           (select d from vintage) as vintage
    union all
    select 'TERC', terc_code,
           case unit_level when 'wojewodztwo' then 1 when 'powiat' then 2 else 3 end,
           unit_level, name_pl,
           case unit_level when 'wojewodztwo' then 'PL' when 'powiat' then woj_code else woj_code || pow_code end,
           woj_code,
           case when unit_level in ('powiat', 'gmina') then woj_code || pow_code end,
           case when unit_level = 'gmina' then terc_code end,
           coalesce(gmina_type_code, unit_type_pl), catalogue_date
    from terc
    union all
    select 'SIMC', s.simc_code, 4, 'miejscowosc', s.name_pl, s.gmina_terc_code,
           substr(s.gmina_terc_code, 1, 2), substr(s.gmina_terc_code, 1, 4), s.gmina_terc_code,
           s.locality_type_code, s.catalogue_date
    from simc s
)
select
    members.scheme || ':' || members.code || ':' || strftime(members.vintage, '%Y%m%d') as geography_key,
    members.scheme, members.code, members.vintage, vintage as valid_from, null::date as valid_to,
    level, level_name, name_pl, parent_code,
    'PL' as l0_code, l1_code, l2_code, l3_code,
    teryt_type, prg.area_ha, round(prg.area_ha / 100, 2) as area_km2,
    false as is_aggregate, 'member' as member_type
from members
left join prg on members.scheme = 'TERC' and prg.prg_code = members.code
union all select '_T', '_T', '_T', null, null, null, null, null, 'wszystkie obszary (all areas)', null, null, null, null, null, null, null, null, true, 'all_areas'
union all select '_Z', '_Z', '_Z', null, null, null, null, null, 'nie dotyczy (not applicable)', null, null, null, null, null, null, null, null, false, 'not_applicable'
union all select '_X', '_X', '_X', null, null, null, null, null, 'nieznany (unknown)', null, null, null, null, null, null, null, null, false, 'unknown'
