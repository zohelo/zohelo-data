{{ config(alias='dim_geography') }}
{# Gold PoC: geography keyed by AREA, not by scheme (design draft 2, scenario 1). One row per
   area; the codes every source uses for it are attributes (iso2, nuts_code, terc_code,
   bdl_unit_id, wb_code), so a GUS series and a Eurostat series of Małopolskie share one key.
   Natural keys: 'PL' (country iso2), 'TERC:<code>' (Polish units), 'NUTS:<code>' (NUTS areas
   without a TERC unit), 'BDL:<id>' (BDL podregiony), 'EUROSTAT:<code>' / 'WB:<code>'
   (aggregates), '_T', '_Z', '_X'. Hierarchy for Poland: Polska → makroregion → województwo →
   region (only Mazowieckie splits) → podregion → powiat → gmina. #}
with nuts_pl as ({{ poc_nuts_pl() }}),

woj_makro as ({{ poc_woj_makroregion() }}),

terc as (select * from {{ ref('teryt_terc') }}),

geo_codes as (
    select
        code,
        any_value(name_en) as name_en
    from {{ ref('eurostat_codes') }}
    where codelist_id = 'GEO'
    group by code
),

wdi as (select * from {{ ref('wdi_countries') }}),

bdl_units as (select * from {{ ref('bdl_units') }}),

-- Countries: Eurostat GEO two-letter codes (EL → GR, UK → GB) and World Bank economies with an ISO-2.
eurostat_countries as (
    select
        case code when 'EL' then 'GR' when 'UK' then 'GB' else code end as iso2,
        name_en
    from geo_codes
    where length(code) = 2 and code not in ('EU', 'EA')
),

wb_countries as (
    select
        iso2_code as iso2,
        short_name as name_en,
        economy_code as iso3,
        region,
        income_group
    from wdi
    where iso2_code is not null and length(iso2_code) = 2 and region is not null
),

countries as (
    select
        coalesce(w.iso2, e.iso2) as geography_key,
        coalesce(w.name_en, e.name_en) as name_en,
        case coalesce(w.iso2, e.iso2) when 'PL' then 'Polska' end as name_pl,
        1 as level,
        'country' as level_name,
        cast(null as varchar) as parent_key,
        coalesce(w.iso2, e.iso2) as iso2,
        w.iso3,
        cast(null as varchar) as nuts_code,
        cast(null as varchar) as terc_code,
        cast(null as varchar) as bdl_unit_id,
        w.iso3 as wb_code,
        false as is_aggregate
    from wb_countries as w
    full outer join eurostat_countries as e on w.iso2 = e.iso2
),

-- Polish administrative units from TERYT; a voivodeship carries its NUTS code (PL9 for Mazowieckie).
wojewodztwa as (
    select
        'TERC:' || t.terc_code as geography_key,
        n.name_pl as name_en,
        t.name_pl,
        3 as level,
        'wojewodztwo' as level_name,
        coalesce('NUTS:' || m.nuts1_code, 'PL') as parent_key,
        cast(null as varchar) as iso2,
        cast(null as varchar) as iso3,
        n.nuts_code,
        t.terc_code,
        cast(null as varchar) as bdl_unit_id,
        cast(null as varchar) as wb_code,
        false as is_aggregate
    from terc as t
    left join nuts_pl as n on t.terc_code = n.woj_code and n.level_name = 'wojewodztwo'
    left join woj_makro as m on t.terc_code = m.woj_code
    where t.unit_level = 'wojewodztwo'
),

powiaty as (
    select
        'TERC:' || terc_code as geography_key,
        name_pl as name_en,
        name_pl,
        6 as level,
        'powiat' as level_name,
        'TERC:' || woj_code as parent_key,
        cast(null as varchar) as iso2,
        cast(null as varchar) as iso3,
        cast(null as varchar) as nuts_code,
        terc_code,
        cast(null as varchar) as bdl_unit_id,
        cast(null as varchar) as wb_code,
        false as is_aggregate
    from terc
    where unit_level = 'powiat'
),

gminy as (
    select
        'TERC:' || terc_code as geography_key,
        name_pl as name_en,
        name_pl,
        7 as level,
        'gmina' as level_name,
        'TERC:' || woj_code || pow_code as parent_key,
        cast(null as varchar) as iso2,
        cast(null as varchar) as iso3,
        cast(null as varchar) as nuts_code,
        terc_code,
        cast(null as varchar) as bdl_unit_id,
        cast(null as varchar) as wb_code,
        false as is_aggregate
    from terc
    where unit_level = 'gmina'
),

makroregiony as (
    select
        'NUTS:' || nuts_code as geography_key,
        name_pl as name_en,
        name_pl,
        2 as level,
        'makroregion' as level_name,
        'PL' as parent_key,
        cast(null as varchar) as iso2,
        cast(null as varchar) as iso3,
        nuts_code,
        cast(null as varchar) as terc_code,
        cast(null as varchar) as bdl_unit_id,
        cast(null as varchar) as wb_code,
        false as is_aggregate
    from nuts_pl
    where level_name = 'makroregion'
),

regiony as (
    select
        'NUTS:' || nuts_code as geography_key,
        name_pl as name_en,
        name_pl,
        4 as level,
        'region' as level_name,
        'TERC:' || woj_code as parent_key,
        cast(null as varchar) as iso2,
        cast(null as varchar) as iso3,
        nuts_code,
        cast(null as varchar) as terc_code,
        cast(null as varchar) as bdl_unit_id,
        cast(null as varchar) as wb_code,
        false as is_aggregate
    from nuts_pl
    where level_name = 'region'
),

-- BDL podregiony (level 4): no TERC code; the parent voivodeship is in the unit id (characters 3–4).
podregiony as (
    select
        'BDL:' || unit_id as geography_key,
        name as name_en,
        name as name_pl,
        5 as level,
        'podregion' as level_name,
        'TERC:' || substr(unit_id, 3, 2) as parent_key,
        cast(null as varchar) as iso2,
        cast(null as varchar) as iso3,
        cast(null as varchar) as nuts_code,
        cast(null as varchar) as terc_code,
        unit_id as bdl_unit_id,
        cast(null as varchar) as wb_code,
        false as is_aggregate
    from bdl_units
    where level = 4
),

-- NUTS regions of other countries, from the Eurostat GEO code list: level by code length.
foreign_nuts as (
    select
        'NUTS:' || code as geography_key,
        name_en,
        cast(null as varchar) as name_pl,
        case length(code) when 3 then 2 when 4 then 3 else 5 end as level,
        case length(code) when 3 then 'nuts1' when 4 then 'nuts2' else 'nuts3' end as level_name,
        case substr(code, 1, 2) when 'EL' then 'GR' when 'UK' then 'GB' else substr(code, 1, 2) end as parent_key,
        cast(null as varchar) as iso2,
        cast(null as varchar) as iso3,
        code as nuts_code,
        cast(null as varchar) as terc_code,
        cast(null as varchar) as bdl_unit_id,
        cast(null as varchar) as wb_code,
        false as is_aggregate
    from geo_codes
    where
        length(code) between 3 and 5 and regexp_matches(code, '^[A-Z]{2}[A-Z0-9]{1,3}$') and substr(code, 1, 2) <> 'PL'
),

aggregates as (
    select
        'EUROSTAT:' || code as geography_key,
        name_en,
        cast(null as varchar) as name_pl,
        0 as level,
        'aggregate' as level_name,
        cast(null as varchar) as parent_key,
        cast(null as varchar) as iso2,
        cast(null as varchar) as iso3,
        cast(null as varchar) as nuts_code,
        cast(null as varchar) as terc_code,
        cast(null as varchar) as bdl_unit_id,
        cast(null as varchar) as wb_code,
        true as is_aggregate
    from geo_codes
    where
        code in ('EU', 'EA')
        or length(code) > 5
        or (length(code) between 3 and 5 and not regexp_matches(code, '^[A-Z]{2}[A-Z0-9]{1,3}$'))
    union all
    select
        'WB:' || economy_code as geography_key,
        short_name as name_en,
        null as name_pl,
        0 as level,
        'aggregate' as level_name,
        null as parent_key,
        null as iso2,
        null as iso3,
        null as nuts_code,
        null as terc_code,
        null as bdl_unit_id,
        economy_code as wb_code,
        true as is_aggregate
    from wdi
    where region is null or iso2_code is null or length(iso2_code) <> 2
),

specials as (
    select * from (
        values
        ('_T', 'All areas', 'Wszystkie obszary', -1, 'special', null, null, null, null, null, null, null, true),
        ('_Z', 'Not applicable', 'Nie dotyczy', -1, 'special', null, null, null, null, null, null, null, false),
        (
            '_X',
            'Unknown or unmatched',
            'Nieznany / niedopasowany',
            -1,
            'special',
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            false
        )
    )
        as t (
            geography_key,
            name_en,
            name_pl,
            level,
            level_name,
            parent_key,
            iso2,
            iso3,
            nuts_code,
            terc_code,
            bdl_unit_id,
            wb_code,
            is_aggregate
        )
),

members as (
    select * from countries
    union all
    select * from wojewodztwa
    union all
    select * from powiaty
    union all
    select * from gminy
    union all
    select * from makroregiony
    union all
    select * from regiony
    union all
    select * from podregiony
    union all
    select * from foreign_nuts
    union all
    select * from aggregates
    union all
    select * from specials
)

select
    m.geography_key,
    m.name_en,
    m.name_pl,
    coalesce(m.name_pl, m.name_en) as name,
    m.level,
    m.level_name,
    m.parent_key,
    m.iso2,
    m.iso3,
    m.nuts_code,
    m.terc_code,
    m.bdl_unit_id,
    m.wb_code,
    m.is_aggregate,
    -- flattened Polish hierarchy for drill-down: country → województwo → powiat
    case
        when m.level >= 1 and m.terc_code is not null then 'PL'
        when m.level_name in ('makroregion', 'region', 'podregion') then 'PL'
        when m.level_name like 'nuts%' then m.parent_key
        else m.geography_key
    end as country_key,
    case
        when m.level_name = 'wojewodztwo' then m.geography_key
        when m.level_name in ('powiat', 'gmina') then 'TERC:' || substr(m.terc_code, 1, 2)
        when m.level_name in ('region', 'podregion') then m.parent_key
    end as wojewodztwo_key,
    case
        when m.level_name = 'powiat' then m.geography_key
        when m.level_name = 'gmina' then 'TERC:' || substr(m.terc_code, 1, 4)
    end as powiat_key
from members as m
qualify row_number() over (partition by m.geography_key order by m.name_en) = 1
