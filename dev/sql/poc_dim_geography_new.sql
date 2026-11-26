

with nuts_pl as (
    select * from (
        values
        ('PL2', 'Makroregion południowy', null, 'makroregion'),
        ('PL4', 'Makroregion północno-zachodni', null, 'makroregion'),
        ('PL5', 'Makroregion południowo-zachodni', null, 'makroregion'),
        ('PL6', 'Makroregion północny', null, 'makroregion'),
        ('PL7', 'Makroregion centralny', null, 'makroregion'),
        ('PL8', 'Makroregion wschodni', null, 'makroregion'),
        ('PL9', 'Makroregion województwo mazowieckie', '14', 'wojewodztwo'),
        ('PL21', 'Małopolskie', '12', 'wojewodztwo'),
        ('PL22', 'Śląskie', '24', 'wojewodztwo'),
        ('PL41', 'Wielkopolskie', '30', 'wojewodztwo'),
        ('PL42', 'Zachodniopomorskie', '32', 'wojewodztwo'),
        ('PL43', 'Lubuskie', '08', 'wojewodztwo'),
        ('PL51', 'Dolnośląskie', '02', 'wojewodztwo'),
        ('PL52', 'Opolskie', '16', 'wojewodztwo'),
        ('PL61', 'Kujawsko-pomorskie', '04', 'wojewodztwo'),
        ('PL62', 'Warmińsko-mazurskie', '28', 'wojewodztwo'),
        ('PL63', 'Pomorskie', '22', 'wojewodztwo'),
        ('PL71', 'Łódzkie', '10', 'wojewodztwo'),
        ('PL72', 'Świętokrzyskie', '26', 'wojewodztwo'),
        ('PL81', 'Lubelskie', '06', 'wojewodztwo'),
        ('PL82', 'Podkarpackie', '18', 'wojewodztwo'),
        ('PL84', 'Podlaskie', '20', 'wojewodztwo'),
        ('PL91', 'Warszawski stołeczny', '14', 'region'),
        ('PL92', 'Mazowiecki regionalny', '14', 'region')
    ) as t (nuts_code, name_pl, woj_code, level_name)
),

oecd_areas as (
    select * from (
        values
        ('EU27_2020', 'European Union (27 countries from 2020)', 'EU27_2020'),
        ('EA20', 'Euro area (20 countries)', 'EA20'),
        ('EA19', 'Euro area (19 countries)', 'EA19'),
        ('EU', 'European Union (OECD composite)', ''),
        ('OECDXEA', 'OECD excluding the euro area', ''),
        ('OECD', 'OECD total', ''),
        ('OECDE', 'OECD Europe', ''),
        ('G7', 'G7', ''),
        ('G20', 'G20', ''),
        ('A5M', 'Major five Asia economies', ''),
        ('G4E', 'Major four European economies', ''),
        ('NAFTA', 'NAFTA', '')
    ) as t (ref_area, name_en, eurostat_code)
),

woj_makro as (
    select * from (
        values
        ('12', 'PL2'), ('24', 'PL2'), ('30', 'PL4'), ('32', 'PL4'), ('08', 'PL4'), ('02', 'PL5'), ('16', 'PL5'),
        ('04', 'PL6'), ('28', 'PL6'), ('22', 'PL6'), ('10', 'PL7'), ('26', 'PL7'), ('06', 'PL8'), ('18', 'PL8'),
        ('20', 'PL8')
    ) as t (woj_code, nuts1_code)
),

terc as (select * from "r2"."silver"."teryt_terc"),

geo_codes as (
    select
        code,
        any_value(name_en) as name_en
    from "r2"."silver"."eurostat_codes"
    where codelist_id = 'GEO'
    group by code
),

-- NUTS regions (#2775): a GEO code of 3 to 5 characters is a NUTS region only when its first two characters are a
-- country with NUTS or statistical regions (EU members, EL and UK, EFTA, and the candidate and neighbouring countries
-- Eurostat gives regions: AL, BA, ME, MK, RS, TR, UA, XK). Every other GEO code of that shape is an aggregate: the
-- euro areas (EA11 … EA21), the EU of a past composition (EU15, EU28), EFTA, NMS10, world regions (AFR, ASI),
-- institutions (ECB, EIB) and the European Economic Area, whose codes begin with Estonia's EE (EEA, EEA28, EEA31),
-- so they are named here.
geo_shapes as (
    select
        code,
        name_en,
        length(code) between 3 and 5
        and regexp_matches(code, '^[A-Z]{2}[A-Z0-9]{1,3}$')
        and substr(code, 1, 2) in (
            'AT', 'BE', 'BG', 'CY', 'CZ', 'DE', 'DK', 'EE', 'EL', 'ES', 'FI', 'FR', 'HR', 'HU', 'IE', 'IT', 'LT',
            'LU', 'LV', 'MT', 'NL', 'PL', 'PT', 'RO', 'SE', 'SI', 'SK', 'UK', 'CH', 'IS', 'LI', 'NO', 'AL', 'BA',
            'ME', 'MK', 'RS', 'TR', 'UA', 'XK'
        )
        and code not like 'EEA%' as is_nuts
    from geo_codes
),

wdi as (select * from "r2"."silver"."wdi_countries"),

bdl_units as (select * from "r2"."silver"."bdl_units"),

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

-- OECD aggregates (#2738): only the codes listed in poc_oecd_areas that are not a Eurostat aggregate; any other
-- OECD code without an ISO 3166 alpha-3 country stays unmatched (_X) and build_check reports it.
oecd_aggregates as (
    select
        ref_area,
        name_en
    from oecd_areas
    where eurostat_code = ''
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

-- NUTS regions of other countries, from the Eurostat GEO code list (geo_shapes): level by code length.
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
    from geo_shapes
    where is_nuts and substr(code, 1, 2) <> 'PL'
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
    from geo_shapes
    where code in ('EU', 'EA') or (length(code) > 2 and not is_nuts)
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
    union all
    select
        'OECD:' || ref_area as geography_key,
        name_en,
        null as name_pl,
        0 as level,
        'aggregate' as level_name,
        null as parent_key,
        null as iso2,
        null as iso3,
        null as nuts_code,
        null as terc_code,
        null as bdl_unit_id,
        null as wb_code,
        true as is_aggregate
    from oecd_aggregates
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
    end as powiat_key,
    -- TERYT attributes of Polish units (#2739): unit type as TERYT names it (powiat, miasto na prawach
    -- powiatu, gmina miejska/wiejska/miejsko-wiejska, miasto, obszar wiejski, dzielnica, delegatura), the
    -- gmina type digit (RODZ), ISO 3166-2 for voivodeships (PL-<TERC>) and the TERYT state date.
    t.unit_type_pl as teryt_unit_type,
    t.gmina_type_code,
    case when m.level_name = 'wojewodztwo' then 'PL-' || m.terc_code end as iso_3166_2,
    t.state_date as teryt_state_date
from members as m
left join terc as t on m.terc_code = t.terc_code
qualify row_number() over (partition by m.geography_key order by m.name_en) = 1