


with geo as (select * from "r2"."gold"."dim_geography"),

bis_areas as (
    select * from (
        values
        ('XM', 'EUROSTAT:EA'), ('4T', ''), ('5R', ''), ('5A', ''), ('G2', ''), ('XW', '')
    ) as t (area_code, candidate_key)
),

bis as (
    select
        'bis' as source_system,
        area_code as source_code,
        candidate_key as geography_key
    from bis_areas
    where candidate_key <> ''
    union all
    select
        'bis' as source_system,
        geography_key as source_code,
        geography_key
    from geo
    where level_name = 'country' and length(geography_key) = 2
),

cepik as (
    select
        'cepik' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'cepik' as source_system,
        'polska' as source_code,
        'PL' as geography_key
),

cie_sio as (
    select
        'cie_sio' as source_system,
        replace(g.geography_key, 'TERC:', '') as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'cie_sio' as source_system,
        'PL' as source_code,
        'PL' as geography_key
),

crossref as (
    select
        'crossref' as source_system,
        'PL' as source_code,
        'PL' as geography_key
),

-- ENTSOG: the operator key is the area code (one operator per country today; GAZ-SYSTEM = Poland, no regional grain).
entsog as (
    select
        'entsog' as source_system,
        o.operator_key as source_code,
        g.geography_key
    from (values ('PL-TSO-0002', 'PL')) as o (operator_key, iso2)
    inner join geo as g on g.iso2 = o.iso2 and g.level_name = 'country'
),

eurostat_nuts_pl as (
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

eurostat_codes as (
    select code from "r2"."silver"."eurostat_codes"
    where codelist_id = 'GEO'
    group by code
),

eurostat_names as (  -- a code can carry one name per code list version
    select
        code,
        regexp_replace(upper(trim(name_en)), '^(M\. ST\. |M\. |MIASTO )', '') as name_u
    from "r2"."silver"."eurostat_codes"
    where codelist_id = 'GEO' and regexp_matches(code, '^PL[0-9A-Z]{3}$') and name_en is not null
    group by all
),

eurostat_podregion as (
    select
        c.code,
        min(g.geography_key) as geography_key
    from eurostat_names as c
    inner join eurostat_nuts_pl as n on substr(c.code, 1, 4) = n.nuts_code and n.woj_code is not null
    inner join geo as g
        on
            g.level_name = 'podregion'
            and g.parent_key = 'TERC:' || n.woj_code
            and regexp_replace(regexp_replace(upper(g.name_pl), '^PODREGION ', ''), '^(M\. ST\. |M\. |MIASTO )', '')
            = c.name_u
    group by c.code
    having count(distinct g.geography_key) = 1
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
            p.geography_key,
            '_X'
        ) as geography_key
    from eurostat_codes as c
    left join eurostat_nuts_pl as n on c.code = n.nuts_code
    left join geo as g_country on c.code = g_country.geography_key and g_country.level_name = 'country'
    left join geo as g_nuts on g_nuts.geography_key = 'NUTS:' || c.code
    left join geo as g_agg on g_agg.geography_key = 'EUROSTAT:' || c.code
    left join eurostat_podregion as p on c.code = p.code
),

gddkia_gpr as (
    select
        'gddkia_gpr' as source_system,
        g.terc_code as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'gddkia_gpr' as source_system,
        'PL' as source_code,
        'PL' as geography_key
),

gios_air_quality as (
    select
        'gios_air_quality' as source_system,
        v.voivodeship as source_code,
        g.geography_key
    from (
    select * from (
        values
        ('DOLNOŚLĄSKIE', '02'), ('KUJAWSKO-POMORSKIE', '04'), ('LUBELSKIE', '06'), ('LUBUSKIE', '08'),
        ('ŁÓDZKIE', '10'), ('MAŁOPOLSKIE', '12'), ('MAZOWIECKIE', '14'), ('OPOLSKIE', '16'), ('PODKARPACKIE', '18'),
        ('PODLASKIE', '20'), ('POMORSKIE', '22'), ('ŚLĄSKIE', '24'), ('ŚWIĘTOKRZYSKIE', '26'),
        ('WARMIŃSKO-MAZURSKIE', '28'), ('WIELKOPOLSKIE', '30'), ('ZACHODNIOPOMORSKIE', '32')
    ) as t (voivodeship, terc_code)
) as v
    inner join geo as g on v.terc_code = g.terc_code and g.level_name = 'wojewodztwo'
    union all
    select
        'gios_air_quality' as source_system,
        'POLSKA' as source_code,
        'PL' as geography_key
),

gtfs_gdansk as (
    select
        'gtfs_gdansk' as source_system,
        'Gdańsk' as source_code,
        g.geography_key
    from geo as g
    where g.geography_key = 'TERC:2261011'
),

gtfs_krakow as (
    select
        'gtfs_krakow' as source_system,
        'Kraków' as source_code,
        g.geography_key
    from geo as g
    where g.geography_key = 'TERC:1261011'
),

gtfs_poznan as (
    select
        'gtfs_poznan' as source_system,
        'Poznań' as source_code,
        g.geography_key
    from geo as g
    where g.geography_key = 'TERC:3064011'
),

gtfs_wroclaw as (
    select
        'gtfs_wroclaw' as source_system,
        'Wrocław' as source_code,
        g.geography_key
    from geo as g
    where g.geography_key = 'TERC:0264011'
),

gugik_prng as (
    select
        'gugik_prng' as source_system,
        g.terc_code as source_code,
        g.geography_key
    from geo as g
    where g.level_name in ('wojewodztwo', 'powiat', 'gmina') and g.terc_code is not null
    union all
    select
        'gugik_prng' as source_system,
        'PL' as source_code,
        'PL' as geography_key
),

gunb_rwdz as (
    select
        'gunb_rwdz' as source_system,
        g.terc_code as source_code,
        g.geography_key
    from geo as g
    where g.level_name in ('wojewodztwo', 'powiat')
    union all
    select
        'gunb_rwdz' as source_system,
        'PL' as source_code,
        'PL' as geography_key
),

gus_culture as (
    select
        'gus_culture' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'gus_culture' as source_system,
        'polska' as source_code,
        'PL' as geography_key
),

gus_culture_participation as (
    select
        'gus_culture_participation' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'gus_culture_participation' as source_system,
        'polska' as source_code,
        'PL' as geography_key
),

gus_nsp2021 as (
    select
        'gus_nsp2021' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'gus_nsp2021' as source_system,
        'polska' as source_code,
        'PL' as geography_key
),

gus_psr2020 as (
    select
        'gus_psr2020' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'gus_psr2020' as source_system,
        'polska' as source_code,
        'PL' as geography_key
),

gus_tourism as (
    select
        'gus_tourism' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'gus_tourism' as source_system,
        'polska' as source_code,
        'PL' as geography_key
),

ilostat as (
    select
        'ilostat' as source_system,
        g.iso3 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 is not null
),

imgw_archive as (
    select
        'imgw_archive' as source_system,
        g.terc_code as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'imgw_archive' as source_system,
        'PL' as source_code,
        'PL' as geography_key
),

internet_gov_broadband as (
    select
        'internet_gov_broadband' as source_system,
        v.area_code as source_code,
        'TERC:' || v.area_code as geography_key
    from (
    select * from (
        values
        ('dolnośląskie', '02'),
        ('kujawsko-pomorskie', '04'),
        ('lubelskie', '06'),
        ('lubuskie', '08'),
        ('łódzkie', '10'),
        ('małopolskie', '12'),
        ('mazowieckie', '14'),
        ('opolskie', '16'),
        ('podkarpackie', '18'),
        ('podlaskie', '20'),
        ('pomorskie', '22'),
        ('śląskie', '24'),
        ('świętokrzyskie', '26'),
        ('warmińsko-mazurskie', '28'),
        ('wielkopolskie', '30'),
        ('zachodniopomorskie', '32')
    ) as t (area_name, area_code)
) as v
    union all
    select
        'internet_gov_broadband' as source_system,
        'PL' as source_code,
        'PL' as geography_key
),

mrirw_zsrir as (
    select
        'mrirw_zsrir' as source_system,
        'PL' as source_code,
        'PL' as geography_key
),

mrpips_social_assistance as (
    select
        'mrpips_social_assistance' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'mrpips_social_assistance' as source_system,
        'polska' as source_code,
        'PL' as geography_key
),

ms_isws as (
    select
        'ms_isws' as source_system,
        'PL' as source_code,
        'PL' as geography_key
),

ms_krs as (
    select
        'ms_krs' as source_system,
        upper(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
),

mz_health_bulletin as (
    select
        'mz_health_bulletin' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'mz_health_bulletin' as source_system,
        'polska' as source_code,
        'PL' as geography_key
),

nfz_contracts as (
    select
        'nfz_contracts' as source_system,
        b.branch_code as source_code,
        g.geography_key
    from (
        values
        ('01', '02'), ('02', '04'), ('03', '06'), ('04', '08'),
        ('05', '10'), ('06', '12'), ('07', '14'), ('08', '16'),
        ('09', '18'), ('10', '20'), ('11', '22'), ('12', '24'),
        ('13', '26'), ('14', '28'), ('15', '30'), ('16', '32')
    ) as b (branch_code, woj_code)
    inner join geo as g on g.level_name = 'wojewodztwo' and g.geography_key = 'TERC:' || b.woj_code
    union all
    select
        'nfz_contracts' as source_system,
        'all_branches' as source_code,
        'PL' as geography_key
),

-- NFZ statistics API, services: 'PL' = the Poland answer (branch=false); the 16 NFZ regional branches (oddziały
-- wojewódzkie) by the API's branch code, numbered alphabetically 01 dolnośląski … 16 zachodniopomorski, each the
-- voivodeship of the same name: TERYT code = 2 × branch code. A static list, so the map never waits for the source.
nfz_services as (
    select
        'nfz_services' as source_system,
        b.source_code,
        b.geography_key
    from (
        values
        ('PL', 'PL'),
        ('01', 'TERC:02'), ('02', 'TERC:04'), ('03', 'TERC:06'), ('04', 'TERC:08'),
        ('05', 'TERC:10'), ('06', 'TERC:12'), ('07', 'TERC:14'), ('08', 'TERC:16'),
        ('09', 'TERC:18'), ('10', 'TERC:20'), ('11', 'TERC:22'), ('12', 'TERC:24'),
        ('13', 'TERC:26'), ('14', 'TERC:28'), ('15', 'TERC:30'), ('16', 'TERC:32')
    ) as b (source_code, geography_key)
),

nfz_waiting_times as (
    select
        'nfz_waiting_times' as source_system,
        lpad(cast(cast(left(g.terc_code, 2) as integer) // 2 as varchar), 2, '0') as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'nfz_waiting_times' as source_system,
        '00' as source_code,
        'PL' as geography_key
),

nid_monuments as (
    select
        'nid_monuments' as source_system,
        substr(g.terc_code, 1, 2) || '|' || trim(regexp_replace(lower(g.name_pl), '[-\s]+', ' ', 'g')) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'powiat'
    union all
    select
        'nid_monuments' as source_system,
        'woj:' || g.terc_code as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'nid_monuments' as source_system,
        'PL' as source_code,
        'PL' as geography_key
),

owid as (
    select
        'owid' as source_system,
        g.iso3 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 is not null
    union all
    select
        'owid' as source_system,
        'OWID_WRL' as source_code,
        'WB:WLD' as geography_key
    union all
    select
        'owid' as source_system,
        'OWID_KOS' as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 = 'XKX'
),

policja_statistics as (
    select
        'policja_statistics' as source_system,
        'woj. ' || lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'policja_statistics' as source_system,
        'Polska' as source_code,
        'PL' as geography_key
),

radon as (
    select
        'radon' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
),

rpwdl as (
    select
        'rpwdl' as source_system,
        left(g.terc_code, 2) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'rpwdl' as source_system,
        'PL' as source_code,
        'PL' as geography_key
),

ted_notices_nuts_pl as (
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

ted_notices as (
    select
        'ted_notices' as source_system,
        n.nuts_code as source_code,
        'TERC:' || n.woj_code as geography_key
    from ted_notices_nuts_pl as n
    where n.level_name = 'wojewodztwo'
    union all
    select
        'ted_notices' as source_system,
        'POL' as source_code,
        'PL' as geography_key
),

ulc_airports as (
    select
        'ulc_airports' as source_system,
        a.airport_code as source_code,
        case when a.airport_code = '_T' then 'PL' else coalesce(g.geography_key, '_X') end as geography_key
    from (
    select * from (values
        ('EPWA', 'WAW', 'Warszawa-Chopin', '14', 'chopin'),
        ('EPKK', 'KRK', 'Kraków-Balice', '12', 'krak[oó]w'),
        ('EPGD', 'GDN', 'Gdańsk im. Lecha Wałęsy', '22', 'gdańsk'),
        ('EPKT', 'KTW', 'Katowice-Pyrzowice', '24', 'katowice'),
        ('EPWR', 'WRO', 'Wrocław-Strachowice', '02', 'wrocław'),
        ('EPPO', 'POZ', 'Poznań-Ławica', '30', 'poznań'),
        ('EPMO', 'WMI', 'Warszawa/Modlin', '14', 'modlin'),
        ('EPRZ', 'RZE', 'Rzeszów-Jasionka', '18', 'rzeszów'),
        ('EPLL', 'LCJ', 'Łódź-Lublinek', '10', 'łódź'),
        ('EPBY', 'BZG', 'Bydgoszcz-Szwederowo', '04', 'bydgoszcz'),
        ('EPSC', 'SZZ', 'Szczecin-Goleniów', '32', 'szczecin'),
        ('EPLB', 'LUZ', 'Lublin', '06', '(^|[^a-z])lublin($|[^a-z])'),
        ('EPSY', 'SZY', 'Olsztyn-Mazury', '28', 'olsztyn'),
        ('EPZG', 'IEG', 'Zielona Góra-Babimost', '08', 'zielona góra'),
        ('EPRA', 'RDO', 'Warszawa-Radom', '14', 'radom'),
        ('_T', null, 'Suma (wszystkie porty w tabeli)', null, '^suma$')
    ) as t (airport_code, iata_code, airport_name, terc_woj_code, name_pattern)
) as a
    left join geo as g on g.level_name = 'wojewodztwo' and g.terc_code = a.terc_woj_code
),

undata_m49 as (
    select * from (
        values
        ('040', 'AT'), ('056', 'BE'), ('100', 'BG'), ('196', 'CY'), ('203', 'CZ'), ('276', 'DE'), ('208', 'DK'),
        ('233', 'EE'), ('724', 'ES'), ('246', 'FI'), ('250', 'FR'), ('300', 'GR'), ('191', 'HR'), ('348', 'HU'),
        ('372', 'IE'), ('380', 'IT'), ('440', 'LT'), ('442', 'LU'), ('428', 'LV'), ('470', 'MT'), ('528', 'NL'),
        ('616', 'PL'), ('620', 'PT'), ('642', 'RO'), ('752', 'SE'), ('705', 'SI'), ('703', 'SK')
    ) as t (m49, iso2)
),

undata as (
    select
        'undata' as source_system,
        cast(cast(m49 as integer) as varchar) as source_code,
        iso2 as geography_key
    from undata_m49
    union all
    select
        'undata' as source_system,
        '1' as source_code,
        'WB:WLD' as geography_key
),

undp_hdr as (
    select
        'undp_hdr' as source_system,
        g.iso3 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 is not null
    union all
    select
        'undp_hdr' as source_system,
        'ZZK.WORLD' as source_code,
        'WB:WLD' as geography_key
),

unece_areas as (
    select * from (
        values
        ('008', 'AL'), ('020', 'AD'), ('031', 'AZ'), ('040', 'AT'), ('051', 'AM'), ('056', 'BE'), ('070', 'BA'),
        ('100', 'BG'), ('112', 'BY'), ('124', 'CA'), ('191', 'HR'), ('196', 'CY'), ('203', 'CZ'), ('208', 'DK'),
        ('233', 'EE'), ('246', 'FI'), ('250', 'FR'), ('268', 'GE'), ('276', 'DE'), ('300', 'GR'), ('348', 'HU'),
        ('352', 'IS'), ('372', 'IE'), ('376', 'IL'), ('380', 'IT'), ('398', 'KZ'), ('417', 'KG'), ('428', 'LV'),
        ('438', 'LI'), ('440', 'LT'), ('442', 'LU'), ('470', 'MT'), ('492', 'MC'), ('498', 'MD'), ('499', 'ME'),
        ('528', 'NL'), ('578', 'NO'), ('616', 'PL'), ('620', 'PT'), ('642', 'RO'), ('643', 'RU'), ('674', 'SM'),
        ('688', 'RS'), ('703', 'SK'), ('705', 'SI'), ('724', 'ES'), ('752', 'SE'), ('756', 'CH'), ('762', 'TJ'),
        ('792', 'TR'), ('795', 'TM'), ('804', 'UA'), ('807', 'MK'), ('826', 'GB'), ('840', 'US'), ('860', 'UZ'),
        ('E27', 'EUROSTAT:EU27_2020'), ('EAA', 'EUROSTAT:EA'),
        ('CIS', ''), ('CWG', ''), ('EAEU', ''), ('ECE', ''), ('NA', ''), ('ST7', '')
    ) as t (area_code, iso2)
),

unece as (
    select
        'unece' as source_system,
        a.area_code as source_code,
        g.geography_key
    from unece_areas as a
    inner join geo as g on a.iso2 = g.iso2 and g.level_name = 'country'
    where length(a.iso2) = 2
    union all
    select
        'unece' as source_system,
        area_code as source_code,
        iso2 as geography_key
    from unece_areas
    where length(iso2) > 2
),

-- World Bank EdStats (OECD PISA republication): countries by ISO 3166-1 alpha-3 (dim_geography iso3), built from
-- Gold's own list like oecd, so the dimensions never wait for the source; World Bank aggregates (OED, ...) are not
-- in this list, so the build stage's inner join leaves them out.
world_bank_edstats as (
    select
        'world_bank_edstats' as source_system,
        g.iso3 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 is not null
),

-- World Bank GEM (API source 15): countries by ISO 3166-1 alpha-3 (dim_geography iso3), built from Gold's own list
-- like oecd, so the dimensions never wait for the GEM source.
world_bank_gem as (
    select
        'world_bank_gem' as source_system,
        g.iso3 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 is not null
),

zus_yearbook as (
    select
        'zus_yearbook' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'zus_yearbook' as source_system,
        'polska' as source_code,
        'PL' as geography_key
),

-- BDL: the web export keys rows by a 7-digit zero-padded TERYT code (0000000 Polska, WW00000
-- województwo, WWPP000 powiat, WWPPGGT gmina); the API by a 12-character unit id (bdl_units),
-- mapped here by level and name (v1: the GUS KTS–TERYT correspondence table).
terc_woj as (
    select
        terc_code,
        upper(name_pl) as name_u
    from "r2"."silver"."teryt_terc"
    where unit_level = 'wojewodztwo'
),

terc_pow as (
    select
        terc_code,
        woj_code,
        lower(name_pl) as name_l,
        unit_type_pl
    from "r2"."silver"."teryt_terc"
    where unit_level = 'powiat'
),

terc_gmi as (
    select
        terc_code,
        woj_code,
        pow_code,
        lower(name_pl) as name_l,
        gmina_type_code
    from "r2"."silver"."teryt_terc"
    where unit_level = 'gmina'
),

bdl_units as (select * from "r2"."silver"."bdl_units"),

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
    from "r2"."silver"."teryt_terc"
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
    from "r2"."silver"."teryt_terc" as g
    cross join type_digits as t
    where g.unit_level = 'gmina' and g.gmina_type_code in ('1', '2', '3') and t.digit <> g.gmina_type_code
),

-- DBW cross-section positions are territory names; matched by name (upper) to Polish areas.
dbw_names as (  -- position labels of the activated DBW blocks (extracted dictionaries)
    select distinct upper(trim(opis)) as source_code
    from "r2"."gold"."src_dbw_dictionary"
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
    from "r2"."silver"."teryt_terc"
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
    from "r2"."silver"."wdi_countries" as w
    left join geo as g on g.level_name = 'country' and w.iso2_code = g.iso2
    left join geo as g_agg on g_agg.geography_key = 'WB:' || w.economy_code
),

-- World Bank IDS (#2819): the same World Bank economy codes as WDI (ISO3 countries, WB: groups such as LMY,
-- ECA), so the mapping is the WDI one under its own source_system; no IDS table is read here.
ids as (
    select
        'world_bank_ids' as source_system,
        source_code,
        geography_key
    from wdi
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
    from "r2"."silver"."teryt_terc"
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
    from "r2"."silver"."teryt_terc"
    union all
    select distinct
        'gugik_prg' as source_system,
        teryt_code as source_code,
        case when unit_level = 'kraj' then 'PL' else 'TERC:' || teryt_code end as geography_key
    from "r2"."silver"."prg_units"
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
-- ZUS statistical portal (PLS-009): '00' = Poland; voivodeships by their 2-digit TERYT code (the Silver model matched
-- ZUS's voivodeship names to these codes). Built from TERYT, so the dimensions never wait for the ZUS source.
zus_psz as (
    select
        'zus_psz' as source_system,
        terc_code as source_code,
        'TERC:' || terc_code as geography_key
    from "r2"."silver"."teryt_terc"
    where unit_level = 'wojewodztwo'
    union all
    select
        'zus_psz' as source_system,
        '00' as source_code,
        'PL' as geography_key
),

unctad_areas as (
    select * from (
        values
        ('040', 'AT'), ('056', 'BE'), ('100', 'BG'), ('191', 'HR'), ('196', 'CY'), ('203', 'CZ'), ('208', 'DK'),
        ('233', 'EE'), ('246', 'FI'), ('250', 'FR'), ('276', 'DE'), ('300', 'GR'), ('348', 'HU'), ('372', 'IE'),
        ('380', 'IT'), ('428', 'LV'), ('440', 'LT'), ('442', 'LU'), ('470', 'MT'), ('528', 'NL'), ('616', 'PL'),
        ('620', 'PT'), ('642', 'RO'), ('703', 'SK'), ('705', 'SI'), ('724', 'ES'), ('752', 'SE'),
        ('3412', 'EUROSTAT:EU27_2020'), ('0000', 'WB:WLD')
    ) as t (economy, candidate_key)
),

unctad as (
    select
        'unctad' as source_system,
        c.economy as source_code,
        coalesce(g.geography_key, '_X') as geography_key
    from (
        select distinct economy from "r2"."silver"."unctad_fdi_flows_stock"
        union
        select distinct economy from "r2"."silver"."unctad_merchandise_trade"
        union
        select distinct economy from "r2"."silver"."unctad_services_trade"
    ) as c
    left join unctad_areas as a on c.economy = a.economy
    left join geo as g on a.candidate_key = g.geography_key
    where c.economy is not null
),

-- WHO data.who.int: areas by M49 code (unpadded) and type; countries through poc_m49_eu27 (the EU27 members),
-- World (GLOBAL 1) = WB:WLD. WHO regions and income groups have no Gold key (_X); the build stage builds only the
-- mapped areas. source_code = <type>:<code>, as region and country codes share one number space.
who_m49 as (
    select * from (
        values
        ('040', 'AT'), ('056', 'BE'), ('100', 'BG'), ('196', 'CY'), ('203', 'CZ'), ('276', 'DE'), ('208', 'DK'),
        ('233', 'EE'), ('724', 'ES'), ('246', 'FI'), ('250', 'FR'), ('300', 'GR'), ('191', 'HR'), ('348', 'HU'),
        ('372', 'IE'), ('380', 'IT'), ('440', 'LT'), ('442', 'LU'), ('428', 'LV'), ('470', 'MT'), ('528', 'NL'),
        ('616', 'PL'), ('620', 'PT'), ('642', 'RO'), ('752', 'SE'), ('705', 'SI'), ('703', 'SK')
    ) as t (m49, iso2)
),

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
        from "r2"."silver"."who_observations"
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
        from "r2"."silver"."unesco_uis_observations"
    ) as a
    left join geo as g on a.is_country_code and g.level_name = 'country' and a.geo_unit = g.iso3
),

-- FAOSTAT: FAO area codes; countries through their UN M49 code (poc_m49_eu27: the EU27 members), FAO aggregates
-- 5707 European Union (27) = EU27_2020 (current members, fixed composition) and 5000 World. Other areas have no
-- Gold key here (_X); the build stage builds only the mapped ones.
faostat_m49 as (
    select * from (
        values
        ('040', 'AT'), ('056', 'BE'), ('100', 'BG'), ('196', 'CY'), ('203', 'CZ'), ('276', 'DE'), ('208', 'DK'),
        ('233', 'EE'), ('724', 'ES'), ('246', 'FI'), ('250', 'FR'), ('300', 'GR'), ('191', 'HR'), ('348', 'HU'),
        ('372', 'IE'), ('380', 'IT'), ('440', 'LT'), ('442', 'LU'), ('428', 'LV'), ('470', 'MT'), ('528', 'NL'),
        ('616', 'PL'), ('620', 'PT'), ('642', 'RO'), ('752', 'SE'), ('705', 'SI'), ('703', 'SK')
    ) as t (m49, iso2)
),

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
        from "r2"."silver"."faostat_observations"
    ) as a
    left join faostat_m49 as m on a.area_code_m49 = m.m49 and a.area_code < 5000
    left join geo as g on g.level_name = 'country' and m.iso2 = g.geography_key
),

-- ECB: ISO two-letter countries (GR, GB) and the aggregates of poc_ecb_areas (U2, I8, I9, B6, B0, D0); static
-- codes, so Gold does not read ECB Silver here. Countries come from dim_geography's country keys.
ecb_areas as (
    select * from (
        values
        ('U2', 'EUROSTAT:EA'), ('I8', 'NUTS:EA19'), ('I9', 'NUTS:EA20'), ('B6', 'EUROSTAT:EU27_2020'),
        ('B0', 'EUROSTAT:EU'), ('D0', 'EUROSTAT:EU')
    ) as t (area_code, candidate_key)
),

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
),

-- UN WPP: locations by ISO two-letter code (countries and areas, matched to dim_geography's country keys) and the
-- groups of poc_un_wpp_areas by LocID (1551 European Union (EU: 27) = EU27_2020, 900 World); static, so Gold does
-- not read WPP Silver here. Other groups have no Gold area (_X); the build stage builds only poc_un_wpp_blocks areas.
un_wpp_areas as (
    select * from (
        values
        ('1551', 'EUROSTAT:EU27_2020'), ('900', 'WB:WLD')
    ) as t (loc_id, candidate_key)
),

un_wpp as (
    select
        'un_wpp' as source_system,
        loc_id as source_code,
        candidate_key as geography_key
    from un_wpp_areas
    union all
    select
        'un_wpp' as source_system,
        geography_key as source_code,
        geography_key
    from geo
    where level_name = 'country' and length(geography_key) = 2
),

-- UNHCR Refugee Statistics (cf_type=ISO): countries of asylum by ISO 3166-1 alpha-3 (dim_geography iso3), built
-- from Gold's own list like oecd, so the dimensions never wait for the UNHCR source; UNHCR's own codes for
-- unknown or stateless origins are breakdowns, not areas.
unhcr as (
    select
        'unhcr' as source_system,
        g.iso3 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 is not null
),

-- EEA greenhouse gas inventory: countries by ISO 3166-1 alpha-2 as published (Greece GR), built from Gold's own
-- list like oecd, so the dimensions never wait for the EEA source; the EEA aggregate EUA has no Gold key (_X).
eea as (
    select
        'eea' as source_system,
        g.iso2 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso2 is not null
)

select
    m.source_system,
    m.source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    g.geography_key is not null as matched
from (
    select * from bis
    union all
    select * from cepik
    union all
    select * from cie_sio
    union all
    select * from crossref
    union all
    select * from entsog
    union all
    select * from eurostat
    union all
    select * from gddkia_gpr
    union all
    select * from gios_air_quality
    union all
    select * from gtfs_gdansk
    union all
    select * from gtfs_krakow
    union all
    select * from gtfs_poznan
    union all
    select * from gtfs_wroclaw
    union all
    select * from gugik_prng
    union all
    select * from gunb_rwdz
    union all
    select * from gus_culture
    union all
    select * from gus_culture_participation
    union all
    select * from gus_nsp2021
    union all
    select * from gus_psr2020
    union all
    select * from gus_tourism
    union all
    select * from ilostat
    union all
    select * from imgw_archive
    union all
    select * from internet_gov_broadband
    union all
    select * from mrirw_zsrir
    union all
    select * from mrpips_social_assistance
    union all
    select * from ms_isws
    union all
    select * from ms_krs
    union all
    select * from mz_health_bulletin
    union all
    select * from nfz_contracts
    union all
    select * from nfz_services
    union all
    select * from nfz_waiting_times
    union all
    select * from nid_monuments
    union all
    select * from owid
    union all
    select * from policja_statistics
    union all
    select * from radon
    union all
    select * from rpwdl
    union all
    select * from ted_notices
    union all
    select * from ulc_airports
    union all
    select * from undata
    union all
    select * from undp_hdr
    union all
    select * from unece
    union all
    select * from world_bank_edstats
    union all
    select * from world_bank_gem
    union all
    select * from zus_yearbook
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
    union all
    select * from ids
    union all
    select * from un_wpp
    union all
    select * from unhcr
    union all
    select * from eea
    union all
    select * from zus_psz
) as m
left join geo as g on m.geography_key = g.geography_key and g.geography_key <> '_X'
where m.source_code is not null
qualify row_number() over (partition by m.source_system, m.source_code order by g.geography_key nulls last) = 1