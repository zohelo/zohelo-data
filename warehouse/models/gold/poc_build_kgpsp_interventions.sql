{{ config(alias='build_kgpsp_interventions', tags=['gold_block']) }}
{# Gold build stage: KG PSP standard summary tables (register EX-003), annual (date_key = 1 January of the year), Poland
   and the 16 voivodeships, from silver.kgpsp_interventions_counts:
     block 'incidents'   table 1, incidents by type and size (unit incidents; codes P ... OGÓŁEM as published);
     block 'casualties'  table 10, deaths and injured in fires and local hazards by group (unit persons; counts only).
   2016-2022 as published in the voivodeship workbooks (rows 'Polska' and the voivodeships). From 2023 the producer
   publishes gmina rows only: a voivodeship is the sum of its gmina rows (TERYT WWPPGG, WW = the voivodeship) and Poland
   the sum of all, kept only when every gmina row of the area has a number for the measure (a non-number never counts
   as 0) and the area's cities with districts (TERYT: Wrocław 0264, Łódź 1061, Kraków 1261, Warszawa 1465, Poznań
   3064) are printed with all their codes, the city row and every district (6, 6, 5, 19 and 6 codes; macro
   poc_kgpsp_interventions_incomplete_cities): the 2025 tables have one row for each of these cities (Warszawa 1,852
   incidents against 22,847 summed over 19 rows in 2024; Dev trial 45756529767), so 2025 has no value for Poland and
   those five voivodeships. Gmina rows themselves are not in Gold yet. #}
with silver as (
    select * from {{ ref('kgpsp_interventions_counts') }}
),

published as (
    select
        report_year,
        table_code,
        area_code,
        measure_code,
        value,
        source_version,
        raw_sha256
    from silver
    where area_level in ('country', 'voivodeship') and value_status = 'ok'
),

-- a city with districts whose tables lack any of its TERYT codes (the city row and every district; one rule with the
-- warning test kgpsp_interventions_city_districts): its voivodeship and Poland are not summed for that year and table
incomplete_cities as (
    {{ poc_kgpsp_interventions_incomplete_cities(ref('kgpsp_interventions_counts')) }}
),

from_gminas as (
    select
        report_year,
        table_code,
        area_code,
        measure_code,
        sum(value) as value,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from (
        select
            report_year,
            table_code,
            voivodeship_code as area_code,
            measure_code,
            value,
            value_status,
            source_version,
            raw_sha256
        from silver
        where area_level = 'gmina'
        union all
        select
            report_year,
            table_code,
            'PL' as area_code,
            measure_code,
            value,
            value_status,
            source_version,
            raw_sha256
        from silver
        where area_level = 'gmina'
    ) as g
    where
        not exists (
            select 1
            from incomplete_cities as i
            where
                i.report_year = g.report_year and i.table_code = g.table_code
                and (g.area_code = 'PL' or i.voivodeship_code = g.area_code)
        )
    group by report_year, table_code, area_code, measure_code
    having count(*) filter (where value_status <> 'ok') = 0
),

observations as (
    select * from published
    union all
    select * from from_gminas
),

incident_names as (
    select *
    from (
        values
        ('P', 'Fires'), ('P/M', 'Small fires'), ('P/Ś', 'Medium fires'), ('P/D', 'Large fires'),
        ('P/BD', 'Very large fires'), ('MZ', 'Local hazards'), ('MZ/M', 'Small local hazards'),
        ('MZ/L', 'Local hazards of local extent'), ('MZ/Ś', 'Medium local hazards'), ('MZ/D', 'Large local hazards'),
        ('MZ/K', 'Giant local hazards or natural disasters'), ('AF', 'False alarms'),
        ('A/Z', 'False alarms, malicious'), ('A/DW', 'False alarms in good faith'),
        ('A/IW', 'False alarms from detection systems'), ('OGÓŁEM', 'Incidents, all types')
    ) as t (code, name)
),

geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'kgpsp_interventions'
),

labelled as (
    select
        o.*,
        case o.table_code when '1' then 'incidents' else 'casualties' end as block_id,
        case o.table_code when '1' then 'incidents' else 'persons' end as unit_code,
        case
            when o.table_code = '1' then n.name
            else
                case split_part(o.measure_code, ':', 3) when 'smiertelne' then 'Deaths' when 'ranni' then 'Injured' end
                || case split_part(o.measure_code, ':', 1)
                    when 'P' then ' in fires' when 'MZ' then ' in local hazards'
                    when 'OGÓŁEM' then ' in fires and local hazards'
                end
                || case split_part(o.measure_code, ':', 2)
                    when 'ratownicy' then ', rescuers' when 'strazacy' then ', of which firefighters'
                    when 'inne_osoby' then ', other persons' when 'dzieci' then ', of which children'
                    when 'ogolem' then ', all persons'
                end
        end as measure_text
    from observations as o
    left join incident_names as n on o.table_code = '1' and o.measure_code = n.code
)

select
    'kgpsp_interventions' as source_system,
    s.block_id,
    case s.block_id
        when 'incidents' then 'KG PSP incidents by type and size'
        else 'KG PSP casualties of fires and local hazards'
    end as block_name,
    'kgpsp_interventions:' || s.block_id || ':' || s.unit_code || ':' || s.measure_code as measure_key,
    s.measure_text || ' (State Fire Service, standard tables)' as measure_name,
    s.measure_code as measure_codes,
    s.unit_code,
    s.unit_code as unit_name,
    s.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(s.report_year, 1, 1) as date_key,
    cast(s.report_year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(s.value as double) as value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from labelled as s
left join geo as g on s.area_code = g.source_code
