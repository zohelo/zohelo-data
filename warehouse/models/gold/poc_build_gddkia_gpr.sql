{{ config(alias='build_gddkia_gpr', tags=['gold_block']) }}
{# Gold build stage: GDDKiA traffic census GPR (register PL-ENV-022), block 'sdrr', one observation per census edition
   (frequency A, date_key = 1 January of the edition year: 2020 for GPR 2020/21, 2025) for Poland and each voivodeship,
   per road class (dk national roads, dw voivodeship roads; the class is part of measure_key). From
   silver.gddkia_gpr_sdrr, sections with a measured SDRR of motor vehicles only (value_status 'ok'; '-' sections are
   left out, never 0):
     motor_vehicles  length-weighted mean SDRR of motor vehicles = sum(SDRR x length) / sum(length), vehicles/day,
                     GDDKiA's own network average (Ruch drogowy publications), rounded to whole vehicles as published;
     trucks          the same for trucks without and with trailer (Sam. ciężarowe bez / z przyczepą), over the sections
                     where both truck classes are measured (weighted by those sections' length only);
     length_km       length of the sections with a measured SDRR of motor vehicles (km).
   A '-' in one class of a measured section (2020/21 national roads: agricultural tractors on 611 sections) leaves
   that class out; it never counts as 0.
   The voivodeship is the file's "Nr woj." of the section. #}
with sections as (
    select
        edition_code,
        edition_label,
        edition_year,
        road_class,
        section_code,
        voivodeship_code,
        length_km,
        max(value) filter (where vehicle_class = 'motor_vehicles' and value_status = 'ok') as motor_vehicles,
        case
            when
                count(*) filter (
                    where vehicle_class in ('trucks_without_trailer', 'trucks_with_trailer') and value_status = 'ok'
                ) = 2
                then sum(value) filter (where vehicle_class in ('trucks_without_trailer', 'trucks_with_trailer'))
        end as trucks,
        max(source_version) as source_version,
        max(raw_sha256) filter (where not from_erratum) as raw_sha256_main,
        max(raw_sha256) as raw_sha256_any
    from {{ ref('gddkia_gpr_sdrr') }}
    where length_km > 0
    group by all
),

areas as (
    select
        edition_code,
        edition_label,
        edition_year,
        road_class,
        area_code,
        sum(motor_vehicles * length_km) / sum(length_km) as motor_vehicles,
        sum(trucks * length_km) / sum(length_km) filter (where trucks is not null) as trucks,
        sum(length_km) as length_km,
        max(source_version) as source_version,
        coalesce(max(raw_sha256_main), max(raw_sha256_any)) as raw_sha256
    from (
        select
            *,
            voivodeship_code as area_code
        from sections
        union all
        select
            *,
            'PL' as area_code
        from sections
    ) as by_area
    where motor_vehicles is not null
    group by edition_code, edition_label, edition_year, road_class, area_code
),

measures as (
    select
        a.*,
        m.measure_code,
        m.unit_code,
        case m.measure_code
            when 'motor_vehicles' then round(a.motor_vehicles)
            when 'trucks' then round(a.trucks)
            else round(a.length_km, 3)
        end as value
    from areas as a
    cross join (
        values ('motor_vehicles', 'veh_per_day'), ('trucks', 'veh_per_day'), ('length_km', 'km')
    ) as m (measure_code, unit_code)
),

geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'gddkia_gpr'
)

select
    'gddkia_gpr' as source_system,
    'sdrr' as block_id,
    'GDDKiA traffic census: average annual daily traffic (SDRR)' as block_name,
    'gddkia_gpr:sdrr:' || s.unit_code || ':' || s.road_class || ':' || s.measure_code as measure_key,
    case s.measure_code
        when 'motor_vehicles' then 'Average annual daily traffic (SDRR) of motor vehicles, length-weighted, '
        when 'trucks' then 'Average annual daily traffic (SDRR) of trucks with and without trailer, length-weighted, '
        else 'Length of the measured sections, '
    end || case s.road_class when 'dk' then 'national roads' else 'voivodeship roads' end
    || ' (GDDKiA traffic census)' as measure_name,
    s.road_class || ':' || s.measure_code as measure_codes,
    s.unit_code,
    case s.unit_code when 'veh_per_day' then 'vehicles per day' else 'kilometres' end as unit_name,
    s.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(s.edition_year, 1, 1) as date_key,
    s.edition_label as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(s.value as double) as value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from measures as s
left join geo as g on s.area_code = g.source_code
