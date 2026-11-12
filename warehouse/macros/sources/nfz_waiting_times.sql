{# nfz_waiting_times (register PLS-015, #2956): the typed snapshot rows both Silver models read, and the Gold
   registration (decision D34, docs/source-registration.md). #}

{# Every queue record of every complete snapshot month in bronze.nfz_waiting_times_queues, typed. Entity
   queues_<YYYYMM>_case<c>_<pp> (ingestion/to_landing/nfz_waiting_times/): the newest copy of each entity (by
   version, then ingest) is read; a month counts only when all 32 entities (2 cases x 16 voivodeships) are in
   Bronze, so a month cut short by the time budget is never shown half. Values as published: counts and periods
   cast to integers (a non-integer fails the build), the month of the statistics and the as-at date to dates, the
   forecast time to the benefit (pcus) kept as the published text; a missing value stays null. A record whose case
   differs from its entity's case fails the build. #}
{% macro poc_nfz_waiting_times_snapshots() %}
with newest as (
    select
        *,
        regexp_extract(entity, '^queues_([0-9]{6})_case([12])_(0[1-9]|1[0-6])$', ['yyyymm', 'case_no', 'province']) as part
    from {{ source('nfz_waiting_times', 'nfz_waiting_times_queues') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

months as (
    select part.yyyymm as yyyymm
    from newest
    group by 1
    having count(distinct entity) = 32
)

select
    strptime(n.part.yyyymm || '01', '%Y%m%d')::date as snapshot_month,
    coalesce(nullif(n.part.province, ''), error('nfz_waiting_times: unknown entity ' || n.entity)) as province_code,
    case
        when n.case_code = n.part.case_no then cast(n.case_code as integer)
        else error('nfz_waiting_times: case ' || coalesce(n.case_code, 'null') || ' in entity ' || n.entity)
    end as case_code,
    n.queue_id,
    n.benefit,
    n.provider_code,
    n.provider_name,
    n.teryt_provider,
    n.place_name,
    n.locality,
    n.county,
    n.teryt_place,
    n.registry_number,
    n.resort_part_vii,
    n.resort_part_viii,
    n.many_places,
    n.anesthesia,
    n.benefits_for_children,
    n.age_range,
    n.covid_19,
    n.queue_in_cer,
    cast(n.type_building as integer) as type_building,
    n.toilet,
    n.ramp,
    n.car_park,
    n.elevator,
    n.air_conditioning,
    n.automatic_door,
    n.wheelchairs,
    n.corridors,
    cast(n.awaiting as integer) as awaiting,
    cast(n.removed as integer) as removed,
    cast(n.average_period as integer) as average_period_days,
    strptime(n.statistics_month || '-01', '%Y-%m-%d')::date as statistics_month,
    cast(n.computed_average_period as integer) as computed_average_period_days,
    n.computed_update,
    cast(n.dates_applicable as boolean) as forecast_applicable,
    n.pcus as forecast_time_text,
    cast(n.date_situation_as_at as date) as forecast_as_at,
    n.entity,
    n.raw_sha256,
    n.source_version,
    n.ingested_at,
    n.page_number,
    n.source_row_number
from newest as n
inner join months as m on n.part.yyyymm = m.yyyymm
{% endmacro %}

{% macro poc_source__nfz_waiting_times() %}
    {{ return({
        'block_model': 'poc_build_nfz_waiting_times',
        'blocks': [['waiting_lists', 'NFZ waiting lists by voivodeship (monthly snapshots of API Terminy Leczenia)']],
    }) }}
{% endmacro %}

{# NFZ voivodeship codes 01-16 number the voivodeships in alphabetical order, as TERYT does with the even codes
   02-32 (NFZ 01 dolnośląskie = TERYT 02, ..., 16 zachodniopomorskie = TERYT 32; checked against the TERYT code of
   every place by tests/nfz_waiting_times_complete.sql); '00' is Poland (sum of the 16). A static list from
   dim_geography, so the map never waits for this source. #}
{% macro poc_geography__nfz_waiting_times() %}
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
)
{% endmacro %}
