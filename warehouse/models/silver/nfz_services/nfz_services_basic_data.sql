{# NFZ statistics API, services (JGP): the basic-data table ("Dane ogólne") of each benefit and period, for Poland
   and each of the 16 NFZ regional branches, newest copy of each benefit. Grain: catalog × benefit_code ×
   period_start × period_end × area_code (area_code 'PL' for the Poland answer, else the NFZ branch code 01-16),
   for the records the API gives once per table and area (area_records = 1). A table whose answer repeats an area
   keeps every record with area_records > 1 and its source_row_number; they are not chosen between, Gold leaves
   them out and tests/nfz_services_repeated_areas.sql lists them (warning). A whole-year table has period_kind
   'year' (1 January to 31 December); a part-year table (the index's ``periods``, where a benefit changed within a
   year) has period_kind 'part_year' and its own dates. Values as published, typed: counts as integers, shares in
   per cent and the ratio as decimals, days as integers, PLN and points as decimals; a null stays null (the API
   publishes no value, e.g. average values after 2016), a branch without hospitalisations may have no row. In a row
   with 0 hospitalisations the API writes 0 for the ratio and the length of stay: a placeholder, not a measurement,
   so those are null here (the counts and the shares stay as published). A text that is not a number fails the
   cast, it is never zeroed. Source: bronze.nfz_services_basic_data (records 'value'). #}
with newest as (
    select *
    from {{ source('nfz_services', 'nfz_services_basic_data') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

typed as (
    select
        entity,
        catalog,
        benefit_code,
        benefit_name,
        table_id,
        cast(year as integer) as year,
        case when period_from is null then 'year' else 'part_year' end as period_kind,
        coalesce(cast(cast(period_from as timestamp) as date), make_date(cast(year as integer), 1, 1)) as period_start,
        coalesce(cast(cast(period_to as timestamp) as date), make_date(cast(year as integer), 12, 31)) as period_end,
        coalesce(branch, 'PL') as area_code,
        branch as branch_code,
        cast(number_of_patients as bigint) as number_of_patients,
        cast(number_of_hospitalizations as bigint) as number_of_hospitalizations,
        cast(ratio_of_rehospitalizations as double) as ratio_of_rehospitalizations,
        cast(percentage as double) as percentage,
        cast(percentage_of_sections as double) as percentage_of_sections,
        cast(duration_median as integer) as duration_median_days,
        cast(duration_mode as integer) as duration_mode_days,
        cast(average_value_of_hospitalization as double) as average_value_of_hospitalization_pln,
        cast(average_value_of_hospitalization_points as double) as average_value_of_hospitalization_points,
        cast(average_value_of_drg as double) as average_value_of_drg_pln,
        cast(average_value_of_drg_points as double) as average_value_of_drg_points,
        raw_sha256,
        source_version,
        ingested_at,
        source_row_number
    from newest
    where record = 'value'
)

select
    entity,
    catalog,
    benefit_code,
    benefit_name,
    table_id,
    year,
    period_kind,
    period_start,
    period_end,
    area_code,
    branch_code,
    count(*) over (partition by entity, table_id, area_code) as area_records,
    number_of_hospitalizations = 0 as without_hospitalisations,
    number_of_patients,
    number_of_hospitalizations,
    case when number_of_hospitalizations <> 0 then ratio_of_rehospitalizations end as ratio_of_rehospitalizations,
    percentage,
    percentage_of_sections,
    case when number_of_hospitalizations <> 0 then duration_median_days end as duration_median_days,
    case when number_of_hospitalizations <> 0 then duration_mode_days end as duration_mode_days,
    case
        when number_of_hospitalizations <> 0 then average_value_of_hospitalization_pln
    end as average_value_of_hospitalization_pln,
    case
        when number_of_hospitalizations <> 0 then average_value_of_hospitalization_points
    end as average_value_of_hospitalization_points,
    case when number_of_hospitalizations <> 0 then average_value_of_drg_pln end as average_value_of_drg_pln,
    case when number_of_hospitalizations <> 0 then average_value_of_drg_points end as average_value_of_drg_points,
    raw_sha256,
    source_version,
    ingested_at,
    source_row_number
from typed
