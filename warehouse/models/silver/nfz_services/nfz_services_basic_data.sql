{# NFZ statistics API, services (JGP): the basic-data ("Dane ogólne") table of each benefit and period, for Poland
   and each of the 16 NFZ regional branches, newest copy of each benefit. Grain: catalog × benefit_code ×
   period_start × period_end × area_code (area_code 'PL' for the Poland answer, else the NFZ branch code 01-16).
   A whole-year table has period_kind 'year' (1 January to 31 December); a part-year table (the index's
   ``periods``, where a benefit changed within a year) has period_kind 'part_year' and its own dates. Values as
   published, typed: counts as integers, shares in per cent and the ratio as decimals, days as integers, PLN and
   points as decimals; a null stays null (the API publishes no value, e.g. average values in recent years), a
   branch without hospitalisations has no row. A text that is not a number fails the cast, it is never zeroed.
   Source: bronze.nfz_services_basic_data (records 'value'). #}
with newest as (
    select *
    from {{ source('nfz_services', 'nfz_services_basic_data') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

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
