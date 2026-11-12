{# NFZ statistics API, pharmacy refunds (bronze.nfz_ra_monthly_provisions, register PLS-013): one row per area x ATC
   selection x month of the newest download of each entity (all its pages), typed. Area PL = no province filter,
   01..16 = NFZ voivodeship codes; atc_code _T = no ATC filter, A ... V = ATC main group (filtered by group name).
   Values as published (PLN with grosze, packages with fractions); a NULL stays NULL. Grain: area_code x atc_code
   x period_start; a duplicated month fails the unique test. #}
with newest as (
    select *
    from {{ source('nfz_pharmacy_refunds', 'nfz_ra_monthly_provisions') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    entity,
    area_code,
    atc_code,
    atc_name,
    make_date(cast(year as integer), cast(month as integer), 1) as period_start,
    cast(quantity as double) as quantity,
    cast(refund as double) as refund,
    cast(donation as double) as donation,
    cast(contribution_of_patient as double) as contribution_of_patient,
    cast(value as double) as value,
    meta_count,
    page,
    source_row_number,
    raw_sha256,
    source_version,
    ingested_at
from newest
