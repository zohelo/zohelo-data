{# NFZ statistics API, pharmacy refunds (bronze.nfz_ra_monthly_provisions, register PLS-013): one row per area x ATC
   selection x month of the newest download of each entity (all its pages), typed. Area PL = no province filter,
   01..16 = NFZ voivodeship codes; atc_code _T = no ATC filter, A ... V = ATC main group (filtered by group name).
   Values as published (PLN with grosze, packages with fractions); a NULL stays NULL. Grain: area_code x atc_code
   x period_start; a duplicated month fails the unique test. value_status: 'ok'; 'all_zero' when all five values of
   a row are 0 (no reimbursed package, no refund and no total cost in a whole voivodeship or in Poland is a gap the
   API fills with 0, not an observation: Dev trial 43812026974 found 105 such rows, every one of voivodeship 07
   mazowieckie, 15 selections x 7 months); 'national_without_province' for a Poland row whose ATC selection and month
   has an 'all_zero' voivodeship row (the API's Poland equals the sum of the provinces, so that total lacks the
   province). Values stay as published in Silver; Gold loads only 'ok' rows. #}
with newest as (
    select *
    from {{ source('nfz_pharmacy_refunds', 'nfz_ra_monthly_provisions') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

typed as (
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
),

flagged as (
    select
        *,
        coalesce(quantity = 0 and refund = 0 and donation = 0 and contribution_of_patient = 0 and value = 0, false)
            as all_zero
    from typed
),

gaps as (
    select distinct
        atc_code,
        period_start
    from flagged
    where all_zero and area_code <> 'PL'
)

select
    f.entity,
    f.area_code,
    f.atc_code,
    f.atc_name,
    f.period_start,
    f.quantity,
    f.refund,
    f.donation,
    f.contribution_of_patient,
    f.value,
    case
        when f.all_zero then 'all_zero'
        when f.area_code = 'PL' and g.atc_code is not null then 'national_without_province'
        else 'ok'
    end as value_status,
    f.meta_count,
    f.page,
    f.source_row_number,
    f.raw_sha256,
    f.source_version,
    f.ingested_at
from flagged as f
left join gaps as g on f.atc_code = g.atc_code and f.period_start = g.period_start
