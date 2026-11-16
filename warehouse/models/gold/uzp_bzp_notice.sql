{{ config(alias='uzp_bzp_notice') }}
{# Gold entity table (register source, playbook section 7): one row per BZP contract or result notice, keyed by the
   BZP notice number (<year>/BZP <number>/<version>), from silver.uzp_bzp_notices. The publication day (UTC) is the
   date key (dim_calendar); the contracting authority's voivodeship is its geography key ('TERC:<code>',
   dim_geography) through map_geography (the same rule as gold.build_uzp_bzp; '_X' for a code that maps to no
   voivodeship, '_Z' when the notice gives none). Codes, contract and estimated values with their currency as
   published; contracting authorities as institutions, contractors only counted (no person data: Landing dropped
   contact persons, e-mails, phones and contractors' names, addresses and ids). #}
with notices as (
    select * from {{ ref('uzp_bzp_notices') }}
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'uzp_bzp'
)

select
    n.notice_number,
    n.bzp_number,
    n.object_id,
    n.tender_id,
    n.notice_type,
    n.publication_date as date_key,
    n.publication_ts_utc,
    n.client_type,
    n.order_type,
    n.tender_type,
    n.is_below_eu_threshold,
    n.order_object,
    n.cpv_codes,
    n.submitting_offers_ts_utc,
    n.procedure_result,
    n.organization_id,
    n.organization_national_id,
    n.organization_name,
    n.organization_city,
    n.organization_province,
    case
        when n.organization_province is null then '_Z'
        else coalesce(g.geography_key, '_X')
    end as geography_key,
    n.contractors,
    n.contractor_provinces,
    n.parts_concluded,
    n.parts_cancelled,
    n.contract_value,
    n.contract_value_currency,
    n.estimated_value,
    n.estimated_value_currency,
    n.source_version,
    n.raw_sha256
from notices as n
left join geo_map as g on n.organization_province = g.source_code
