{{ config(alias='build_knf_insurance', tags=['gold_block']) }}
{# Gold build stage: KNF insurance and pension statistics (register PLG-018, Silver knf_insurance_observations), the
   market totals for Poland as published (unit and scale as printed; no rescaling). No breakdown: the business line,
   product and measure are part of the measure. Blocks, one frequency each:
   insurance_market  quarterly bulletin part A totals ("Razem / Total"), life and non-life insurers: gross written
                     premium, earned premium net of reinsurance, gross claims paid, claims incurred net of
                     reinsurance; thousand zloty, cumulative from 1 January to the quarter end (Q)
   ofe               open pension funds (OFE) total: net assets (zloty) and members (persons) at the month end (M)
   ike_ikze          individual retirement accounts (IKE) and individual retirement security accounts (IKZE), all
                     providers: accounts held at 31 December (szt.) and their value (thousand zloty) (A; an
                     edition at 30 June, frequency S, has no fact model and stays in Silver)
   The area is Poland ('PL' through poc_map_geography). #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'knf_insurance'
),

observations as (
    select
        series,
        table_code,
        business_line,
        measure_code,
        unit_code,
        unit_label,
        frequency,
        period_start,
        period_label,
        value,
        source_version,
        raw_sha256,
        case
            when series = 'bulletin' then 'insurance_market'
            when series = 'ofe' then 'ofe'
            else 'ike_ikze'
        end as block_id,
        case
            when series = 'bulletin' then business_line || ':' || measure_code
            when series = 'ofe' then measure_code
            when measure_code = 'count' then series || ':accounts'
            else series || ':assets'
        end as codes
    from {{ ref('knf_insurance_observations') }}
    where
        (series = 'bulletin' and is_total)
        or (series = 'ofe' and is_total and measure_code in ('net_assets', 'members'))
        or (
            series in ('ike', 'ikze') and frequency = 'A' and provider = '_T'
            and item_label like '%prowadzone na koniec okresu sprawozdawczego%'
        )
),

names as (
    select * from (
        values
        ('life:gross_written_premium', 'Life insurers: gross written premium, cumulative from 1 January'),
        ('life:earned_premium_net', 'Life insurers: earned premium net of reinsurance, cumulative from 1 January'),
        ('life:claims_paid_gross', 'Life insurers: gross claims paid, cumulative from 1 January'),
        ('life:claims_incurred_net', 'Life insurers: claims incurred net of reinsurance, cumulative from 1 January'),
        ('non_life:gross_written_premium', 'Non-life insurers: gross written premium, cumulative from 1 January'),
        (
            'non_life:earned_premium_net',
            'Non-life insurers: earned premium net of reinsurance, cumulative from 1 January'
        ),
        ('non_life:claims_paid_gross', 'Non-life insurers: gross claims paid, cumulative from 1 January'),
        (
            'non_life:claims_incurred_net',
            'Non-life insurers: claims incurred net of reinsurance, cumulative from 1 January'
        ),
        ('net_assets', 'Open pension funds (OFE): net assets, end of month'),
        ('members', 'Open pension funds (OFE): members, end of month'),
        ('ike:accounts', 'IKE: accounts held at 31 December, all providers'),
        ('ike:assets', 'IKE: value of accounts held at 31 December, all providers'),
        ('ikze:accounts', 'IKZE: accounts held at 31 December, all providers'),
        ('ikze:assets', 'IKZE: value of accounts held at 31 December, all providers')
    ) as t (codes, measure_name)
),

blocks as (
    select * from (
        values
        ('insurance_market', 'KNF quarterly insurance bulletin, market totals (year to date)'),
        ('ofe', 'KNF open pension funds (OFE), monthly'),
        ('ike_ikze', 'KNF individual retirement accounts (IKE, IKZE), yearly')
    ) as t (block_id, block_name)
)

select
    'knf_insurance' as source_system,
    o.block_id,
    b.block_name,
    'knf_insurance:' || o.block_id || ':' || o.unit_code || ':' || o.codes as measure_key,
    n.measure_name,
    o.codes as measure_codes,
    o.unit_code,
    coalesce(regexp_replace(o.unit_label, '^w ', ''), o.unit_code) as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    o.frequency,
    o.period_start as date_key,
    o.period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(o.value as double) as value,
    'ok' as observation_status_key,
    o.source_version,
    o.raw_sha256
from observations as o
inner join blocks as b on o.block_id = b.block_id
left join names as n on o.codes = n.codes
left join geo as g on g.source_code = 'PL'
