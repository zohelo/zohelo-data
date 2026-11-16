{{ config(alias='build_arimr_payments', tags=['gold_block']) }}
{# Gold build stage: ARiMR direct-payment statistics (register PLS-034), silver.arimr_payments_direct_payments.
   Block 'direct_payments': one measure_key per measure (frequency A only, the campaign year; date_key = 1 January
   of the campaign). Values are cumulative as of the newest report holding the campaign (period_label names that
   date); a campaign whose payment window (to 30 June of the following year) had not closed at that date is
   'provisional'. The area is the voivodeship name or 'polska', mapped in poc_map_geography (TERYT names). #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'arimr_payments'
)

select
    'arimr_payments' as source_system,
    'direct_payments' as block_id,
    'ARiMR direct payments (applications, decisions, amounts paid by campaign)' as block_name,
    'arimr_payments:direct_payments:' || s.unit_code || ':' || s.measure_code as measure_key,
    case s.measure_code
        when 'applications' then 'Applications for area payments submitted, by campaign (ARiMR)'
        when 'decisions' then 'Decisions granting area payments issued, by campaign (ARiMR)'
        when 'amount_paid' then 'Direct payments paid, by campaign (ARiMR)'
    end as measure_name,
    s.measure_code as measure_codes,
    s.unit_code,
    case s.unit_code when 'PLN' then 'złoty' when 'applications' then 'applications' when 'decisions' then 'decisions' end
        as unit_name,
    s.area_name as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(s.campaign_year, 1, 1) as date_key,
    'Kampania ' || s.campaign_year || ' (stan na ' || strftime(s.as_of_date, '%d.%m.%Y') || ')' as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(s.value as double) as value,
    case when s.as_of_date < make_date(s.campaign_year + 1, 7, 1) then 'provisional' else 'ok' end
        as observation_status_key,
    s.source_version,
    s.raw_sha256
from {{ ref('arimr_payments_direct_payments') }} as s
left join geo as g on s.area_name = g.source_code
where s.value is not null
