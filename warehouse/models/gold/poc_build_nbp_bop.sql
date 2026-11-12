{{ config(alias='build_nbp_bop', tags=['gold_block']) }}
{# Gold build stage: NBP balance of payments (bop_m monthly, bop_q quarterly) and international investment
   position (iip_q, end of quarter) of Poland from silver.nbp_bop_observations, in million PLN and million
   EUR (the USD sheets stay in Silver). One measure per workbook × currency × item; no breakdowns. IIP components
   and their parent totals (assets.direct_investment and its equity / debt instruments) are separate measures: a
   sum over measures double-counts. #}
select
    'nbp_bop' as source_system,
    entity as block_id,
    case entity
        when 'bop_m' then 'NBP balance of payments, monthly'
        when 'bop_q' then 'NBP balance of payments, quarterly'
        else 'NBP international investment position, quarterly'
    end as block_name,
    'nbp_bop:' || entity || ':' || currency || ':' || item_code as measure_key,
    case entity
        when 'iip_q' then 'International investment position (NBP)'
        else 'Balance of payments (NBP)'
    end || ' — ' || coalesce(item_en, item_code) || ' — million ' || currency as measure_name,
    currency || ':' || item_code as measure_codes,
    'mln_' || lower(currency) as unit_code,
    'million ' || currency as unit_name,
    'PL' as geography_source_code,
    'PL' as geography_key,
    frequency,
    date_key,
    period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    value,
    'ok' as observation_status_key,
    source_version,
    raw_sha256
from {{ ref('nbp_bop_observations') }}
where currency in ('PLN', 'EUR') and value is not null and date_key is not null
