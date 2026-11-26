{{ config(alias='build_knf_pensions', tags=['gold_block']) }}
{# Gold build stage: KNF quarterly PPK information (register PLG-018), observations of
   silver.knf_pensions_ppk_net_assets. Block 'ppk_net_assets', one measure (frequency Q): net asset value of the PPK
   target-date funds at the end of the quarter, thousand PLN as published; the total row ('Suma') is the total, each
   managing institution a breakdown manager (its name as printed in the newest release). Poland only (geography
   'PL'). Cells marked '-' (cell_status no_value) are left out, never 0. #}
select
    'knf_pensions' as source_system,
    'ppk_net_assets' as block_id,
    'KNF PPK net asset value' as block_name,
    'knf_pensions:ppk_net_assets:thousand_pln' as measure_key,
    'Net asset value of PPK target-date funds (KNF)' as measure_name,
    'ppk_net_assets' as measure_codes,
    'thousand_pln' as unit_code,
    'tys. PLN' as unit_name,
    'PL' as geography_source_code,
    'PL' as geography_key,
    'Q' as frequency,
    make_date(period_year, period_quarter * 3 - 2, 1) as date_key,
    period_year || '-Q' || period_quarter as period_label,
    case
        when is_total then '{}'
        else cast(to_json(map(['manager'], [lower(manager_label)])) as varchar)
    end as breakdown_json,
    case when not is_total then cast(to_json(map(['manager'], [manager_label])) as varchar) end as breakdown_label_json,
    cast(null as varchar) as sex_code,
    is_total,
    value,
    'ok' as observation_status_key,
    source_version,
    raw_sha256
from {{ ref('knf_pensions_ppk_net_assets') }}
where cell_status = 'ok' and value is not null
