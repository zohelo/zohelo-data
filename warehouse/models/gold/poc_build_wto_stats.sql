{{ config(alias='build_wto_stats', tags=['gold_block']) }}
{# Gold build stage: WTO Stats, OECD-WTO BaTIS BPM6 (register GL-020), block 'batis', from
   silver.wto_stats_services_trade. Selection (register modelling notes, the smallest valuable one): Poland as
   reporter, every partner, total services 'S' and the twelve EBOPS 2010 main items 'SA' … 'SL', exports and imports,
   annual. One measure per flow, item and value kind:
   wto_stats:batis:USD_MN:<X|M>.<item>.<final|balanced>, e.g. wto_stats:batis:USD_MN:X.S.final (Poland's services
   exports, the reporter's series after the OECD-WTO estimation) and …:X.S.balanced (the balanced matrix value, an
   estimate reconciling both partners' figures). The two kinds are different figures and never combined. The partner
   is the breakdown {"partner": "<BaTIS code>"}, the World 'WL' is the total (is_total).
   Status: final values the reporter published (methodology R_*) are 'ok'; final values aggregated from figures of
   different methodology (A) are 'flagged'; other final values (E*, M*, W0: estimated or modelled) and every
   balanced value are 'estimated'. Values and unit as published (D33: millions of US dollars). Areas through
   poc_map_geography (section wto_stats, static ISO2). Left out of Gold, kept in Silver: Poland as partner (the other
   reporters' figures), the country-group reporters, sub-items and derived items, reported_value (the reporter's own
   figure, mostly empty). A year after the build year cannot be an observation and is left out. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'wto_stats'
),

obs as (
    select
        s.*,
        k.value_kind,
        case k.value_kind when 'final' then s.final_value else s.balanced_value end as value
    from {{ ref('wto_stats_services_trade') }} as s
    cross join (values ('final'), ('balanced')) as k (value_kind)
    where
        s.reporter_code = 'PL'
        and s.item_code in ('S', 'SA', 'SB', 'SC', 'SD', 'SE', 'SF', 'SG', 'SH', 'SI', 'SJ', 'SK', 'SL')
)

select
    'wto_stats' as source_system,
    'batis' as block_id,
    'OECD-WTO BaTIS: trade in services by partner and EBOPS 2010 main item' as block_name,
    'wto_stats:batis:USD_MN:' || o.flow || '.' || o.item_code || '.' || o.value_kind as measure_key,
    case o.flow when 'X' then 'Services exports, ' else 'Services imports, ' end
    || o.item_name || ', ' || o.value_kind || ' value (OECD-WTO BaTIS BPM6)' as measure_name,
    o.flow || '.' || o.item_code || '.' || o.value_kind as measure_codes,
    'USD_MN' as unit_code,
    'millions of US dollars' as unit_name,
    o.reporter_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(o.year, 1, 1) as date_key,
    cast(o.year as varchar) as period_label,
    cast(json_object('partner', o.partner_code) as varchar) as breakdown_json,
    cast(json_object('partner', o.partner_name) as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    o.partner_code = 'WL' as is_total,
    o.value,
    case
        when o.value_kind = 'balanced' then 'estimated'
        when starts_with(o.final_value_methodology, 'R_') then 'ok'
        when o.final_value_methodology = 'A' then 'flagged'
        else 'estimated'
    end as observation_status_key,
    o.source_version,
    o.raw_sha256
from obs as o
left join geo as g on o.reporter_code = g.source_code
where o.value is not null and o.year <= year(current_date)
