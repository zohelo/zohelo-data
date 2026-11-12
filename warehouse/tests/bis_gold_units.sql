-- BIS (#2861): gold.build_bis names the unit of every built series from macro poc_bis_measures; the BIS sends the
-- unit as UNIT_MEASURE on the series. Fails with the series whose UNIT_MEASURE in silver.bis_observations is no
-- longer the one the macro expects (a rebased index, a changed unit), so a unit never changes under a measure key.
with measures as ({{ poc_bis_measures() }}),

series as (
    select
        o.entity,
        o.series_key,
        o.unit_measure,
        case o.entity
            when 'cbpol' then ''
            when 'eer'
                then
                    json_extract_string(o.dimensions_json, '$.EER_TYPE')
                    || '.' || json_extract_string(o.dimensions_json, '$.EER_BASKET')
            when 'spp'
                then
                    json_extract_string(o.dimensions_json, '$.VALUE')
                    || '.' || json_extract_string(o.dimensions_json, '$.UNIT_MEASURE')
            when 'tc'
                then
                    json_extract_string(o.dimensions_json, '$.TC_BORROWERS')
                    || '.' || json_extract_string(o.dimensions_json, '$.TC_LENDERS')
                    || '.' || json_extract_string(o.dimensions_json, '$.VALUATION')
                    || '.' || json_extract_string(o.dimensions_json, '$.UNIT_TYPE')
                    || '.' || json_extract_string(o.dimensions_json, '$.TC_ADJUST')
            when 'credit_gap'
                then
                    json_extract_string(o.dimensions_json, '$.TC_BORROWERS')
                    || '.' || json_extract_string(o.dimensions_json, '$.TC_LENDERS')
                    || '.' || json_extract_string(o.dimensions_json, '$.CG_DTYPE')
            when 'dsr' then json_extract_string(o.dimensions_json, '$.DSR_BORROWERS')
        end as series_code
    from {{ ref('bis_observations') }} as o
    where o.obs_value is not null
    group by all
)

select
    s.entity,
    s.series_key,
    s.unit_measure,
    m.source_unit as expected_unit_measure
from series as s
inner join measures as m on s.entity = m.entity and s.series_code = m.series_code
where s.unit_measure is distinct from m.source_unit
