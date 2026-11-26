{# GUS STRATEG (register PLS-003): the indicators of the Strategy for Responsible Development (SOR, strategy 78) from
   the newest copy of entity sor (bronze.gus_strateg_records, record_kind 'indicator'). One row per indicator dimension:
   names and unit as published in Polish and English, the SOR objective, the levels and years STRATEG offers, and the
   strategy's base, intermediate and target values as published JSON text ({year: value}; targets such as
   'co najmniej 75,0%' or '76-80' are text, not observations, and never reach Gold). target_territory_code is the
   territory the base and target values belong to. Grain: indicator_id x dimension_id. #}
with records as (
    select *
    from {{ source('gus_strateg', 'gus_strateg_records') }}
    where record_kind = 'indicator'
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    cast(indicator_id as integer) as indicator_id,
    cast(dimension_id as integer) as dimension_id,
    indicator_name_pl,
    indicator_name_en,
    coalesce(nullif(unit_pl, ''), '_Z') as unit_code,
    unit_en,
    objective_pl,
    territory_code as target_territory_code,
    territory_levels_json,
    years_json,
    base_value_json,
    intermediate_values_json,
    target_value_json,
    raw_sha256,
    source_version
from records
