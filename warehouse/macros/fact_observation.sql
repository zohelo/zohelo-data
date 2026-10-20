{# Incremental Gold fact_observation (models/gold/fact_observation.sql, owner policy #822, #870).
   A fact row's recipe is its source_system and the Silver input file it came from
   (raw_sha256). recipes: one row per current input file with at least one non-null value.
   Each source joins with its own union branch (BDL #749, Eurostat #840). raw_sha256 is
   coalesced to '' on both sides, so the NOT IN key predicates never see a null. #}
{% macro fact_observation_recipes() -%}
recipes as (
    select distinct
        'world_bank_wdi' as source_system,
        coalesce(o.raw_sha256, '') as raw_sha256
    from {{ ref('wdi_observations') }} as o
    where o.value is not null
)
{%- endmacro %}

{# Pre-hook: delete the rows whose recipe is no longer current (a source's superseded file). #}
{% macro fact_observation_clear() -%}
  delete from {{ this }}
  where source_system || '|' || coalesce(raw_sha256, '') not in (
      with {{ fact_observation_recipes() }}
      select r.source_system || '|' || r.raw_sha256 from recipes as r
  )
{%- endmacro %}
