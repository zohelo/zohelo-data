{# UNCTAD geography codes (D34 registration, docs/source-registration.md). Reads the source's own Silver
   data through poc_optional_ref, so models/gold/poc_map_geography.sql lists those models in its config meta
   `optional_parents` (ops/repo_check.py pairs the two). #}
{% macro poc_geography__unctad() %}
-- UNCTAD: economies by UNCTAD code (M49; France 250 in the FDI, merchandise and services tables, 251 only in port
-- calls) from poc_unctad_areas: the EU27 members, 3412 European Union (2020 …) = EU27_2020, 0000 World. Other
-- economies have no ISO code in the tables (_X); the build stage builds only the listed areas, and
-- tests/unctad_areas_present.sql fails when a listed code is missing from one of the three built tables.
unctad_areas as ({{ poc_unctad_areas() }}),

unctad as (
    select
        'unctad' as source_system,
        c.economy as source_code,
        coalesce(g.geography_key, '_X') as geography_key
    from (
        select distinct economy from {{ poc_optional_ref('unctad_fdi_flows_stock', 'economy:varchar') }}
        union
        select distinct economy from {{ poc_optional_ref('unctad_merchandise_trade', 'economy:varchar') }}
        union
        select distinct economy from {{ poc_optional_ref('unctad_services_trade', 'economy:varchar') }}
    ) as c
    left join unctad_areas as a on c.economy = a.economy
    left join geo as g on a.candidate_key = g.geography_key
    where c.economy is not null
)
{% endmacro %}
