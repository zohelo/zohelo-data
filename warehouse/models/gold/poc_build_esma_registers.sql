{{ config(alias='build_esma_registers', tags=['gold_block']) }}
{# Gold build stage: ESMA register entries with home member state Poland (silver.esma_registers_entities), an entity
   snapshot of the newest copy of the slice (latest-only: one date). Measure = entries by status as ESMA publishes it
   (measure_key esma_registers:upreg:entities:<Active|Inactive|_T>); breakdowns entity_type (ESMA's type code, e.g.
   MIF, AIF, UCI) and office_type (e.g. Branch), each alone with the other at _T (no cross-tabulation), _U = not
   stated. Frequency D: the snapshot belongs to the download day (UTC) of the Landing copy (source_version), the day
   the register showed this state; ESMA gives no date for the slice. Geography: the home member state through
   map_geography ('POLAND' → PL). Counts of the entries as listed; a combination with no entry has no row (never 0). #}
with entries as (
    select
        home_member_state as geo_code,
        coalesce(status, '_U') as status,
        coalesce(entity_type_code, '_U') as entity_type,
        coalesce(office_type, '_U') as office_type,
        source_version,
        raw_sha256
    from {{ ref('esma_registers_entities') }}
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'esma_registers'
),

{% set dims = ['status', 'entity_type', 'office_type'] %}
{% set cuts = [[], ['entity_type'], ['office_type']] %}
counted as (  -- each breakdown alone (the other _T), with and without the status
    {% for with_status in [false, true] %}{% for cut in cuts %}
        {% set kept = (['status'] if with_status else []) + cut %}
        {% if not loop.first or with_status %}union all{% endif %}
        select
            geo_code,
            {% for d in dims %}
                {{ d if d in kept else "'_T'" }} as {{ d }},
            {% endfor %}
            count(*) as entries
        from entries
        group by geo_code{% for d in kept %}, {{ d }}{% endfor %}
    {% endfor %}{% endfor %}
),

snapshot as (
    select  -- Silver holds the newest copy only; a second copy is an error, not a choice
        case
            when count(distinct raw_sha256) > 1 then error('build_esma_registers: Silver holds several Landing copies')
            else max(source_version)
        end as source_version,
        max(raw_sha256) as raw_sha256
    from entries
)

select
    'esma_registers' as source_system,
    'upreg' as block_id,
    'ESMA register entries, home member state Poland (snapshot counts)' as block_name,
    'esma_registers:upreg:entities:' || c.status as measure_key,
    'ESMA register entries, home member state, status ' || c.status as measure_name,
    c.status as measure_codes,
    'entities' as unit_code,
    'register entries' as unit_name,
    c.geo_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'D' as frequency,
    cast(strptime(s.source_version, '%Y%m%dT%H%M%SZ') as date) as date_key,
    cast(cast(strptime(s.source_version, '%Y%m%dT%H%M%SZ') as date) as varchar) as period_label,
    cast(json_object('entity_type', c.entity_type, 'office_type', c.office_type) as varchar) as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    c.entity_type = '_T' and c.office_type = '_T' as is_total,
    cast(c.entries as double) as value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from counted as c
cross join snapshot as s
left join geo_map as g on c.geo_code = g.source_code
