{{ config(alias='build_knf_entities', tags=['gold_block']) }}
{# Gold build stage: entries of the ESMA registers whose home member state is Poland (register PLG-016, #2994),
   counted from silver.knf_entities_esma_entries, Poland only (the register gives a free-text head-office address,
   not a coded location, and the address is not loaded). Three blocks, one frequency each (a series_key never holds
   two frequencies):
   - register_entries (entity snapshot, D at the download day of the newest answer; latest-only, one date):
     knf_entities:register_entries:entries:<Active | Inactive | _T> by status (the measure), with the breakdowns
     entity_type (ae_entityTypeCode) and office_type (Head office | Branch) one at a time, the other at _T;
   - authorisations (A): knf_entities:authorisations:entries, entries by the year of their authorisation or
     notification date, in total and by entity_type; only entries still listed (Active or Inactive) are counted, and
     the date is the register's (AIFMs registered when the register started carry that day);
   - withdrawals (A): knf_entities:withdrawals:entries, entries by the year of their withdrawal date, in total and
     by entity_type.
   Only periods with an entry have a row (no zero filling, never a 0); the year of the download day is provisional.
   raw_sha256 = the Landing file's SHA-256 (one file). #}
with entries as (
    select
        entity_type_code,
        office_type,
        status,
        authorised_on,
        withdrawn_on,
        source_version,
        raw_sha256
    from {{ ref('knf_entities_esma_entries') }}
),

snapshot as (
    select
        cast(strptime(max(source_version), '%Y%m%dT%H%M%SZ') as date) as snapshot_date,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from entries
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'knf_entities'
),

{% set cuts = [["'_T'", "'_T'"], ['entity_type_code', "'_T'"], ["'_T'", 'office_type']] %}
{% set ns = namespace(first=true) %}
register_counts as (  -- each breakdown alone (the other _T), by status and over both statuses
    {% for status in ['status', "'_T'"] %}{% for etype, otype in cuts %}
        {% if not ns.first %}union all{% endif %}{% set ns.first = false %}
        select
            {{ status }} as status,
            {{ etype }} as entity_type,
            {{ otype }} as office_type,
            count(*) as entries
        from entries
        group by all
    {% endfor %}{% endfor %}
),

{% set events = [('authorisations', 'authorised_on', 'authorised'), ('withdrawals', 'withdrawn_on', 'withdrawn')] %}
{% set ns = namespace(first=true) %}
yearly_counts as (  -- entries by the year of the event, in total and by entity type
    {% for block, column, code in events %}{% for etype in ["'_T'", 'entity_type_code'] %}
        {% if not ns.first %}union all{% endif %}{% set ns.first = false %}
        select
            '{{ block }}' as block_id,
            '{{ code }}' as measure_codes,
            cast(date_trunc('year', {{ column }}) as date) as date_key,
            {{ etype }} as entity_type,
            count(*) as entries
        from entries
        where {{ column }} is not null
        group by all
    {% endfor %}{% endfor %}
),

observations as (
    select
        'register_entries' as block_id,
        'Polish entries of the ESMA registers by status' as block_name,
        'knf_entities:register_entries:entries:' || r.status as measure_key,
        'Polish entries of the ESMA registers, status ' || r.status as measure_name,
        r.status as measure_codes,
        'D' as frequency,
        s.snapshot_date as date_key,
        cast(s.snapshot_date as varchar) as period_label,
        cast(json_object('entity_type', r.entity_type, 'office_type', r.office_type) as varchar) as breakdown_json,
        r.entity_type = '_T' and r.office_type = '_T' as is_total,
        r.entries,
        'ok' as observation_status_key
    from register_counts as r
    cross join snapshot as s
    union all
    select
        y.block_id,
        case y.block_id
            when 'authorisations' then 'Polish entries of the ESMA registers by year of authorisation'
            else 'Polish entries of the ESMA registers by year of withdrawal'
        end as block_name,
        'knf_entities:' || y.block_id || ':entries' as measure_key,
        case y.block_id
            when 'authorisations' then 'Polish entries of the ESMA registers, by year of authorisation or notification'
            else 'Polish entries of the ESMA registers, by year of withdrawal'
        end as measure_name,
        y.measure_codes,
        'A' as frequency,
        y.date_key,
        strftime(y.date_key, '%Y') as period_label,
        cast(json_object('entity_type', y.entity_type) as varchar) as breakdown_json,
        y.entity_type = '_T' as is_total,
        y.entries,
        case when y.date_key = cast(date_trunc('year', s.snapshot_date) as date) then 'provisional' else 'ok' end
            as observation_status_key
    from yearly_counts as y
    cross join snapshot as s
)

select
    'knf_entities' as source_system,
    o.block_id,
    o.block_name,
    o.measure_key,
    o.measure_name,
    o.measure_codes,
    'entries' as unit_code,
    'register entries' as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    o.frequency,
    o.date_key,
    o.period_label,
    o.breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    o.is_total,
    cast(o.entries as double) as value,
    o.observation_status_key,
    s.source_version,
    s.raw_sha256
from observations as o
cross join snapshot as s
left join geo_map as g on g.source_code = 'PL'
