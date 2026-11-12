{{ config(alias='build_senat', tags=['gold_block']) }}
{# Gold build stage: event counts of the Senate of the Republic of Poland, term XI (register PLG-025, route D36),
   Poland. Events of finished sittings only (status 'Zakończone'; a planned or running sitting is not counted):
   - block sitting_days: sitting days (silver.senat_sitting_days), dated by the day;
     senat:sitting_days:days:monthly (M) and senat:sitting_days:days:annual (A);
   - block votes: votes listed on the sitting days (silver.senat_votes), dated by their day;
     senat:votes:votes:monthly (M) and senat:votes:votes:annual (A).
   One frequency per measure_key, so a series_key never holds two frequencies. A period gets a row only when the
   Senate sat (or voted) in it: months without a sitting have no row, never a 0. The period holding the download
   date of the sittings list is provisional (a later sitting can still add to it). raw_sha256 of a count is the
   SHA-256 of the sorted Landing hashes it was counted from. #}
with sitting_days as (
    select
        day_date,
        raw_sha256,
        source_version
    from {{ ref('senat_sitting_days') }}
    where is_finished
),

votes as (
    select
        day_date,
        raw_sha256,
        source_version
    from {{ ref('senat_votes') }}
    where is_finished
),

snapshot as (
    select cast(strptime(max(source_version), '%Y%m%dT%H%M%SZ') as date) as snapshot_date
    from {{ ref('senat_sitting_days') }}
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'senat'
),

{% set combos = [] %}
{% for block in [('sitting_days', 'days'), ('votes', 'votes')] %}
    {% for freq in [('M', 'monthly', 'month', '%Y-%m'), ('A', 'annual', 'year', '%Y')] %}
        {% do combos.append((block, freq)) %}
    {% endfor %}
{% endfor %}
counted as (
    {% for (block, unit), (freq, measure_code, part, label) in combos %}
        {% if not loop.first %}union all{% endif %}
        select
            '{{ block }}' as block_id,
            '{{ unit }}' as unit_code,
            '{{ freq }}' as frequency,
            '{{ measure_code }}' as measure_code,
            cast(date_trunc('{{ part }}', day_date) as date) as date_key,
            strftime(date_trunc('{{ part }}', day_date), '{{ label }}') as period_label,
            count(*) as events,
            max(source_version) as source_version,
            sha256(string_agg(distinct raw_sha256, '|' order by raw_sha256)) as raw_sha256
        from {{ block }}
        group by all
    {% endfor %}
)

select
    'senat' as source_system,
    c.block_id,
    case c.block_id
        when 'sitting_days' then 'Senate sitting days (term XI)'
        else 'Senate votes (term XI)'
    end as block_name,
    'senat:' || c.block_id || ':' || c.unit_code || ':' || c.measure_code as measure_key,
    case c.block_id
        when 'sitting_days' then 'Senate sitting days, '
        else 'Votes held in the Senate, '
    end
    || case c.frequency when 'M' then 'per month' else 'per year' end || ' (Senate of the Republic of Poland)'
        as measure_name,
    c.measure_code as measure_codes,
    c.unit_code,
    case c.unit_code when 'days' then 'sitting days' else 'votes' end as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    c.frequency,
    c.date_key,
    c.period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(c.events as double) as value,
    case
        when c.frequency = 'M' and c.date_key = cast(date_trunc('month', s.snapshot_date) as date) then 'provisional'
        when c.frequency = 'A' and c.date_key = cast(date_trunc('year', s.snapshot_date) as date) then 'provisional'
        else 'ok'
    end as observation_status_key,
    c.source_version,
    c.raw_sha256
from counted as c
cross join snapshot as s
left join geo_map as g on g.source_code = 'PL'
