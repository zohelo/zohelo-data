{{ config(alias='build_eli_pl', tags=['gold_block']) }}
{# Gold build stage: acts published in the Polish official journals per month (register PLG-026), Poland, from
   silver.eli_pl_acts, dated by the promulgation date (publication in the journal; an act without one is not
   counted, the Dev trial counts them). One measure, eli_pl:acts:acts:monthly (M), with breakdowns as published:
   journal (DU = Dziennik Ustaw, MP = Monitor Polski) x act type (the API's Polish label); '_T' = all journals or
   all types. Only the total of both journals and all types is is_total (the KPI series); each journal total and
   each journal x type are their own series. A month gets a row only when an act of that series was published (no
   row is a 0). The month of the newest download is provisional. raw_sha256 of a count is the SHA-256 of the sorted
   Landing hashes it was counted from. #}
with acts as (
    select
        publisher,
        act_type,
        cast(date_trunc('month', promulgation_date) as date) as date_key,
        source_version,
        raw_sha256
    from {{ ref('eli_pl_acts') }}
    where promulgation_date is not null
),

snapshot as (
    select cast(strptime(max(source_version), '%Y%m%dT%H%M%SZ') as date) as snapshot_date
    from {{ ref('eli_pl_acts') }}
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'eli_pl'
),

{% set levels = [("publisher", "act_type"), ("publisher", "'_T'"), ("'_T'", "'_T'")] %}
counted as (
    {% for journal, act_type in levels %}
        {% if not loop.first %}union all{% endif %}
        select
            {{ journal }} as journal,
            {{ act_type }} as act_type,
            date_key,
            count(*) as acts,
            max(source_version) as source_version,
            sha256(string_agg(distinct raw_sha256, '|' order by raw_sha256)) as raw_sha256
        from acts
        group by all
    {% endfor %}
)

select
    'eli_pl' as source_system,
    'acts' as block_id,
    'Acts published in Dziennik Ustaw and Monitor Polski' as block_name,
    'eli_pl:acts:acts:monthly' as measure_key,
    'Acts published per month in the official journals (ELI, Chancellery of the Sejm)' as measure_name,
    'monthly' as measure_codes,
    'acts' as unit_code,
    'acts' as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'M' as frequency,
    c.date_key,
    strftime(c.date_key, '%Y-%m') as period_label,
    cast(json_object('journal', c.journal, 'type', c.act_type) as varchar) as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    c.journal = '_T' and c.act_type = '_T' as is_total,
    cast(c.acts as double) as value,
    case
        when c.date_key = cast(date_trunc('month', s.snapshot_date) as date) then 'provisional'
        else 'ok'
    end as observation_status_key,
    c.source_version,
    c.raw_sha256
from counted as c
cross join snapshot as s
left join geo_map as g on g.source_code = 'PL'
