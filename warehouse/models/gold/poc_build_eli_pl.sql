{{ config(alias='build_eli_pl', tags=['gold_block']) }}
{# Gold build stage: acts published in the Polish official journals per month (register PLG-026), Poland, from
   silver.eli_pl_acts, dated by the promulgation date (publication in the journal). One measure per journal,
   eli_pl:acts:acts:DU (Dziennik Ustaw) and eli_pl:acts:acts:MP (Monitor Polski), monthly (M); the breakdown is the
   act type as the API labels it, '_T' = all types (is_total). The journals are never summed: their date coverage
   differs.
   Coverage rule: the API gives no promulgation date for many older acts. A journal year (the year in the act's ELI
   id) enters Gold only when at most 1% of its acts lack the date; its undated acts are counted nowhere, and every
   month that counts an act of a journal year with undated acts is low_reliability. Journal years above 1% are left
   out whole, so no month is a silent undercount (the Dev trial lists the years). A month gets a row only when an
   act of that series was published (no row is a 0). The month of the newest download is provisional. raw_sha256 of
   a count is the SHA-256 of the sorted Landing hashes it was counted from. #}
with acts as (
    select * from {{ ref('eli_pl_acts') }}
),

journal_years as (
    select
        publisher,
        year,
        count(*) filter (where promulgation_date is null) as undated,
        count(*) as acts
    from acts
    group by publisher, year
),

dated as (
    select
        a.publisher,
        a.act_type,
        cast(date_trunc('month', a.promulgation_date) as date) as date_key,
        y.undated > 0 as from_incomplete_year,
        a.source_version,
        a.raw_sha256
    from acts as a
    inner join journal_years as y on a.publisher = y.publisher and a.year = y.year
    where a.promulgation_date is not null and y.undated <= 0.01 * y.acts
),

snapshot as (
    select cast(strptime(max(source_version), '%Y%m%dT%H%M%SZ') as date) as snapshot_date
    from acts
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'eli_pl'
),

{% set levels = ["act_type", "'_T'"] %}
counted as (
    {% for act_type in levels %}
        {% if not loop.first %}union all{% endif %}
        select
            publisher,
            {{ act_type }} as act_type,
            date_key,
            count(*) as acts,
            bool_or(from_incomplete_year) as from_incomplete_year,
            max(source_version) as source_version,
            sha256(string_agg(distinct raw_sha256, '|' order by raw_sha256)) as raw_sha256
        from dated
        group by all
    {% endfor %}
)

select
    'eli_pl' as source_system,
    'acts' as block_id,
    'Acts published in Dziennik Ustaw and Monitor Polski' as block_name,
    'eli_pl:acts:acts:' || c.publisher as measure_key,
    case c.publisher
        when 'DU' then 'Acts published in Dziennik Ustaw per month'
        else 'Acts published in Monitor Polski per month'
    end || ' (ELI, Chancellery of the Sejm)' as measure_name,
    c.publisher as measure_codes,
    'acts' as unit_code,
    'acts' as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'M' as frequency,
    c.date_key,
    strftime(c.date_key, '%Y-%m') as period_label,
    cast(json_object('type', c.act_type) as varchar) as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    c.act_type = '_T' as is_total,
    cast(c.acts as double) as value,
    case
        when c.date_key = cast(date_trunc('month', s.snapshot_date) as date) then 'provisional'
        when c.from_incomplete_year then 'low_reliability'
        else 'ok'
    end as observation_status_key,
    c.source_version,
    c.raw_sha256
from counted as c
cross join snapshot as s
left join geo_map as g on g.source_code = 'PL'
