{{ config(alias='build_kprm_work_list', tags=['gold_block']) }}
{# Gold build stage: event counts of the KPRM work list (silver.kprm_work_list_drafts), Poland. Event = a draft
   entering the Council of Ministers' work list, dated by its publication on the list (Data publikacji, Warsaw local
   time, month or year of it). Two measures, one frequency each, so a series_key never holds two frequencies:
   kprm_work_list:entries:list_entries:monthly (M) and kprm_work_list:entries:list_entries:annual (A). Breakdowns
   kind (Rodzaj dokumentu) and responsible_body (Organ odpowiedzialny, as published; _U where the cell is empty),
   each alone with the other at _T (no cross-tabulation). A period gets a row only when at least one draft entered
   the list in it: empty months are not filled with zeros. The period holding the file's download date is open
   (provisional). Counts are of the drafts the newest file lists: an entry the list no longer shows (numbers are
   skipped, e.g. UD has 503 rows up to UD505) is not counted. Not built: realised and withdrawn counts by date,
   because the file gives the entry's publication date, not the date of the status change. #}
with drafts as (
    select
        kind,
        coalesce(responsible_body, '_U') as responsible_body,
        published_at,
        source_version,
        raw_sha256
    from {{ ref('kprm_work_list_drafts') }}
),

snapshot as (
    select
        max(source_version) as source_version,
        cast(strptime(max(source_version), '%Y%m%dT%H%M%SZ') as date) as snapshot_date,
        min(raw_sha256) as raw_sha256,
        count(distinct raw_sha256) as files
    from drafts
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'kprm_work_list'
),

{% set combos = [] %}
{% for freq in [('M', 'monthly', 'month', '%Y-%m'), ('A', 'annual', 'year', '%Y')] %}
    {% for cut in [[], ['kind'], ['responsible_body']] %}{% do combos.append((freq, cut)) %}{% endfor %}
{% endfor %}
counted as (
    {% for (freq, measure_code, part, label), cut in combos %}
        {% if not loop.first %}union all{% endif %}
        select
            '{{ freq }}' as frequency,
            '{{ measure_code }}' as measure_code,
            cast(date_trunc('{{ part }}', published_at) as date) as date_key,
            strftime(date_trunc('{{ part }}', published_at), '{{ label }}') as period_label,
            {{ "kind" if "kind" in cut else "'_T'" }} as kind,
            {{ "responsible_body" if "responsible_body" in cut else "'_T'" }} as responsible_body,
            count(*) as drafts
        from drafts
        group by all
    {% endfor %}
)

select
    'kprm_work_list' as source_system,
    'entries' as block_id,
    'Drafts entering the Council of Ministers work list (KPRM)' as block_name,
    'kprm_work_list:entries:list_entries:' || c.measure_code as measure_key,
    'Government drafts entering the Council of Ministers work list, '
    || case c.frequency when 'M' then 'per month' else 'per year' end || ' (KPRM)' as measure_name,
    c.measure_code as measure_codes,
    'list_entries' as unit_code,
    'drafts entered on the work list' as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    c.frequency,
    c.date_key,
    c.period_label,
    cast(json_object('kind', c.kind, 'responsible_body', c.responsible_body) as varchar) as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    c.kind = '_T' and c.responsible_body = '_T' as is_total,
    cast(c.drafts as double) as value,
    case
        when c.frequency = 'M' and c.date_key = cast(date_trunc('month', s.snapshot_date) as date) then 'provisional'
        when c.frequency = 'A' and c.date_key = cast(date_trunc('year', s.snapshot_date) as date) then 'provisional'
        else 'ok'
    end as observation_status_key,
    s.source_version,
    case when s.files = 1 then s.raw_sha256 else error('build_kprm_work_list: Silver holds more than one file') end
        as raw_sha256
from counted as c
cross join snapshot as s
left join geo_map as g on g.source_code = 'PL'
