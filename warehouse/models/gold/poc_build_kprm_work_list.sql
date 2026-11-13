{{ config(alias='build_kprm_work_list', tags=['gold_block']) }}
{# Gold build stage: event counts of the KPRM work list (silver.kprm_work_list_drafts), Poland. Event = a draft
   entering the Council of Ministers' work list, dated by its publication on the list (Data publikacji, Warsaw local
   time, month or year of it). One frequency per measure_key, so a series_key never holds two frequencies:
   - kprm_work_list:entries:list_entries:monthly (M) and ...:annual (A): drafts entering the list; total, by kind
     (Rodzaj dokumentu) or by responsible body (Organ odpowiedzialny, as published; _U where the cell is empty),
     one breakdown at a time with the other at _T (no cross-tabulation);
   - kprm_work_list:entries:list_entries:monthly_by_current_status (M) and ...:annual_by_current_status (A): the
     drafts that entered the list in the period (month or year of publication on the list), by their status at
     download time, i.e. in the newest file (realised, withdrawn, in_progress). It changes retroactively: when a
     draft is realised or withdrawn, the value of its publication period moves between statuses at the next
     download. It is not a history of status changes: the file gives no date of a status change, so realised or
     withdrawn counts by the date of the change are not built.
     No total row (it is the entries total).
   A period gets a row only when at least one draft entered the list in it: empty months are not filled with zeros.
   The period holding the file's download date is open (provisional). Counts are of the drafts the newest file
   lists: an entry the list no longer shows (numbers are skipped, e.g. UD has 503 rows up to UD505) is not
   counted. #}
with drafts as (
    select
        kind,
        coalesce(responsible_body, '_U') as responsible_body,
        status,
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
{% set cuts = [[], ['kind'], ['responsible_body'], ['current_status']] %}
{% for freq in [('M', 'monthly', 'month', '%Y-%m'), ('A', 'annual', 'year', '%Y')] %}
    {% for cut in cuts %}{% do combos.append((freq, cut)) %}{% endfor %}
{% endfor %}
counted as (
    {% for (freq, measure_code, part, label), cut in combos %}
        {% if not loop.first %}union all{% endif %}
        select
            '{{ freq }}' as frequency,
            {% set suffix = "_by_current_status" if "current_status" in cut else "" %}
            '{{ measure_code ~ suffix }}' as measure_code,
            cast(date_trunc('{{ part }}', published_at) as date) as date_key,
            strftime(date_trunc('{{ part }}', published_at), '{{ label }}') as period_label,
            {{ "kind" if "kind" in cut else "'_T'" }} as kind,
            {{ "responsible_body" if "responsible_body" in cut else "'_T'" }} as responsible_body,
            {{ "status" if "current_status" in cut else "cast(null as varchar)" }} as current_status,
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
    case
        when c.current_status is null
            then 'Government drafts entering the Council of Ministers work list, per '
            || case c.frequency when 'M' then 'month' else 'year' end || ' of publication on the list (KPRM)'
        else 'Government drafts that entered the Council of Ministers work list, by '
            || case c.frequency when 'M' then 'month' else 'year' end
            || ' of publication on the list and by their status at download time (KPRM; earlier periods change'
            || ' as statuses change)'
    end as measure_name,
    c.measure_code as measure_codes,
    'list_entries' as unit_code,
    'drafts entered on the work list' as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    c.frequency,
    c.date_key,
    c.period_label,
    case
        when c.current_status is null
            then cast(json_object('kind', c.kind, 'responsible_body', c.responsible_body) as varchar)
        else cast(json_object('current_status', c.current_status) as varchar)
    end as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    c.current_status is null and c.kind = '_T' and c.responsible_body = '_T' as is_total,
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
