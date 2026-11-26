{{ config(alias='build_kprm_work_list', tags=['gold_block']) }}
{# Gold build stage: government drafts published on the work list of the Council of Ministers (register PLG-034,
   #491), Poland, counted from silver.kprm_work_list_drafts by the month (M) and the year (A) of Data publikacji.
   Block drafts_published, one measure_key per breakdown and frequency (a series_key never holds two frequencies):
   kprm_work_list:drafts_published:drafts:<breakdown>:<monthly|annual> with <breakdown>
   - total: all drafts (is_total);
   - kind: by Rodzaj dokumentu ({"document_kind": <as published>});
   - status: by the status of the downloaded list ({"status_key": realised | withdrawn | in_progress}); a draft's
     status changes after it is published, so these counts are the state on the download date;
   - responsible_body: by Organ odpowiedzialny ({"responsible_body": <as published>}; '_U' where the list names none).
   Only periods in which a draft was published have a row (no zero filling, never a 0). Planned adoption dates are
   plans and are not counted. The period holding the download date is provisional. raw_sha256 = the Landing file's
   SHA-256 (one file). #}
with drafts as (
    select
        published_on,
        document_kind,
        status_key,
        status_text,
        responsible_body,
        source_version,
        raw_sha256
    from {{ ref('kprm_work_list_drafts') }}
),

snapshot as (
    select cast(strptime(max(source_version), '%Y%m%dT%H%M%SZ') as date) as snapshot_date
    from drafts
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'kprm_work_list'
),

{% set breakdowns = [
    ('total', "'{}'", 'null'),
    ('kind', "json_object('document_kind', document_kind)::varchar", "json_object('document_kind', document_kind)::varchar"),
    ('status', "json_object('status_key', status_key)::varchar", "json_object('status_key', coalesce(status_text, 'w realizacji (brak statusu)'))::varchar"),
    ('responsible_body', "json_object('responsible_body', coalesce(responsible_body, '_U'))::varchar", "json_object('responsible_body', coalesce(responsible_body, 'nie podano (not given)'))::varchar"),
] %}
{% set freqs = [('M', 'monthly', 'month', '%Y-%m'), ('A', 'annual', 'year', '%Y')] %}
counted as (
    {% for code, breakdown, label in breakdowns %}
        {% for freq, freq_code, part, fmt in freqs %}
            {% if not (code == 'total' and freq == 'M') %}union all{% endif %}
            select
                '{{ code }}' as breakdown_code,
                '{{ freq }}' as frequency,
                '{{ freq_code }}' as frequency_code,
                cast(date_trunc('{{ part }}', published_on) as date) as date_key,
                strftime(date_trunc('{{ part }}', published_on), '{{ fmt }}') as period_label,
                {{ breakdown }} as breakdown_json,
                {{ label }} as breakdown_label_json,
                count(*) as drafts,
                max(source_version) as source_version,
                max(raw_sha256) as raw_sha256
            from drafts
            group by all
        {% endfor %}
    {% endfor %}
)

select
    'kprm_work_list' as source_system,
    'drafts_published' as block_id,
    'Government drafts published on the work list of the Council of Ministers' as block_name,
    'kprm_work_list:drafts_published:drafts:' || c.breakdown_code || ':' || c.frequency_code as measure_key,
    'Government drafts published on the work list of the Council of Ministers'
    || case c.breakdown_code
        when 'total' then ''
        when 'kind' then ', by kind of document'
        when 'status' then ', by current status'
        else ', by responsible body'
    end
    || case c.frequency when 'M' then ', per month' else ', per year' end as measure_name,
    c.breakdown_code || ':' || c.frequency_code as measure_codes,
    'drafts' as unit_code,
    'drafts' as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    c.frequency,
    c.date_key,
    c.period_label,
    c.breakdown_json,
    c.breakdown_label_json,
    cast(null as varchar) as sex_code,
    c.breakdown_code = 'total' as is_total,
    cast(c.drafts as double) as value,
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
