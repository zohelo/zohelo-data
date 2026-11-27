{{ config(alias='build_knf_warnings', tags=['gold_block']) }}
{# Gold build stage: KNF public warning list (silver.knf_warnings_entries, register PLG-019), an entity snapshot counted
   per copy of the list: one daily observation per series at the copy's download date (frequency D; a date has a row
   only when the list changed and a new copy was written, never a carried-forward or 0 value). Two copies on one date
   (a manual rerun): the newest version of that date (a version pick). Poland. Block warning_list, unit list_entries,
   one measure_key per breakdown:
   - knf_warnings:warning_list:entries:total (all entries, is_total);
   - ...:legal_basis ({"legal_basis": section key}, the section heading in breakdown_label_json);
   - ...:prosecutor_level ({"prosecutor_level": krajowa | regionalna | okregowa | rejonowa | other});
   - ...:identifier_kind ({"identifier_kind": krs | nip_regon | not_registered_pl | no_data | other});
   - ...:final_ruling_mentioned (entries whose ruling column mentions a final ruling).
   A category absent from a copy has no row. Counts of notifications of a suspected offence, not of offences. #}
with newest as (
    select *
    from {{ ref('knf_warnings_entries') }}
    qualify rank() over (partition by snapshot_date order by ingested_at desc) = 1
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'knf_warnings'
),

counted as (
    select
        'total' as breakdown_code,
        snapshot_date,
        '{}' as breakdown_json,
        cast(null as varchar) as breakdown_label_json,
        count(*) as entries,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from newest
    group by snapshot_date
    union all
    select
        'legal_basis' as breakdown_code,
        snapshot_date,
        cast(json_object('legal_basis', section_key) as varchar) as breakdown_json,
        cast(json_object('legal_basis', max(section_title)) as varchar) as breakdown_label_json,
        count(*) as entries,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from newest
    group by snapshot_date, section_key
    union all
    select
        'prosecutor_level' as breakdown_code,
        snapshot_date,
        cast(json_object('prosecutor_level', prosecutor_level) as varchar) as breakdown_json,
        cast(null as varchar) as breakdown_label_json,
        count(*) as entries,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from newest
    group by snapshot_date, prosecutor_level
    union all
    select
        'identifier_kind' as breakdown_code,
        snapshot_date,
        cast(json_object('identifier_kind', identifier_kind) as varchar) as breakdown_json,
        cast(null as varchar) as breakdown_label_json,
        count(*) as entries,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from newest
    group by snapshot_date, identifier_kind
    union all
    select
        'final_ruling_mentioned' as breakdown_code,
        snapshot_date,
        '{}' as breakdown_json,
        cast(null as varchar) as breakdown_label_json,
        count(*) as entries,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from newest
    where final_ruling_mentioned
    group by snapshot_date
)

select
    'knf_warnings' as source_system,
    'warning_list' as block_id,
    'KNF public warning list' as block_name,
    'knf_warnings:warning_list:entries:' || c.breakdown_code as measure_key,
    case c.breakdown_code
        when 'total' then 'Entries on the KNF public warning list'
        when 'legal_basis' then 'Entries on the KNF public warning list, by legal basis of the notification'
        when 'prosecutor_level' then 'Entries on the KNF public warning list, by level of the prosecutor''s office'
        when 'identifier_kind' then 'Entries on the KNF public warning list, by kind of identifier listed'
        else 'Entries on the KNF public warning list mentioning a final ruling'
    end as measure_name,
    c.breakdown_code as measure_codes,
    'list_entries' as unit_code,
    'list entries' as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'D' as frequency,
    c.snapshot_date as date_key,
    cast(c.snapshot_date as varchar) as period_label,
    c.breakdown_json,
    c.breakdown_label_json,
    cast(null as varchar) as sex_code,
    c.breakdown_code = 'total' as is_total,
    cast(c.entries as double) as value,
    'ok' as observation_status_key,
    c.source_version,
    c.raw_sha256
from counted as c
left join geo_map as g on g.source_code = 'PL'
