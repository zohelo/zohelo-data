{{ config(alias='build_ms_isws', tags=['gold_block']) }}
{# Gold build stage: Ministry of Justice ISWS multi-year workbooks (register PLG-031), annual observations of
   silver.ms_isws_tables, Poland only. Block 'multi_year'. measure_key = ms_isws:<table>:<unit>:<measure> (frequency
   A only); the case type (caseload) and the court level (duration) are breakdowns, the row "ogółem" is the total.
   Values and units as published (D33): cases, months, notarial acts. Rows without a value ('•') are not loaded. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'ms_isws'
)

select
    'ms_isws' as source_system,
    'multi_year' as block_id,
    'ISWS multi-year tables (courts, adoptions, notarial acts)' as block_name,
    'ms_isws:' || s.table_code || ':' || s.unit_code || ':' || s.measure_code as measure_key,
    case (s.table_code || ':' || s.measure_code)
        when 'caseload:incoming' then 'Cases received by the common courts (ISWS)'
        when 'caseload:resolved' then 'Cases resolved by the common courts (ISWS)'
        when 'caseload:pending' then 'Cases pending in the common courts at the end of the year (ISWS)'
        when 'duration:avg_duration' then 'Average duration of court proceedings, selected case categories (ISWS)'
        when 'adoptions:incoming' then 'Adoption cases received by the family courts (ISWS)'
        when 'adoptions:resolved' then 'Adoption cases resolved by the family courts (ISWS)'
        when 'adoptions:granted' then 'Adoptions granted by the family courts, cases (ISWS)'
        when 'adoptions:pending' then 'Adoption cases pending at the end of the year (ISWS)'
        when 'notarial_acts:acts' then 'Notarial acts drawn up (ISWS)'
    end as measure_name,
    s.measure_code as measure_codes,
    s.unit_code,
    case s.unit_code when 'cases' then 'cases' when 'months' then 'months' when 'acts' then 'notarial acts' end
        as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(s.period_year, 1, 1) as date_key,
    cast(s.period_year as varchar) as period_label,
    case
        when s.breakdown_code = 'total' then '{}'
        when s.table_code = 'duration' then '{"court_level":"' || s.breakdown_code || '"}'
        else '{"case_type":"' || s.breakdown_code || '"}'
    end as breakdown_json,
    case s.breakdown_code
        when 'total' then cast(null as varchar)
        when 'criminal' then '{"case_type":"criminal cases"}'
        when 'civil' then '{"case_type":"civil cases"}'
        when 'family' then '{"case_type":"family law cases"}'
        when 'labour' then '{"case_type":"labour law cases"}'
        when 'social_insurance' then '{"case_type":"social insurance cases"}'
        when 'commercial' then '{"case_type":"commercial cases"}'
        when 'regional' then '{"court_level":"regional courts"}'
        when 'district' then '{"court_level":"district courts"}'
    end as breakdown_label_json,
    cast(null as varchar) as sex_code,
    s.breakdown_code = 'total' as is_total,
    s.value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from {{ ref('ms_isws_tables') }} as s
left join geo as g on g.source_code = 'PL'
where s.value is not null
