{# GUS exam results (register PLS-025, #449): mean result of the eighth-grade exam (egzamin ósmoklasisty) in % of the
   maximum number of points, by area (Poland 'Ogółem' and the 16 voivodeships, names as printed), subject (Polish,
   mathematics and six modern foreign languages) and sex (ogółem, dziewczęta, chłopcy), annual from 2019, from the
   newest copy of entity eighth_grade_means (bronze.gus_exam_results_means, the GUS CSV republishing the OKE results).
   subject_code and sex_code are fixed translations of the published labels; a label not listed gives null and fails
   the not_null tests. value is the published number when the row has no GUS sign; a row with a sign keeps value null
   and its sign in flag, never 0 (GUS sends value '0' with both signs): '(.)' = no or unreliable data
   (observation_status 'missing'), '(–)' = did not occur, e.g. no pupil of the area took that language
   (observation_status 'nil'). A value that is not a number without a sign fails the build. Dev probe 45894428046:
   2,856 rows, 139 '(.)' and 370 '(–)'.
   Grain: area_name x subject x sex x period_year. #}
with records as (
    select *
    from {{ source('gus_exam_results', 'gus_exam_results_means') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

typed as (
    select
        *,
        nullif(trim(flag), '') as flag_text,
        trim(value) as value_trimmed
    from records
)

select
    entity,
    variable_name,
    country as country_name,
    area as area_name,
    subject,
    case subject
        when 'Język polski' then 'polish'
        when 'Matematyka' then 'mathematics'
        when 'Język angielski' then 'english'
        when 'Język niemiecki' then 'german'
        when 'Język francuski' then 'french'
        when 'Język hiszpański' then 'spanish'
        when 'Język rosyjski' then 'russian'
        when 'Język włoski' then 'italian'
    end as subject_code,
    sex,
    case sex when 'ogółem' then 'T' when 'dziewczęta' then 'F' when 'chłopcy' then 'M' end as sex_code,
    unit_label,
    case unit_label when 'relacja [%]' then 'pct_max_points' end as unit_code,
    case when regexp_full_match(period, '(19|20)[0-9]{2}') then cast(period as integer) end as period_year,
    case
        when flag_text is not null then null
        when regexp_full_match(value_trimmed, '-?[0-9]+([.,][0-9]+)?')
            then cast(replace(value_trimmed, ',', '.') as double)
        else error('gus_exam_results_eighth_grade: value without a sign is not a number: ' || coalesce(value, 'null'))
    end as value,
    value as value_text,
    flag_text as flag,
    case
        when flag_text is null then 'ok'
        when flag_text = '(.)' then 'missing'
        when flag_text = '(–)' then 'nil'
    end as observation_status,
    raw_sha256,
    source_version,
    ingested_at
from typed
