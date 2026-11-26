{# GIOŚ groundwater quality monitoring (register PL-ENV-012, #2870): the physico-chemical results of the yearly result
   files of dane.gov.pl dataset 1405 (bronze.gios_groundwater_records, newest copy of each entity
   <year>_<inorganic|organic>_<diagnostic|operational>), one row per monitoring point x indicator x sampling date.
   point_key = the MONBADA point number as published; a row without one (from 2022, about 2 % of the samples) is keyed
   by its ID Monitoring number as 'IDM:<number>' (Bronze rejects a row with neither). Indicator = the column name as
   published (name and unit in brackets, e.g. 'Azotany [mgNO3/l]'); indicator_key folds case and white space, because
   the organic names are spelt differently between years ('Benzo[a]antracen', 'benzo[a] antracen'). Cell markers
   (Dev probe 45810873298): a number (stored number in XLSX, decimal comma in CSV and in text cells) → value,
   value_status 'measured'; '<' and a number (below the limit of quantification) → value NULL, below_limit true,
   limit_value the number, value_status 'below_limit' (never 0); 'n.o.' (not determined) is no result and is not
   loaded; any other text keeps value NULL and value_status 'not_numeric', which fails the accepted_values test.
   Sampling dates: ISO text in the CSV files, Excel serial numbers in the XLSX files; anything else fails the build.
   Grain: point_key x indicator_key x sample_date. #}
with records as (
    select *
    from {{ source('gios_groundwater', 'gios_groundwater_records') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

samples as (
    select
        entity,
        raw_sha256,
        sheet_row,
        coalesce(regexp_replace(point_monbada, '\.0$', ''), 'IDM:' || regexp_replace(point_id_monitoring, '\.0$', ''))
            as point_key,
        case
            when regexp_full_match(sample_date, '[0-9]{4}-[0-9]{2}-[0-9]{2}') then cast(sample_date as date)
            when regexp_full_match(sample_date, '[0-9]{5}(\.0+)?')
                then cast(date '1899-12-30' + to_days(cast(cast(sample_date as double) as integer)) as date)
            else error('gios_groundwater: sampling date ' || coalesce(sample_date, 'null') || ' in ' || entity)
        end as sample_date
    from records
    where record_kind = 'sample'
),

cells as (
    select
        v.entity,
        s.point_key,
        s.sample_date,
        v.indicator,
        trim(v.value) as value_text,
        v.raw_sha256,
        v.source_version
    from records as v
    inner join samples as s on v.entity = s.entity and v.raw_sha256 = s.raw_sha256 and v.sheet_row = s.sheet_row
    where v.record_kind = 'value' and trim(v.value) <> 'n.o.'
)

select
    point_key,
    lower(regexp_replace(indicator, '\s+', '', 'g')) as indicator_key,
    sample_date,
    cast(left(entity, 4) as integer) as survey_year,
    split_part(entity, '_', 2) as indicator_group,
    split_part(entity, '_', 3) as monitoring_kind,
    indicator,
    trim(regexp_replace(indicator, '\s*\[[^\]]*\]\s*$', '')) as indicator_name,
    nullif(regexp_extract(indicator, '\[([^\]]*)\]\s*$', 1), '') as unit_code,
    case
        when regexp_full_match(value_text, '-?[0-9]+([.,][0-9]+)?([eE][-+]?[0-9]+)?')
            then cast(replace(value_text, ',', '.') as double)
    end as value,
    regexp_full_match(value_text, '<\s*[0-9]+([.,][0-9]+)?') as below_limit,
    case
        when regexp_full_match(value_text, '<\s*[0-9]+([.,][0-9]+)?')
            then cast(replace(trim(substr(value_text, 2)), ',', '.') as double)
    end as limit_value,
    case
        when regexp_full_match(value_text, '-?[0-9]+([.,][0-9]+)?([eE][-+]?[0-9]+)?') then 'measured'
        when regexp_full_match(value_text, '<\s*[0-9]+([.,][0-9]+)?') then 'below_limit'
        else 'not_numeric'
    end as value_status,
    value_text,
    entity,
    raw_sha256,
    source_version
from cells
