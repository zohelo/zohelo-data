{# EEA greenhouse gas inventory (GovReg: national emissions reported to the UNFCCC and to the EU under the
   Governance Regulation), newest Landing copy (one version of the dataset, bronze.eea_ghg_inventory). One row per
   country × reporting format × pollutant × sector × year, typed: emissions as a number in the published unit (Gg,
   Gg CO2 equivalent), the notation key as published (NO, NE, IE, NA, C, ...; a row with a notation and no value
   stays null, never 0), sector codes and the parent sector as published. A value that is not a number fails the
   build; a duplicated row fails the unique test. #}
with newest as (
    select *
    from {{ source('eea', 'eea_ghg_inventory') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    entity,
    country_code,
    country_code_3,
    country,
    format_name,
    pollutant_name,
    sector_code,
    sector_name,
    parent_sector_code,
    unit,
    cast(year as integer) as year,
    case
        when nullif(trim(emissions), '') is null then null
        when try_cast(trim(emissions) as double) is not null then cast(trim(emissions) as double)
        else error('eea_ghg_inventory: emissions is not a number: ' || emissions)
    end as value,
    nullif(trim(notation), '') as notation,
    publicationdate as publication_date,
    datasource as data_source,
    raw_sha256,
    source_version,
    ingested_at,
    source_row_number
from newest
