{# GUGiK BDOT10k, topographic objects database (register PL-ENV-003): the catalogue of the powiat download packages,
   one row per package of the newest copy of the WFS index PobieranieBDOT10k / BDOT10k_powiaty
   (bronze.bdot10k_packages). Grain: package_id, the powiat TERYT code the index gives (4 digits; the package file is
   <package_id>_GML.zip). Powiat name as published, voivodeship code (first 2 digits), update date
   (Data_aktualizacji, published as 'YYYY-MM-DD 00:00:00'), its year, the GML package URL and the application schema
   year the URL names (schemat2021). snapshot_date is the date the index copy was read (Landing ingested_at): the
   date of the Gold counts. Guards that fail the build: a TERYT that is not 4 digits, an update date that is not a
   date, a URL that is not opendata.geoportal.gov.pl/bdot10k/schemat<year>/<voivodeship>/<TERYT>_GML.zip of the same
   TERYT. #}
{% set url_pattern = '^https://opendata\\.geoportal\\.gov\\.pl/bdot10k/'
    ~ 'schemat([0-9]{4})/([0-9]{2})/([0-9]{4})_GML\\.zip$' %}
with newest as (
    select *
    from {{ source('gugik_bdot10k', 'bdot10k_packages') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

cleaned as (
    select
        case
            when regexp_full_match(trim(n.teryt), '[0-9]{4}') then trim(n.teryt)
            else error('bdot10k_powiat_packages: TERYT is not 4 digits: ' || coalesce(n.teryt, 'null'))
        end as package_id,
        nullif(trim(n.nazwa_powiatu), '') as powiat_name,
        case
            when regexp_full_match(trim(n.data_aktualizacji), '[0-9]{4}-[0-9]{2}-[0-9]{2}( 00:00:00)?')
                then cast(left(trim(n.data_aktualizacji), 10) as date)
            else error('bdot10k_powiat_packages: update date is not a date: ' || coalesce(n.data_aktualizacji, 'null'))
        end as update_date,
        trim(n.url_gml) as url_gml,
        n.feature_number,
        n.source_version,
        n.raw_sha256,
        n.ingested_at
    from newest as n
)

select
    c.package_id,
    left(c.package_id, 2) as voivodeship_code,
    c.powiat_name,
    c.update_date,
    year(c.update_date) as update_year,
    c.url_gml,
    case
        when
            regexp_extract(c.url_gml, '{{ url_pattern }}', 2) = left(c.package_id, 2)
            and regexp_extract(c.url_gml, '{{ url_pattern }}', 3) = c.package_id
            then regexp_extract(c.url_gml, '{{ url_pattern }}', 1)
        else error(
            'bdot10k_powiat_packages: URL does not name package ' || c.package_id || ': ' || coalesce(c.url_gml, 'null')
        )
    end as application_schema,
    cast(timezone('UTC', c.ingested_at) as date) as snapshot_date,
    c.feature_number,
    c.source_version,
    c.raw_sha256
from cleaned as c
