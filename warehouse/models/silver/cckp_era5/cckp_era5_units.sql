{# CCKP ERA5 (register GL-036): CCKP's spatial units from geonames.json in the newest copy of entity era5
   (bronze.cckp_era5_records, record_kind 'unit'): countries and territories by ISO3 code (unit_level 'country') and
   their first-level units by CCKP code, e.g. POL.1982376 Mazowieckie (unit_level 'adm1', parent_geocode the country).
   Names as published, without diacritics and with CCKP's own spellings (POL.1982373 'Lubeiskie', POL.1982375
   'Malopolske'), so Gold maps voivodeships by a stated code list (macros/sources/cckp_era5.sql), never by name.
   Grain: geocode. #}
with records as (
    select *
    from {{ source('cckp_era5', 'cckp_era5_records') }}
    where record_kind = 'unit'
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    geocode,
    name,
    parent_geocode,
    case when parent_geocode is null then 'country' else 'adm1' end as unit_level,
    raw_sha256,
    source_version
from records
