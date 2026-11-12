{# GUGiK PRNG, state register of geographical names (register PL-ENV-002), national part (bronze.prng_names): one row
   per named object of the newest copy of each package, entity miejscowosci (localities) or obiekty_fizjograficzne
   (physiographic objects). Grain: prng_id (the register's own identifier, one sequence over both parts; the INSPIRE
   id iip_id beside it). Names, name status, object category and kind, parent locality, SIMC identifier of localities
   (external_system 'TERYT (SIMC)'), the source document, the territory as published (voivodeship, powiat and gmina
   names and the gmina TERYT codes; several, separated by ';', for an object that spans units: the code pairs are in
   silver.prng_object_gminas) and the coordinates as published (WGS84 degrees-minutes-seconds and PL-1992 XY text),
   with latitude and longitude in decimal degrees parsed from the WGS84 text of a single main point. Empty text and
   the literal 'NULL' are null. Version and validity dates are Excel serial numbers in the workbook, converted here;
   a value that is not a number fails the build, as does a PRNG identifier that is not a whole number. #}
{# 50°43'05" 16°39'17" (latitude, then longitude); the apostrophe doubled for the SQL literal #}
{% set dms = "^([0-9]+)°([0-9]+)''([0-9]+(?:\\.[0-9]+)?)\" ([0-9]+)°([0-9]+)''([0-9]+(?:\\.[0-9]+)?)\"$" %}
with newest as (
    select *
    from {{ source('gugik_prng', 'prng_names') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

cleaned as (
    select
        n.entity,
        case
            when regexp_full_match(trim(n.identyfikator_prng), '[0-9]+')
                then cast(nullif(trim(n.identyfikator_prng), '') as bigint)
            else error('prng_objects: PRNG id is not a whole number: ' || coalesce(n.identyfikator_prng, 'null'))
        end as prng_id,
        nullif(nullif(trim(n.identyfikator_iip), ''), 'NULL') as iip_id,
        nullif(nullif(trim(n.nazwa_glowna), ''), 'NULL') as name,
        nullif(nullif(trim(n.element_rozrozniajacy), ''), 'NULL') as name_specific,
        nullif(nullif(trim(n.element_rodzajowy), ''), 'NULL') as name_generic,
        nullif(nullif(trim(n.dopelniacz), ''), 'NULL') as name_genitive,
        nullif(nullif(trim(n.przymiotnik), ''), 'NULL') as name_adjective,
        nullif(nullif(trim(n.status_nazwy), ''), 'NULL') as name_status,
        nullif(nullif(trim(n.kategoria_obiektu), ''), 'NULL') as object_category,
        nullif(nullif(trim(n.rodzaj_obiektu), ''), 'NULL') as object_kind,
        nullif(nullif(trim(n.nazwa_miejscowosci_nadrzednej), ''), 'NULL') as parent_locality_name,
        nullif(nullif(trim(n.identyfikator_miejscowosci_nadrzednej), ''), 'NULL') as parent_locality_prng_id,
        nullif(nullif(trim(n.informacje_dodatkowe), ''), 'NULL') as additional_info,
        nullif(nullif(trim(n.system_zewnetrzny), ''), 'NULL') as external_system,
        nullif(nullif(trim(n.identyfikator_zewnetrzny), ''), 'NULL') as external_id,
        nullif(nullif(trim(n.tytul_dokumentu_zrodlowego), ''), 'NULL') as source_document_title,
        nullif(nullif(trim(n.data_dokumentu_zrodlowego), ''), 'NULL') as source_document_date,
        nullif(nullif(trim(n.wydawca_dokumentu_zrodlowego), ''), 'NULL') as source_document_publisher,
        nullif(nullif(trim(n.rodzaj_reprezentacji), ''), 'NULL') as representation_kind,
        nullif(nullif(trim(n.wspolrzedne_geograficzne), ''), 'NULL') as coordinates_wgs84,
        nullif(nullif(trim(n.wspolrzedne_prostokatne_xy), ''), 'NULL') as coordinates_pl1992_xy,
        nullif(nullif(trim(n.nazwa_historyczna), ''), 'NULL') as historical_name,
        nullif(nullif(trim(n.nazwa_oboczna), ''), 'NULL') as variant_name,
        nullif(nullif(trim(n.nazwa_dodatkowa), ''), 'NULL') as additional_name,
        nullif(nullif(trim(n.jezyk_nazwy_dodatkowej), ''), 'NULL') as additional_name_language,
        nullif(nullif(trim(n.latynizacja_nazwy_dodatkowej), ''), 'NULL') as additional_name_latin,
        nullif(nullif(trim(n.endonim), ''), 'NULL') as endonym,
        nullif(nullif(trim(n.jezyk_endonimu), ''), 'NULL') as endonym_language,
        nullif(nullif(trim(n.latynizacja_endonimu), ''), 'NULL') as endonym_latin,
        nullif(nullif(trim(n.egzonim), ''), 'NULL') as exonym,
        nullif(nullif(trim(n.jezyk_egzonimu), ''), 'NULL') as exonym_language,
        nullif(nullif(trim(n.latynizacja_egzonimu), ''), 'NULL') as exonym_latin,
        nullif(nullif(trim(n.wojewodztwo), ''), 'NULL') as voivodeship_names,
        nullif(nullif(trim(n.powiat), ''), 'NULL') as powiat_names,
        nullif(nullif(trim(n.gmina), ''), 'NULL') as gmina_names,
        nullif(nullif(trim(n.identyfikator_gminy), ''), 'NULL') as gmina_codes,
        trim(n.poczatek_wersji_obiektu) as version_start_serial,
        trim(n.wersja_obiektu) as version_serial,
        trim(n.wazna_od) as valid_from_serial,
        cast(n.export_date as date) as export_date,
        n.source_row_number,
        n.source_version,
        n.raw_sha256
    from newest as n
),

parsed as (
    select
        c.*,
        regexp_extract(c.coordinates_wgs84, '{{ dms }}', 1) as lat_d,
        regexp_extract(c.coordinates_wgs84, '{{ dms }}', 2) as lat_m,
        regexp_extract(c.coordinates_wgs84, '{{ dms }}', 3) as lat_s,
        regexp_extract(c.coordinates_wgs84, '{{ dms }}', 4) as lon_d,
        regexp_extract(c.coordinates_wgs84, '{{ dms }}', 5) as lon_m,
        regexp_extract(c.coordinates_wgs84, '{{ dms }}', 6) as lon_s
    from cleaned as c
)

select
    p.entity,
    p.prng_id,
    p.iip_id,
    p.name,
    p.name_specific,
    p.name_generic,
    p.name_genitive,
    p.name_adjective,
    p.name_status,
    p.object_category,
    p.object_kind,
    p.parent_locality_name,
    p.parent_locality_prng_id,
    p.additional_info,
    p.external_system,
    p.external_id,
    p.source_document_title,
    p.source_document_date,
    p.source_document_publisher,
    p.representation_kind,
    p.coordinates_wgs84,
    p.coordinates_pl1992_xy,
    p.historical_name,
    p.variant_name,
    p.additional_name,
    p.additional_name_language,
    p.additional_name_latin,
    p.endonym,
    p.endonym_language,
    p.endonym_latin,
    p.exonym,
    p.exonym_language,
    p.exonym_latin,
    p.voivodeship_names,
    p.powiat_names,
    p.gmina_names,
    p.gmina_codes,
    case when p.external_system = 'TERYT (SIMC)' then p.external_id end as simc_id,
    case
        when p.gmina_codes is null then 0
        else len(string_split(p.gmina_codes, ';'))
    end as gmina_count,
    case
        when p.representation_kind = 'punkt główny' and p.lat_d <> ''
            then round(
                cast(nullif(p.lat_d, '') as double)
                + cast(nullif(p.lat_m, '') as double) / 60
                + cast(nullif(p.lat_s, '') as double) / 3600,
                6
            )
    end as latitude,
    case
        when p.representation_kind = 'punkt główny' and p.lon_d <> ''
            then round(
                cast(nullif(p.lon_d, '') as double)
                + cast(nullif(p.lon_m, '') as double) / 60
                + cast(nullif(p.lon_s, '') as double) / 3600,
                6
            )
    end as longitude,
    case
        when p.version_start_serial is null or p.version_start_serial = '' then null
        when regexp_full_match(p.version_start_serial, '[0-9]+(\.[0-9]+)?')
            then
                timestamp '1899-12-30 00:00:00'
                + to_seconds(cast(round(cast(nullif(p.version_start_serial, '') as double) * 86400) as bigint))
        else error('prng_objects: początek wersji obiektu is not an Excel serial number: ' || p.version_start_serial)
    end as version_start_at,
    case
        when p.version_serial is null or p.version_serial = '' then null
        when regexp_full_match(p.version_serial, '[0-9]+(\.[0-9]+)?')
            then
                timestamp '1899-12-30 00:00:00'
                + to_seconds(cast(round(cast(nullif(p.version_serial, '') as double) * 86400) as bigint))
        else error('prng_objects: wersja obiektu is not an Excel serial number: ' || p.version_serial)
    end as version_at,
    case
        when p.valid_from_serial is null or p.valid_from_serial = '' then null
        when regexp_full_match(p.valid_from_serial, '[0-9]+(\.0+)?')
            then cast(
                date '1899-12-30' + cast(cast(nullif(p.valid_from_serial, '') as double) as integer) as date
            )
        else error('prng_objects: ważna od is not a whole Excel serial day: ' || p.valid_from_serial)
    end as valid_from,
    p.export_date,
    p.source_row_number,
    p.source_version,
    p.raw_sha256
from parsed as p
