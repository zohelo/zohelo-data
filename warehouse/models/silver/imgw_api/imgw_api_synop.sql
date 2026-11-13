{# IMGW public data API, synoptic stations (/api/data/synop), from every landed snapshot (bronze.imgw_api_synop).
   Grain: station_id x observed_at (one row per station and observation hour). The API serves only the newest
   hourly observation of each station, so a snapshot is a version of that observation: the newest snapshot
   (ingested_at) of each station and observation time is kept, never another row. observed_at = data_pomiaru +
   godzina_pomiaru hours in UTC (the API names no zone; on 13 Nov 2026 at 00:26 UTC it showed hour 0 of 13 Nov,
   Dev probe 43336937240). Values typed as published; a JSON null stays null (never 0); a value that is not a number
   fails the build. Provisional current observations: the verified history is imgw_archive. #}
with snapshots as (
    select
        *,
        cast(data_pomiaru as date) + to_hours(cast(godzina_pomiaru as integer)) as observed_at
    from {{ source('imgw_api', 'imgw_api_synop') }}
    qualify row_number() over (partition by id_stacji, observed_at order by ingested_at desc) = 1
)

select
    id_stacji as station_id,
    stacja as station_name,
    observed_at,
    case
        when temperatura is null then null
        when try_cast(temperatura as double) is not null then cast(temperatura as double)
        else error('imgw_api_synop: temperatura is not a number: ' || temperatura)
    end as temperature_c,
    case
        when predkosc_wiatru is null then null
        when try_cast(predkosc_wiatru as double) is not null then cast(predkosc_wiatru as double)
        else error('imgw_api_synop: predkosc_wiatru is not a number: ' || predkosc_wiatru)
    end as wind_speed_ms,
    case
        when kierunek_wiatru is null then null
        when try_cast(kierunek_wiatru as double) is not null then cast(kierunek_wiatru as double)
        else error('imgw_api_synop: kierunek_wiatru is not a number: ' || kierunek_wiatru)
    end as wind_direction_deg,
    case
        when wilgotnosc_wzgledna is null then null
        when try_cast(wilgotnosc_wzgledna as double) is not null then cast(wilgotnosc_wzgledna as double)
        else error('imgw_api_synop: wilgotnosc_wzgledna is not a number: ' || wilgotnosc_wzgledna)
    end as relative_humidity_pct,
    case
        when suma_opadu is null then null
        when try_cast(suma_opadu as double) is not null then cast(suma_opadu as double)
        else error('imgw_api_synop: suma_opadu is not a number: ' || suma_opadu)
    end as precipitation_mm,
    case
        when cisnienie is null then null
        when try_cast(cisnienie as double) is not null then cast(cisnienie as double)
        else error('imgw_api_synop: cisnienie is not a number: ' || cisnienie)
    end as pressure_hpa,
    raw_sha256,
    source_version,
    ingested_at
from snapshots
