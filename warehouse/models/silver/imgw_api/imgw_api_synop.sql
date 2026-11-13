{# IMGW public data API, synoptic stations (/api/data/synop), from every landed snapshot (bronze.imgw_api_synop).
   Grain: station_id x observed_at (one row per station and observation hour). The API serves only the newest
   hourly observation of each station, so a snapshot is a version of that observation: the newest snapshot
   (ingested_at) of each station and observation time is kept, never another row. observed_at = data_pomiaru +
   godzina_pomiaru hours in UTC (the API names no zone; on 13 Nov 2026 at 00:26 UTC it showed hour 0 of 13 Nov,
   Dev probe 43336937240). Values typed as published; a JSON null stays null (never 0); a value that is not a number
   fails the build. Provisional current observations: the verified history is imgw_archive. #}
{% set measures = {
    'temperatura': 'temperature_c',
    'predkosc_wiatru': 'wind_speed_ms',
    'kierunek_wiatru': 'wind_direction_deg',
    'wilgotnosc_wzgledna': 'relative_humidity_pct',
    'suma_opadu': 'precipitation_mm',
    'cisnienie': 'pressure_hpa'
} %}
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
    {%- for field, name in measures.items() %}
    case
        when {{ field }} is null then null
        when try_cast({{ field }} as double) is not null then cast({{ field }} as double)
        else error('imgw_api_synop: {{ field }} is not a number: ' || {{ field }})
    end as {{ name }},
    {%- endfor %}
    raw_sha256,
    source_version,
    ingested_at
from snapshots
