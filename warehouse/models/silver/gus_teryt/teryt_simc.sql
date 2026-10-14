{# TERYT SIMC (official variant): localities at the newest catalogue state, with their
   gmina (TERC) and parent locality. #}
with newest as (
    select * from {{ source('gus_teryt', 'teryt_records') }}
    where
        entity = 'SIMC_Urzedowy'
        and source_version = (
            select max(source_version) from {{ source('gus_teryt', 'teryt_records') }}
            where entity = 'SIMC_Urzedowy'
        )
)

select
    sym as simc_code,
    nullif(sympod, sym) as parent_simc_code,
    woj || pow || gmi || rodz_gmi as gmina_terc_code,
    rm as locality_type_code,
    mz = '1' as is_independent_name,
    nazwa as name_pl,
    cast(stan_na as date) as state_date,
    cast(strptime(source_version, '%Y%m%dT%H%M%SZ') as date) as catalogue_date,
    raw_sha256
from newest
