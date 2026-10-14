{# TERYT TERC (official variant): territorial units at the newest catalogue state. One row
   per unit: voivodeship (WOJ), powiat (WOJ+POW), gmina (WOJ+POW+GMI+RODZ). #}
with newest as (
    select * from {{ source('gus_teryt', 'teryt_records') }}
    where
        entity = 'TERC_Urzedowy'
        and source_version = (
            select max(source_version) from {{ source('gus_teryt', 'teryt_records') }}
            where entity = 'TERC_Urzedowy'
        )
)

select
    woj
    || coalesce(nullif(pow, ''), '')
    || coalesce(nullif(gmi, ''), '')
    || coalesce(nullif(rodz, ''), '') as terc_code,
    case
        when coalesce(pow, '') = '' then 'wojewodztwo'
        when coalesce(gmi, '') = '' then 'powiat'
        else 'gmina'
    end as unit_level,
    woj as woj_code,
    nullif(pow, '') as pow_code,
    nullif(gmi, '') as gmi_code,
    nullif(rodz, '') as gmina_type_code,
    nazwa as name_pl,
    nazwa_dod as unit_type_pl,
    cast(stan_na as date) as state_date,
    cast(strptime(source_version, '%Y%m%dT%H%M%SZ') as date) as catalogue_date,
    raw_sha256
from newest
