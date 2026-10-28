{{ config(alias='dim_source') }}
{# Gold PoC: the platform's sources, one row each, from the seed warehouse/seeds/source.csv. #}
select
    source_key as source_system,
    name_pl,
    name_en,
    publisher,
    family,
    block_kind,
    register_id,
    provider_type,
    licence_key,
    url
from {{ ref('source') }}
