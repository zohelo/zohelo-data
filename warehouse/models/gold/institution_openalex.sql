{{ config(alias='institution_openalex') }}
{# Gold entity table (reference source, playbook section 7): the institutions OpenAlex places in Poland (register
   CD-028, #2996), one row per OpenAlex institution id ('I' + digits), from silver.openalex_institutions_pl. The ROR
   id is OpenAlex's own link to the Research Organization Registry, kept as published; no join to other registers by
   name. Area: Poland (OpenAlex country_code PL); OpenAlex's region is an English name without a code, so no
   voivodeship is assigned. Works and citation counts are all-time at the read (per year: gold.build_openalex, block
   institutions_pl). Organisations only. #}
select
    institution_id,
    institution_uri,
    ror,
    display_name,
    institution_type,
    'PL' as geography_key,
    geo_city,
    geo_region,
    latitude,
    longitude,
    works_count,
    cited_by_count,
    parent_ids_json,
    updated_at,
    source_version,
    raw_sha256
from {{ ref('openalex_institutions_pl') }}
