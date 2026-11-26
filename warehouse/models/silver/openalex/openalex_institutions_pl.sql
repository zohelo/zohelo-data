{# OpenAlex (register CD-028, #2996): the institutions OpenAlex places in Poland (filter country_code:PL), from the
   newest copy of entity institutions_pl (bronze.openalex_institutions). Entity table keyed by the OpenAlex id
   ('I' + digits, the URI's last part; institution_uri keeps the URI as published), with the ROR id as published,
   name, OpenAlex institution type, geo as published (city; region: an English region name such as 'Mazovia', no
   code, so no voivodeship is derived), coordinates, the all-time works and citation counts at the read, parent institutions (lineage without
   the institution itself, JSON list of ids) and OpenAlex's updated date. Organisations only; no person is named.
   Grain: institution_id. #}
with records as (
    select *
    from {{ source('openalex', 'openalex_institutions') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    case
        when regexp_full_match(institution_id, 'https://openalex\.org/I[0-9]+')
            then replace(institution_id, 'https://openalex.org/', '')
        else error('openalex: institution id ' || coalesce(institution_id, 'null'))
    end as institution_id,
    institution_id as institution_uri,
    ror,
    display_name,
    institution_type,
    country_code,
    geo_city,
    nullif(geo_region, '') as geo_region,
    cast(latitude as double) as latitude,
    cast(longitude as double) as longitude,
    cast(works_count as bigint) as works_count,
    cast(cited_by_count as bigint) as cited_by_count,
    cast(to_json(list_filter(
        list_transform(from_json(lineage_json, '["VARCHAR"]'), lambda x: replace(x, 'https://openalex.org/', '')),
        lambda x: x <> replace(institution_id, 'https://openalex.org/', '')
    )) as varchar) as parent_ids_json,
    cast(updated_date as timestamp) as updated_at,
    raw_sha256,
    source_version
from records
