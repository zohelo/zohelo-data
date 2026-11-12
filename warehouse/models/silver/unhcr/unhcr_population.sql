{# UNHCR Refugee Statistics API, population endpoint (bronze.unhcr_population): people under UNHCR's mandate by
   country of asylum (coa), country of origin (coo) and year, the newest Landing copy of each selection (entity
   population_by_asylum: every country of asylum, origins summed, coo = '-'; poland_by_origin: Poland by every
   country of origin). Grain: entity × year × coa × coo (UNHCR's own codes; the ISO alpha-3 codes are kept beside
   them). Stocks at the end of the year, persons, as published. "-" (no figure for that population type) becomes
   null, never 0; any other text that is not a whole number fails the build (no silent zeros). A duplicated row
   fails the unique test. tests/unhcr_selection.sql checks each entity holds only its declared selection. #}
with newest as (
    select *
    from {{ source('unhcr', 'unhcr_population') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    entity,
    cast(year as integer) as year,
    trim(coa) as coa,
    trim(coa_iso) as coa_iso,
    trim(coa_name) as coa_name,
    trim(coo) as coo,
    trim(coo_iso) as coo_iso,
    trim(coo_name) as coo_name,
    case
        when trim(refugees) = '-' then null
        when regexp_full_match(trim(refugees), '[0-9]+') then cast(trim(refugees) as bigint)
        else error('unhcr_population: refugees is not a whole number: ' || coalesce(refugees, 'null'))
    end as refugees,
    case
        when trim(asylum_seekers) = '-' then null
        when regexp_full_match(trim(asylum_seekers), '[0-9]+') then cast(trim(asylum_seekers) as bigint)
        else error('unhcr_population: asylum_seekers is not a whole number: ' || coalesce(asylum_seekers, 'null'))
    end as asylum_seekers,
    case
        when trim(returned_refugees) = '-' then null
        when regexp_full_match(trim(returned_refugees), '[0-9]+') then cast(trim(returned_refugees) as bigint)
        else error('unhcr_population: returned_refugees is not a whole number: ' || coalesce(returned_refugees, 'null'))
    end as returned_refugees,
    case
        when trim(idps) = '-' then null
        when regexp_full_match(trim(idps), '[0-9]+') then cast(trim(idps) as bigint)
        else error('unhcr_population: idps is not a whole number: ' || coalesce(idps, 'null'))
    end as idps,
    case
        when trim(returned_idps) = '-' then null
        when regexp_full_match(trim(returned_idps), '[0-9]+') then cast(trim(returned_idps) as bigint)
        else error('unhcr_population: returned_idps is not a whole number: ' || coalesce(returned_idps, 'null'))
    end as returned_idps,
    case
        when trim(stateless) = '-' then null
        when regexp_full_match(trim(stateless), '[0-9]+') then cast(trim(stateless) as bigint)
        else error('unhcr_population: stateless is not a whole number: ' || coalesce(stateless, 'null'))
    end as stateless,
    case
        when trim(ooc) = '-' then null
        when regexp_full_match(trim(ooc), '[0-9]+') then cast(trim(ooc) as bigint)
        else error('unhcr_population: ooc is not a whole number: ' || coalesce(ooc, 'null'))
    end as ooc,
    case
        when trim(oip) = '-' then null
        when regexp_full_match(trim(oip), '[0-9]+') then cast(trim(oip) as bigint)
        else error('unhcr_population: oip is not a whole number: ' || coalesce(oip, 'null'))
    end as oip,
    case
        when trim(hst) = '-' then null
        when regexp_full_match(trim(hst), '[0-9]+') then cast(trim(hst) as bigint)
        else error('unhcr_population: hst is not a whole number: ' || coalesce(hst, 'null'))
    end as hst,
    raw_sha256,
    source_version,
    ingested_at,
    source_row_number
from newest
