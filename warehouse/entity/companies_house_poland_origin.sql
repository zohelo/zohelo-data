-- Silver: Companies House Free Company Data Product — the companies on the UK register whose country of origin
-- is Poland (the file's value 'POLAND', the only spelling in the data: no other variant is guessed), newest
-- snapshot only. Grain: one row per company number. Overseas companies with a UK establishment (numbers FC…, SF…)
-- and overseas entities owning UK property (OE…); a UK-incorporated company with Polish owners has country of
-- origin 'United Kingdom' and is not here. Registered office reduced to post town and country (both free text,
-- as published); SIC texts as published ('None Supplied' kept). Dates are DD/MM/YYYY in the file.
-- Applied by the loader to ONE Bronze file; `bronze` is that file (design #875).
select
    entity,
    company_number,
    company_name,
    company_category,
    company_status,
    trim(country_of_origin) as country_of_origin,
    cast(try_strptime(incorporation_date, '%d/%m/%Y') as date) as incorporation_date,
    sic_text_1,
    sic_text_2,
    sic_text_3,
    sic_text_4,
    post_town,
    address_country,
    cast(snapshot_date as date) as snapshot_date,
    raw_sha256,
    source_version,
    ingested_at
from bronze
where trim(country_of_origin) = 'POLAND'
