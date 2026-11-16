-- Silver: Companies House Free Company Data Product — companies on the UK register per snapshot, counted by
-- country of origin (as published, trimmed; blank → null), company category and company status. Grain: one row
-- per snapshot_date × country_of_origin × company_category × company_status; entity = the snapshot date, one
-- file per snapshot, kept (the monthly history). reference_month = the month the snapshot closes: the month
-- before the month of the file's date ("compiled up to the end of the previous month": the file dated
-- 1 November holds incorporations up to 31 October), whatever the day in the file name.
-- incorporated_in_reference_month counts the companies on the register at the snapshot that were incorporated
-- in that month: companies incorporated and already dissolved are not in the file, so it is a lower bound of
-- the month's incorporations, and no earlier month is counted (survivorship). Counts only where companies
-- exist: an absent combination has no row.
-- Applied by the loader to ONE Bronze file; `bronze` is that file (design #875).
with typed as (
    select
        cast(snapshot_date as date) as snapshot_date,
        nullif(trim(country_of_origin), '') as country_of_origin,
        company_category,
        company_status,
        cast(try_strptime(incorporation_date, '%d/%m/%Y') as date) as incorporation_date,
        raw_sha256,
        source_version,
        ingested_at
    from bronze
)

select
    cast(snapshot_date as varchar) as entity,
    snapshot_date,
    cast(date_trunc('month', snapshot_date) - interval 1 month as date) as reference_month,
    country_of_origin,
    company_category,
    company_status,
    count(*) as companies,
    count(*) filter (
        where date_trunc('month', incorporation_date) = date_trunc('month', snapshot_date) - interval 1 month
    ) as incorporated_in_reference_month,
    count(*) filter (where incorporation_date is null) as incorporation_date_missing,
    raw_sha256,
    source_version,
    ingested_at
from typed
group by all
