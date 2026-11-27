{# dane.gov.pl open-data catalogue via data.europa.eu (register PLG-030, #3263): the distributions (resources) of
   each dataset of silver.dane_gov_pl_datasets (the record kept there), counted by the month of their own last
   modification and their format, from bronze.dane_gov_pl_distributions. Grain: dataset_number x modified_month x
   format_code. One row per distribution would be about 670,000 rows growing by about 45,000 a month (developers'
   daily price lists), past the 1 M full-rebuild ceiling within a year (#822, D28); the rows stay in Bronze.
   format_code = the format's id, else the end of its EU file-type URI, '_U' where the record names none.
   Distributions without a modification time are not in this table (they are in distribution_count of the dataset).
   bytes = the sum of the published byte sizes; sized = how many of the distributions publish one. #}
with kept as (
    select
        dataset_number,
        dataset_id,
        raw_sha256
    from {{ ref('dane_gov_pl_datasets') }}
),

rows as (
    select
        k.dataset_number,
        cast(date_trunc('month', timezone('UTC', cast(b.modified as timestamptz))) as date) as modified_month,
        coalesce(
            nullif(b.format, ''),
            nullif(regexp_extract(b.format_resource, '/file-type/([A-Za-z0-9_]+)$', 1), ''),
            '_U'
        ) as format_code,
        cast(b.modified as timestamptz) as modified_at,
        try_cast(b.byte_size as bigint) as byte_size,
        k.raw_sha256
    from kept as k
    inner join {{ source('dane_gov_pl', 'dane_gov_pl_distributions') }} as b
        on k.dataset_id = b.dataset_id and k.raw_sha256 = b.raw_sha256
    where b.modified is not null
)

select
    dataset_number,
    modified_month,
    format_code,
    count(*) as distributions,
    count(byte_size) as sized,
    sum(byte_size) as bytes,
    max(modified_at) as newest_modified_at,
    raw_sha256
from rows
group by all
