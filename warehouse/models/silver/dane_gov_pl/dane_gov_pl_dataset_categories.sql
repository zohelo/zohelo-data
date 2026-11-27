{# dane.gov.pl open-data catalogue via data.europa.eu (register PLG-030, #3263): the EU data themes of each dataset of
   silver.dane_gov_pl_datasets (the record kept there), from the categories of its Bronze record. Grain:
   dataset_number x category_code. category_code = the theme's id, else the end of its URI
   (http://publications.europa.eu/resource/authority/data-theme/<code>), as published; a dataset without a theme has
   no row here. #}
with kept as (
    select
        dataset_number,
        dataset_id,
        raw_sha256
    from {{ ref('dane_gov_pl_datasets') }}
),

themes as (
    select
        k.dataset_number,
        unnest(cast(b.categories as json[])) as theme,
        k.raw_sha256
    from kept as k
    inner join {{ source('dane_gov_pl', 'dane_gov_pl_datasets') }} as b
        on k.dataset_id = b.dataset_id and k.raw_sha256 = b.raw_sha256
)

select
    dataset_number,
    nullif(coalesce(
        nullif(theme ->> '$.id', ''), regexp_extract(theme ->> '$.resource', '/data-theme/([A-Za-z0-9_]+)$', 1)
    ), '') as category_code,
    count(*) as listings,  -- a theme listed twice on one record is one theme of the dataset, counted here
    raw_sha256
from themes
group by all
