{{ config(alias='build_dane_gov_pl', tags=['gold_block']) }}
{# Gold build stage: the dane.gov.pl open-data catalogue as harvested by data.europa.eu (register PLG-030, #3263),
   Poland, monthly (M). Block catalogue, one measure_key per count and breakdown (one frequency per measure_key):
   dane_gov_pl:catalogue:<datasets|distributions>:<breakdown>:monthly
   - datasets: datasets of silver.dane_gov_pl_datasets by the month of their last modification (modified);
   - distributions: distributions of silver.dane_gov_pl_distribution_months by the month of their own last
     modification (distributions without a modification time are not counted here);
   with <breakdown>
   - total: all (is_total);
   - publisher: by the dane.gov.pl institution number ({"publisher": "<n>"}, label the publisher's name; '_U' where
     the record links no institution);
   - category: by EU data theme ({"category": "<code>"}; '_U' for a dataset without one). A dataset in several
     themes counts in each, so the categories add up to more than the total.
   The counts describe the catalogue on the download date: a dataset modified again moves to the newer month, so the
   months before change with every harvest; the month of the download date is provisional. Only months with a count
   have a row (no zero filling). Catalogue metadata only: no figure of the datasets themselves. #}
with datasets as (
    select * from {{ ref('dane_gov_pl_datasets') }}
),

categories as (
    select * from {{ ref('dane_gov_pl_dataset_categories') }}
),

distributions as (
    select * from {{ ref('dane_gov_pl_distribution_months') }}
),

snapshot as (
    select
        cast(strptime(max(source_version), '%Y%m%dT%H%M%SZ') as date) as snapshot_date,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from datasets
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'dane_gov_pl'
),

publishers as (
    select
        dataset_number,
        coalesce(cast(publisher_number as varchar), '_U') as publisher_code
    from datasets
),

-- one name per institution number, the label of the breakdown: the name on its newest named dataset (records of
-- 367 datasets give the institution link without a name, Dev probe 45818878323)
publisher_names as (
    select
        coalesce(cast(publisher_number as varchar), '_U') as publisher_code,
        coalesce(
            arg_max(publisher_name, modified_at) filter (where publisher_name is not null), 'nie podano (not given)'
        ) as publisher_label
    from datasets
    group by 1
),

dataset_rows as (
    select
        d.dataset_number,
        cast(date_trunc('month', d.modified_on) as date) as date_key,
        cast(1 as bigint) as n
    from datasets as d
),

distribution_rows as (
    select
        dataset_number,
        modified_month as date_key,
        distributions as n
    from distributions
),

{% set counts = [('datasets', 'dataset_rows'), ('distributions', 'distribution_rows')] %}
counted as (
    {% for count_code, rows in counts %}
        {% if not loop.first %}union all{% endif %}
        select
            '{{ count_code }}' as count_code,
            'total' as breakdown_code,
            r.date_key,
            '{}' as breakdown_json,
            cast(null as varchar) as breakdown_label_json,
            sum(r.n) as value
        from {{ rows }} as r
        group by all
        union all
        select
            '{{ count_code }}' as count_code,
            'publisher' as breakdown_code,
            r.date_key,
            cast(json_object('publisher', p.publisher_code) as varchar) as breakdown_json,
            cast(json_object('publisher', pn.publisher_label) as varchar) as breakdown_label_json,
            sum(r.n) as value
        from {{ rows }} as r
        inner join publishers as p on r.dataset_number = p.dataset_number
        inner join publisher_names as pn on p.publisher_code = pn.publisher_code
        group by all
        union all
        select
            '{{ count_code }}' as count_code,
            'category' as breakdown_code,
            r.date_key,
            cast(json_object('category', coalesce(c.category_code, '_U')) as varchar) as breakdown_json,
            cast(json_object('category', coalesce(c.category_code, 'brak kategorii (no category)')) as varchar)
                as breakdown_label_json,
            sum(r.n) as value
        from {{ rows }} as r
        left join categories as c on r.dataset_number = c.dataset_number
        group by all
    {% endfor %}
)

select
    'dane_gov_pl' as source_system,
    'catalogue' as block_id,
    'dane.gov.pl open-data catalogue (datasets and distributions)' as block_name,
    'dane_gov_pl:catalogue:' || c.count_code || ':' || c.breakdown_code || ':monthly' as measure_key,
    case c.count_code
        when 'datasets' then 'Datasets on dane.gov.pl by month of last modification'
        else 'Distributions (resources) on dane.gov.pl by month of last modification'
    end
    || case c.breakdown_code when 'total' then '' when 'publisher' then ', by publisher' else ', by EU data theme' end
        as measure_name,
    c.count_code || ':' || c.breakdown_code || ':monthly' as measure_codes,
    c.count_code as unit_code,
    c.count_code as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'M' as frequency,
    c.date_key,
    strftime(c.date_key, '%Y-%m') as period_label,
    c.breakdown_json,
    c.breakdown_label_json,
    cast(null as varchar) as sex_code,
    c.breakdown_code = 'total' as is_total,
    cast(c.value as double) as value,
    case
        when c.date_key = cast(date_trunc('month', s.snapshot_date) as date) then 'provisional'
        else 'ok'
    end as observation_status_key,
    s.source_version,
    s.raw_sha256
from counted as c
cross join snapshot as s
left join geo_map as g on g.source_code = 'PL'
