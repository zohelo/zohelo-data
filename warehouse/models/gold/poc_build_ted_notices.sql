{{ config(alias='build_ted_notices', tags=['gold_block']) }}
{# Gold build stage (#2976): TED notices of Polish buyers (silver.ted_notices), counted by publication month. One
   measure, ted_notices:notices:notices (notices, a count); breakdowns form_type (eForms form: planning,
   competition, result, change, ...) and notice_type (eForms notice type, e.g. cn-standard, can-standard), each
   broken down alone with the other at _T, codes as published. Areas: Poland (every notice: the query selects
   buyer country POL) and the voivodeship of each Polish buyer (BT-507 NUTS code reduced to NUTS 2; PL91 and PL92
   are Mazowieckie): a notice of several buyers counts once in each of their voivodeships, so voivodeships need
   not add up to Poland; extra-regio codes (PLZ...) count for Poland only. Only complete months: the month of the
   newest publication day is left out until a later day is published. Values are counts as published notices;
   contract values are not summed here (they stay in Silver as published, with their currency). #}
with notices as (
    select
        publication_number,
        form_type,
        notice_type,
        cast(date_trunc('month', publication_date) as date) as month_start,
        buyer_nuts,
        raw_sha256,
        source_version
    from {{ ref('ted_notices') }}
),

open_month as (
    select max(month_start) as month_start
    from notices
),

complete as (
    select n.*
    from notices as n
    cross join open_month as o
    where n.month_start < o.month_start
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'ted_notices'
),

-- one row per notice and area: Poland, and each distinct voivodeship of its Polish buyers; codes that match no
-- voivodeship (none in the trial) meet in one '_X' row per month with their codes listed
regions as (
    select distinct
        c.publication_number,
        case when u.code like 'PL9%' then 'PL9' else left(u.code, 4) end as geo_code
    from complete as c, unnest(c.buyer_nuts) as u (code)
    where u.code like 'PL%' and u.code not like 'PLZ%'
),

areas as (
    select
        publication_number,
        'POL' as geo_code
    from complete
    union all
    select
        publication_number,
        geo_code
    from regions
),

located as (
    select
        c.*,
        a.geo_code,
        coalesce(g.geography_key, '_X') as geography_key
    from complete as c
    inner join areas as a on c.publication_number = a.publication_number
    left join geo_map as g on a.geo_code = g.source_code
),

{% set cuts = [[], ['form_type'], ['notice_type']] %}
counted as (
    {% for cut in cuts %}
        {% if not loop.first %}union all{% endif %}
        select
            month_start,
            geography_key,
            string_agg(distinct geo_code, ';' order by geo_code) as geo_code,
            {{ 'form_type' if 'form_type' in cut else "'_T'" }} as form_type,
            {{ 'notice_type' if 'notice_type' in cut else "'_T'" }} as notice_type,
            count(distinct publication_number) as notices,
            max(source_version) as source_version,
            sha256(string_agg(distinct raw_sha256, ',' order by raw_sha256)) as files_sha256
        from located
        group by month_start, geography_key{% for d in cut %}, {{ d }}{% endfor %}
    {% endfor %}
)

select
    'ted_notices' as source_system,
    'notices' as block_id,
    'TED notices of Polish buyers (monthly counts)' as block_name,
    'ted_notices:notices:notices' as measure_key,
    'TED notices of Polish buyers published in the month' as measure_name,
    'notices' as measure_codes,
    'notices' as unit_code,
    'notices' as unit_name,
    geo_code as geography_source_code,
    geography_key,
    'M' as frequency,
    month_start as date_key,
    strftime(month_start, '%Y-%m') as period_label,
    cast(json_object('form_type', form_type, 'notice_type', notice_type) as varchar) as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    form_type = '_T' and notice_type = '_T' as is_total,
    cast(notices as double) as value,
    'ok' as observation_status_key,
    source_version,
    files_sha256 as raw_sha256
from counted
