{{ config(alias='build_uzp_bzp', tags=['gold_block']) }}
{# Gold build stage (#2981): BZP contract and result notices (silver.uzp_bzp_notices) by publication month (UTC day
   of publication; complete months only: every day of the month has both notice types landed, silver.uzp_bzp_days,
   so the current month and a month with a day that failed Landing stay out). Notices as first published:
   corrections (change notices) are not loaded. Two measures:
   - uzp_bzp:notices:notices — notices published in the month (a count), breakdown notice_type (ContractNotice,
     TenderResultNotice, _T both);
   - uzp_bzp:notices:<currency>:contract_value — the sum of the contract values (8.2 lines, one per part with a
     concluded contract) that the result notices published in the month report, as published in their currency
     (PLN; another currency is its own measure, never converted, and only with a unit_map row); a notice whose
     value is absent, unparsed or in several currencies adds nothing (Silver keeps its line counts), so no month is filled with 0.
   Areas: Poland (every notice) and the voivodeship of the contracting authority (each notice counts once, in
   one voivodeship, so voivodeships add up to Poland except notices without a voivodeship); a code that maps to no
   voivodeship meets in '_X' with the code kept. #}
with notices as (
    select
        notice_number,
        notice_type,
        cast(date_trunc('month', publication_date) as date) as month_start,
        organization_province,
        contract_value,
        contract_value_currency,
        raw_sha256,
        source_version
    from {{ ref('uzp_bzp_notices') }}
),

landed_days as (
    -- days of which both types were landed (silver.uzp_bzp_days, also days without notices)
    select publication_date
    from {{ ref('uzp_bzp_days') }}
    group by publication_date
    having count(distinct notice_type) = 2
),

complete_months as (
    select cast(date_trunc('month', publication_date) as date) as month_start
    from landed_days
    group by cast(date_trunc('month', publication_date) as date)
    having count(*) = day(last_day(min(publication_date)))
),

complete as (
    select n.*
    from notices as n
    inner join complete_months as m on n.month_start = m.month_start
),

mapped_currencies as (
    -- a contract value enters Gold only in a currency with a unit_map row of this source (each its own measure);
    -- tests/uzp_bzp_currency_unmapped.sql lists the notices of any other currency
    select unit_code as currency
    from {{ ref('unit_map') }}
    where source_system = 'uzp_bzp'
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'uzp_bzp'
),

areas as (
    select
        c.*,
        'POL' as geo_code
    from complete as c
    union all
    select
        c.*,
        c.organization_province as geo_code
    from complete as c
    where c.organization_province is not null
),

located as (
    select
        a.*,
        coalesce(g.geography_key, '_X') as geography_key
    from areas as a
    left join geo_map as g on a.geo_code = g.source_code
),

{% set cuts = ['notice_type', "'_T'"] %}
counted as (
    {% for cut in cuts %}
        {% if not loop.first %}union all{% endif %}
        select
            month_start,
            geography_key,
            string_agg(distinct geo_code, ';' order by geo_code) as geo_code,
            {{ cut }} as notice_type,
            count(*) as notices,
            max(source_version) as source_version,
            sha256(string_agg(distinct raw_sha256, ',' order by raw_sha256)) as files_sha256
        from located
        group by month_start, geography_key{% if cut == 'notice_type' %}, notice_type{% endif %}
    {% endfor %}
),

valued as (
    select
        month_start,
        geography_key,
        string_agg(distinct geo_code, ';' order by geo_code) as geo_code,
        contract_value_currency as currency,
        sum(contract_value) as contract_value,
        max(source_version) as source_version,
        sha256(string_agg(distinct raw_sha256, ',' order by raw_sha256)) as files_sha256
    from located
    where
        notice_type = 'TenderResultNotice'
        and contract_value is not null
        and contract_value_currency in (select m.currency from mapped_currencies as m)
    group by month_start, geography_key, contract_value_currency
)

select
    'uzp_bzp' as source_system,
    'notices' as block_id,
    'BZP contract and result notices (monthly counts and contract values)' as block_name,
    'uzp_bzp:notices:notices' as measure_key,
    'BZP contract and result notices published in the month' as measure_name,
    'notices' as measure_codes,
    'notices' as unit_code,
    'notices' as unit_name,
    geo_code as geography_source_code,
    geography_key,
    'M' as frequency,
    month_start as date_key,
    strftime(month_start, '%Y-%m') as period_label,
    cast(json_object('notice_type', notice_type) as varchar) as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    notice_type = '_T' as is_total,
    cast(notices as double) as value,
    'ok' as observation_status_key,
    source_version,
    files_sha256 as raw_sha256
from counted
union all
select
    'uzp_bzp' as source_system,
    'notices' as block_id,
    'BZP contract and result notices (monthly counts and contract values)' as block_name,
    'uzp_bzp:notices:' || currency || ':contract_value' as measure_key,
    'Value of the contracts reported by BZP result notices published in the month' as measure_name,
    'contract_value' as measure_codes,
    currency as unit_code,
    currency as unit_name,
    geo_code as geography_source_code,
    geography_key,
    'M' as frequency,
    month_start as date_key,
    strftime(month_start, '%Y-%m') as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    contract_value as value,
    'ok' as observation_status_key,
    source_version,
    files_sha256 as raw_sha256
from valued
