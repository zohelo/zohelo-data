{# e-Zamówienia BZP contract and result notices (read API v1, bronze.uzp_bzp_notices): one row per published notice,
   keyed by its BZP notice number (<year>/BZP <number>/<version>), from the newest Landing copy of each (notice type,
   UTC publication day) entity. Codes as published: notice type, client type (SL.MO.013), order type (ENUM.002),
   tender type (ENUM.017/018/019), voivodeship of the contracting authority (SL.MT.007, 'PL02'...'PL32' = 'PL' + the
   TERYT voivodeship code). Contracting authorities are institutions; Landing already dropped the authority's name,
   city and ids of a subsidised authority (client type 3) and every contractor's name, city and id
   (ingestion/to_landing/uzp_bzp/extract.py). Amounts come from the form's value lines (htmlBody, kept in Landing as
   `values`): "<amount> <currency>" with a decimal comma, parsed to a number; a line that does not have that shape
   stays unparsed (counted, never 0). contract_value = sum of the "8.2.) Wartość umowy/umowy ramowej" lines of a
   result notice (one per part with a concluded contract) when all are parsed and share one currency, else null with
   the counts kept; estimated_value = the one "Wartość zamówienia" / "Łączna wartość poszczególnych części" line of
   the notice (4.1.5 or 4.3), null when there is none or more than one. Values as published (the form does not say
   gross or net), never converted. A notice found in two files fails the unique test. #}
with newest as (
    select *
    from {{ source('uzp_bzp', 'uzp_bzp_notices') }}
    qualify rank() over (partition by entity order by ingested_at desc) = 1
),

unnested as (
    select
        n.notice_number,
        unnest(from_json(n.form_values, '[{"field":"VARCHAR","part":"BIGINT","value":"VARCHAR"}]')) as line
    from newest as n
),

lines as (
    select
        u.notice_number,
        struct_extract(u.line, 'field') as field,
        struct_extract(u.line, 'part') as part,
        struct_extract(u.line, 'value') as value_text,
        regexp_extract(
            struct_extract(u.line, 'value'),
            '^\s*([0-9][0-9 \x{00A0}]*(?:[.,][0-9]{1,2})?)\s*([A-Z]{3})\s*$',
            ['amount', 'currency']
        ) as parsed
    from unnested as u
),

amounts as (
    select
        l.notice_number,
        l.field,
        l.part,
        l.value_text,
        try_cast(
            replace(regexp_replace(struct_extract(l.parsed, 'amount'), '[ \x{00A0}]', '', 'g'), ',', '.') as double
        ) as amount,
        nullif(struct_extract(l.parsed, 'currency'), '') as currency
    from lines as l
    where l.field in ('contract_value', 'estimated_value')
),

contractor_lists as (
    select
        notice_number,
        len(from_json(contractors, '["JSON"]')) as contractors,
        list_sort(list_distinct(list_filter(
            list_transform(
                from_json(contractors, '[{"contractorProvince":"VARCHAR","contractorCountry":"VARCHAR"}]'),
                lambda c: struct_extract(c, 'contractorProvince')
            ),
            lambda province: province is not null
        ))) as contractor_provinces
    from newest
),

per_notice as (
    select
        notice_number,
        count(*) filter (where field = 'contract_value') as contract_value_lines,
        count(*) filter (where field = 'contract_value' and amount is null) as contract_value_unparsed,
        count(distinct currency) filter (where field = 'contract_value') as contract_value_currencies,
        sum(amount) filter (where field = 'contract_value') as contract_value_sum,
        min(currency) filter (where field = 'contract_value') as contract_value_currency_one,
        count(*) filter (where field = 'estimated_value') as estimated_value_lines,
        min(amount) filter (where field = 'estimated_value') as estimated_value_one,
        min(currency) filter (where field = 'estimated_value') as estimated_value_currency_one
    from amounts
    group by notice_number
),

outcomes as (
    select
        notice_number,
        count(*) filter (where field = 'part_outcome' and value_text ilike '%zawarciem umowy%') as parts_concluded,
        count(*) filter (where field = 'part_outcome' and value_text ilike '%unieważn%') as parts_cancelled
    from lines
    group by notice_number
)

select
    n.notice_number,
    n.bzp_number,
    n.object_id,
    n.notice_type,
    strptime(left(n.publication_date, 19), '%Y-%m-%dT%H:%M:%S') as publication_ts_utc,
    cast(left(n.publication_date, 10) as date) as publication_date,
    n.tender_id,
    n.client_type,
    n.order_type,
    n.tender_type,
    cast(n.is_tender_amount_below_eu as boolean) as is_below_eu_threshold,
    n.order_object,
    regexp_extract_all(n.cpv_code, '([0-9]{8}-[0-9])', 1) as cpv_codes,
    strptime(left(n.submitting_offers_date, 19), '%Y-%m-%dT%H:%M:%S') as submitting_offers_ts_utc,
    n.procedure_result,
    n.organization_id,
    n.organization_national_id,
    n.organization_name,
    n.organization_city,
    n.organization_province,
    n.organization_country,
    c.contractors,
    c.contractor_provinces,
    coalesce(o.parts_concluded, 0) as parts_concluded,
    coalesce(o.parts_cancelled, 0) as parts_cancelled,
    coalesce(p.contract_value_lines, 0) as contract_value_lines,
    coalesce(p.contract_value_unparsed, 0) as contract_value_unparsed,
    case
        when p.contract_value_lines > 0 and p.contract_value_unparsed = 0 and p.contract_value_currencies = 1
            then p.contract_value_sum
    end as contract_value,
    case
        when p.contract_value_lines > 0 and p.contract_value_unparsed = 0 and p.contract_value_currencies = 1
            then p.contract_value_currency_one
    end as contract_value_currency,
    coalesce(p.estimated_value_lines, 0) as estimated_value_lines,
    case when p.estimated_value_lines = 1 then p.estimated_value_one end as estimated_value,
    case when p.estimated_value_lines = 1 then p.estimated_value_currency_one end as estimated_value_currency,
    n.stats_count,
    n.entity,
    n.raw_sha256,
    n.source_version,
    n.ingested_at,
    n.source_row_number
from newest as n
left join per_notice as p on n.notice_number = p.notice_number
left join outcomes as o on n.notice_number = o.notice_number
left join contractor_lists as c on n.notice_number = c.notice_number
