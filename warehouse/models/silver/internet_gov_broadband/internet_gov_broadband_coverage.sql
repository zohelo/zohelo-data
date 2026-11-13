{# internet.gov.pl broadband, "Polska w zasięgu stacjonarnego dostępu do internetu" (register PL-ENV-034): the
   national report of each quarter, newest copy of each edition (bronze.internet_gov_broadband_cells). Grain:
   reference_date x table_code x area_code x class_code.
   - territory (the report's table "Liczba punktów adresowych w zasięgu na terenie całego kraju"): the 16
     voivodeships (printed names mapped to TERC codes, macro poc_internet_gov_broadband_voivodeships) and Poland
     (its SUMA row), class '_T': coverages, address points in coverage, all address points; no share printed.
   - bandwidth ("... w podziale na przepustowość"): Poland by bandwidth class in Mb/s as printed (2 ... 10000) and
     the SUMA row as '_T': coverages and address points in coverage, each with its printed share (coverages: of all
     coverages; address points: of the address points in coverage).
   - service_type ("... w podziale na rodzaj usług"): Poland by kind of coverage (actual, wholesale only,
     theoretical only, none) and SUMA as '_T': address points with the printed share of all address points.
   One Bronze cell per row label and column (Bronze rejects a table with a repeated row label), so the pivot below
   never merges two rows. Numbers as printed (space or no-break space thousands separator, decimal comma); Bronze rejects an edition whose
   cells do not parse or whose classes do not add up to SUMA (to 1 / 10,000 for the bandwidth address points), so a
   cast failing here is a new defect, not data. #}
with cells as (
    select *
    from {{ source('internet_gov_broadband', 'internet_gov_broadband_cells') }}
    qualify dense_rank() over (partition by entity order by ingested_at desc, source_version desc) = 1
),

typed as (
    select
        c.entity,
        c.table_code,
        c.row_label,
        c.column_code,
        c.page_number,
        c.reference_date_text,
        case
            when c.column_code like '%_share'
                then cast(replace(replace(c.value_text, '%', ''), ',', '.') as decimal(7, 2))
        end as share_value,
        case
            when c.column_code not like '%_share'
                then cast(regexp_replace(c.value_text, '[   ]', '', 'g') as bigint)
        end as count_value,
        c.raw_sha256,
        c.source_version
    from cells as c
),

rows as (
    select
        entity,
        table_code,
        row_label,
        reference_date_text,
        min(page_number) as page_number,
        max(count_value) filter (where column_code = 'coverages') as coverages,
        max(share_value) filter (where column_code = 'coverages_share') as coverages_share_pct,
        max(count_value) filter (where column_code = 'address_points') as address_points,
        max(share_value) filter (where column_code = 'address_points_share') as address_points_share_pct,
        max(count_value) filter (where column_code = 'address_points_total') as address_points_total,
        raw_sha256,
        source_version
    from typed
    group by entity, table_code, row_label, reference_date_text, raw_sha256, source_version
),

months (month_name, month_number) as (
    values ('marca', 3), ('czerwca', 6), ('września', 9), ('grudnia', 12)
),

dated as (
    select
        r.*,
        make_date(
            cast(regexp_extract(r.reference_date_text, '(\d{4})$', 1) as integer),
            m.month_number,
            cast(regexp_extract(r.reference_date_text, '^(\d{1,2}) ', 1) as integer)
        ) as reference_date
    from rows as r
    left join months as m on regexp_extract(r.reference_date_text, '^\d{1,2} (\S+) \d{4}$', 1) = m.month_name
),

voivodeships as (
    {{ poc_internet_gov_broadband_voivodeships() }}
)

select
    d.reference_date,
    cast(year(d.reference_date) as varchar) || '-Q' || cast(quarter(d.reference_date) as varchar) as period_label,
    d.table_code,
    case
        when d.row_label = 'SUMA' or d.table_code <> 'territory' then 'PL'
        else v.area_code
    end as area_code,
    case when d.table_code = 'territory' and d.row_label <> 'SUMA' then d.row_label else 'Polska' end as area_name,
    case
        when d.table_code = 'territory' or d.row_label = 'SUMA' then '_T'
        when d.table_code = 'bandwidth' then d.row_label
        else lower(regexp_replace(strip_accents(regexp_replace(d.row_label, ' \(wyłącznie\)$', '')), ' ', '_', 'g'))
    end as class_code,
    case when d.table_code = 'territory' then 'all' else d.row_label end as class_label,
    case when d.table_code = 'bandwidth' and d.row_label <> 'SUMA' then cast(d.row_label as integer) end
        as bandwidth_mbps,
    d.coverages,
    d.coverages_share_pct,
    d.address_points,
    d.address_points_share_pct,
    case
        when d.table_code = 'bandwidth' then 'address points in coverage'
        when d.table_code = 'service_type' then 'all address points'
    end as address_points_share_basis,
    d.address_points_total,
    d.page_number,
    d.entity,
    d.source_version,
    d.raw_sha256
from dated as d
left join voivodeships as v on d.table_code = 'territory' and d.row_label = v.area_name
