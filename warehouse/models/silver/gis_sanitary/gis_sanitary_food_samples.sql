{# GIS annual report (register PLS-018): table "Jakość zdrowotna środków spożywczych" of every edition ("Stan
   sanitarny kraju" 2012-2023, "Raport z działań PIS" 2024-2025), newest copy of each edition
   (bronze.gis_sanitary_cells). Grain: edition x reference_year x origin_code. Each edition prints the report year
   and several years before it; every printed row is kept (is_report_year marks the report's own year, the row Gold
   reads). origin_code: total (all foodstuffs, "razem") and domestic ("krajowe"). The imported and EU columns stay
   in Bronze only: their printed order differs between editions and in at least the 2019 edition the 2018 and 2019
   rows contradict its own header (the 2023 edition and the 2019 text give the other order), so no label is
   trusted for them. components_minus_total (total rows): domestic + imported + EU samples minus the total, as
   printed (-50 for 2020 in every edition, 0 otherwise; Bronze rejects more than 1 / 1,000). Counts and shares as
   printed (thousands separators removed, decimal comma); Bronze rejects a file whose cells do not parse, so a
   failing cast here is a new defect. The table has no suppression or missing marker. #}
with cells as (
    select *
    from {{ source('gis_sanitary', 'gis_sanitary_cells') }}
    qualify dense_rank() over (partition by entity order by ingested_at desc, source_version desc) = 1
),

typed as (
    select
        entity,
        cast(row_label as integer) as reference_year,
        split_part(column_code, '_', 1) as origin_code,
        column_code,
        page_number,
        case when column_code like '%_samples' then cast(replace(value_text, ' ', '') as bigint) end as samples,
        case
            when column_code like '%_disqualified_pct' then cast(replace(value_text, ',', '.') as decimal(5, 2))
        end as pct,
        raw_sha256,
        source_version
    from cells
),

parts as (
    select
        entity,
        reference_year,
        sum(samples) filter (where origin_code in ('domestic', 'imported', 'eu'))
        - max(samples) filter (where origin_code = 'total') as components_minus_total
    from typed
    group by entity, reference_year
)

select
    t.entity,
    cast(regexp_extract(t.entity, '_(\d{4})$', 1) as integer) as edition_year,
    case when t.entity like 'pis_%' then 'Raport z działań PIS' else 'Stan sanitarny kraju' end as edition_family,
    t.reference_year,
    cast(t.reference_year as varchar) as period_label,
    t.origin_code,
    case t.origin_code when 'total' then 'razem' when 'domestic' then 'krajowe' end as origin_label,
    max(t.samples) as samples_examined,
    max(t.pct) as disqualified_pct,
    t.reference_year = cast(regexp_extract(t.entity, '_(\d{4})$', 1) as integer) as is_report_year,
    case when t.origin_code = 'total' then max(p.components_minus_total) end as components_minus_total,
    min(t.page_number) as page_number,
    count(*) as cell_count,
    t.source_version,
    t.raw_sha256
from typed as t
inner join parts as p on t.entity = p.entity and t.reference_year = p.reference_year
where t.origin_code in ('total', 'domestic')
group by t.entity, t.reference_year, t.origin_code, t.source_version, t.raw_sha256
