{# NBP balance of payments (monthly bop_m, quarterly bop_q) and international investment position (quarterly
   iip_q): one row per workbook × currency × item × period of the newest Landing copy of each workbook. The
   period comes from the year header and the period label (roman month I-XII; "I kw / I Q" … "IV kw / IV Q");
   a label of neither form is not a period and is left out. item_en is the English half of the bilingual
   label ("Rachunek bieżący / Current account"; one IIP label separates the halves by spaces), else the label. item_code is the item's slug, prefixed
   by its block's and then its section's slug where the slug alone repeats within the workbook and currency
   (equity under direct investment assets and liabilities). Amounts in million units of the currency. #}
with source_rows as (
    select *
    from {{ source('nbp_bop', 'nbp_bop_cells') }}
    qualify row_number() over (
        partition by raw_sha256, sheet, source_row_number, column_number order by ingested_at
    ) = 1
),

newest as (
    select *
    from source_rows
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

labelled as (
    select
        *,
        trim(regexp_replace(sheet, '^IIP ', '')) as currency,
        case entity when 'bop_m' then 'M' else 'Q' end as frequency,
        -- the English half: after " / ", else after a run of spaces ("…długoterminowe  Bonds and notes")
        trim(coalesce(
            nullif(regexp_extract(item_label, '/\s*([^/]+)$', 1), ''),
            nullif(regexp_extract(item_label, '\S\s{2,}([A-Z][^ ].*)$', 1), ''),
            item_label
        )) as item_en,
        trim(coalesce(nullif(regexp_extract(block_label, '/\s*([^/]+)$', 1), ''), block_label)) as block_en,
        trim(coalesce(nullif(regexp_extract(section_label, '/\s*([^/]+)$', 1), ''), section_label)) as section_en,
        trim(regexp_replace(period_text, '\s+', ' ', 'g')) as period_clean
    from newest
),

periods as (
    select
        *,
        try_cast(year_text as integer) as year,
        case
            when frequency = 'M'
                then list_position(
                    ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'XI', 'XII'], upper(period_clean)
                )
            else
                (list_position(['I', 'II', 'III', 'IV'], upper(regexp_extract(period_clean, '^([IV]+) kw', 1))) - 1)
                * 3 + 1
        end as start_month,
        regexp_replace(lower(trim(item_en)), '[^a-z0-9]+', '_', 'g') as item_slug,
        regexp_replace(lower(trim(block_en)), '[^a-z0-9]+', '_', 'g') as block_slug,
        regexp_replace(lower(trim(section_en)), '[^a-z0-9]+', '_', 'g') as section_slug
    from labelled
),

placed as (
    select
        *,
        make_date(year, start_month, 1) as date_key,
        count(distinct source_row_number) over (partition by entity, sheet, item_slug) as same_item,
        count(distinct source_row_number) over (partition by entity, sheet, block_slug, item_slug) as same_in_block
    from periods
    where year is not null and start_month is not null and start_month > 0
)

select
    entity,
    currency,
    frequency,
    date_key,
    cast(year as varchar) || case frequency
        when 'M' then '-' || lpad(cast(start_month as varchar), 2, '0')
        else '-Q' || cast((start_month + 2) // 3 as varchar)
    end as period_label,
    trim(both '_' from case
        when same_item = 1 then item_slug
        when item_slug = block_slug then section_slug || '.' || item_slug
        when same_in_block = 1 then block_slug || '.' || item_slug
        else section_slug || '.' || block_slug || '.' || item_slug
    end) as item_code,
    item_label,
    item_en,
    block_label,
    section_label,
    source_row_number,
    try_cast(replace(value_text, ',', '.') as double) as value,
    value_text,
    raw_sha256,
    source_version,
    ingested_at
from placed
