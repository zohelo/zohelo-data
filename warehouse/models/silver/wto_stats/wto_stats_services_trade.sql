{# WTO Stats, OECD-WTO Balanced Trade in Services (BaTIS, BPM6; register GL-020): annual trade in services in
   millions of US dollars by reporter, partner, flow (X exports, M imports) and service item (total services 'S',
   the twelve EBOPS 2010 main items 'SA' … 'SL', their sub-items and the derived items of type 'd'), from the newest
   copy of entity batis_bpm6 (bronze.wto_stats_batis: Poland as reporter or partner, and the country-group reporters).
   Three values per cell, each as published: reported_value (the figure the reporter published, null where it
   published none), final_value (the reporter's series after the OECD-WTO estimation, with final_value_methodology:
   R_* reported, E* estimated, M* modelled, W0 and A as the codes sheet explains) and balanced_value (the balanced
   matrix, an estimate that reconciles both partners' figures). Zeros are published zeros (E0 "estimated as
   negligible/zero"), never filled empty cells. Labels from the codes workbook of the same ZIP
   (bronze.wto_stats_batis_codes). Codes are BaTIS' own: ISO 3166-1 alpha-2 for most economies, WL World, EU,
   GEU EU extra-trade, ROW rest of the world.
   Grain: reporter_code x partner_code x flow x item_code x year. #}
with batis as (
    select *
    from {{ source('wto_stats', 'wto_stats_batis') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

codes as (
    select *
    from {{ source('wto_stats', 'wto_stats_batis_codes') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

code_rows as (
    select
        sheet_name,
        row_number,
        max(case when column_number = 1 then value_text end) as code,
        max(case when column_number = 2 then value_text end) as label
    from codes
    where row_number > 1 and sheet_name in ('economies', 'service items', 'methodology codes')
    group by sheet_name, row_number
),

economies as (
    select
        code,
        label
    from code_rows
    where sheet_name = 'economies'
),

items as (
    select
        code,
        label
    from code_rows
    where sheet_name = 'service items'
),

methods as (
    select
        code,
        label
    from code_rows
    where sheet_name = 'methodology codes'
)

select
    b.entity,
    regexp_extract(b.csv_member, 'BPM6_([A-Za-z]+[0-9]{4})_bulk', 1) as release,
    b.reporter as reporter_code,
    r.label as reporter_name,
    b.type_reporter as reporter_type,
    b.partner as partner_code,
    p.label as partner_name,
    b.type_partner as partner_type,
    b.flow,
    b.item_code,
    i.label as item_name,
    b.type_item as item_type,
    case when regexp_full_match(b.year, '(19|20)[0-9]{2}') then cast(b.year as integer) end as year,
    case
        when regexp_full_match(b.reported_value, '-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?')
            then cast(b.reported_value as double)
    end as reported_value,
    case
        when regexp_full_match(b.final_value, '-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?')
            then cast(b.final_value as double)
    end as final_value,
    b.final_value_methodology,
    m.label as final_value_methodology_label,
    case
        when regexp_full_match(b.balanced_value, '-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?')
            then cast(b.balanced_value as double)
    end as balanced_value,
    b.raw_sha256,
    b.source_version
from batis as b
left join economies as r on b.reporter = r.code
left join economies as p on b.partner = p.code
left join items as i on b.item_code = i.code
left join methods as m on b.final_value_methodology = m.code
