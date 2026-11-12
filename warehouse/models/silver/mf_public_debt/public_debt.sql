{# Ministry of Finance public debt workbook (Dlug_publiczny_<q>.xlsx, newest copy), every time-series sheet in
   long form (macro xlsx_wide_series): public finance sector debt before and after consolidation by subsector,
   by instrument (place of issue; residency), general government (EDP) debt by ESA 2010 instrument, PDP vs EDP
   bridge, and debt in relation to GDP. One row per sheet × block × item × period end. unit: mln_pln (nominal,
   million zloty) or ratio (a fraction of GDP, the '%'-formatted cells). #}
select
    sheet_name,
    block_title,
    item_label,
    item_occurrence,
    item_code,
    period_end,
    value,
    case when contains(coalesce(value_format, ''), '%') then 'ratio' else 'mln_pln' end as unit,
    header_row,
    source_row,
    raw_sha256,
    source_version,
    ingested_at
from ({{ xlsx_wide_series(source('mf_public_debt', 'mf_public_debt_cells')) }})
