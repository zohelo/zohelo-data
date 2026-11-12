{# Ministry of Finance State Treasury debt workbook (Zadluzenie_Skarbu_Panstwa.xlsx, newest copy), every
   time-series sheet in long form (macro xlsx_wide_series): debt by instrument, by holder, by currency, actual
   maturity, duration and average maturity, monthly since 2001. One row per sheet × block × item × period end;
   repeated labels (e.g. "EUR" under fixed and floating rate) are told apart by item_occurrence. unit: mln_pln
   for amounts, ratio for '%'-formatted cells, number otherwise (e.g. years of duration). #}
select
    sheet_name,
    block_title,
    item_label,
    item_occurrence,
    item_code,
    period_end,
    value,
    case
        when contains(coalesce(value_format, ''), '%') then 'ratio'
        when sheet_name like 'duration%' then 'number'
        else 'mln_pln'
    end as unit,
    header_row,
    source_row,
    raw_sha256,
    source_version,
    ingested_at
from ({{ xlsx_wide_series(source('mf_treasury_debt', 'mf_treasury_debt_cells')) }})
