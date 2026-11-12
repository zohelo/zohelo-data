-- ZUS statistical portal, insured_health: the month of a row is its place in the year file (the files mistype some
-- labels, e.g. 2026: I, II, III, VI, V, VI, VII, VII, IX). Guards that position: a file has at most 12 month rows and
-- the label of its last row carries the Roman numeral of that row's month. A skipped or extra row in column A fails
-- here instead of shifting the months; returns the offending files.
with months as (
    select
        entity,
        month(period_start) as month_no,
        list_position(
            ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'XI', 'XII'],
            regexp_extract(row_label, '^([IVX]+)\s', 1)
        ) as label_no
    from {{ ref('zus_psz_observations') }}
    where family = 'insured_health'
)

select
    entity,
    count(*) as month_rows,
    max(month_no) as last_month,
    arg_max(label_no, month_no) as last_label_month
from months
group by entity
having count(*) > 12 or max(month_no) <> coalesce(arg_max(label_no, month_no), -1)
