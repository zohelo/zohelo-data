-- NBP balance of payments (silver.nbp_bop_observations): a value cell in an item row outside the period grid
-- (no year or period label above it) or one that is not a number is kept with date_key or value null; any such
-- cell fails here, so a change of the workbook layout is never dropped silently (#2754).
select
    entity,
    currency,
    item_code,
    source_row_number,
    source_column_number,
    period_label,
    value_text
from {{ ref('nbp_bop_observations') }}
where date_key is null or value is null
