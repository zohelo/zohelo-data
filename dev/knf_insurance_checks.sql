-- silver_by_series (log only)
select series, frequency, count(*), count(distinct entity), min(period_label), max(period_label)
from knf_insurance_observations group by all order by 1;
-- annotate grain_dups
select count(*) from (
    select series, period_label, table_code, item_label, provider, measure_code from knf_insurance_observations
    group by all having count(*) > 1
);
-- annotate null_keys
select count(*) from knf_insurance_observations
where series is null or period_label is null or table_code is null or item_label is null or provider is null
    or measure_code is null or unit_code is null or value is null or raw_sha256 is null;
-- annotate bulletin_period_vs_entity
select count(*) filter (where entity <> 'bulletin_' || year(period_end) || '_q' || quarter(period_end) || '_a'),
    count(*) filter (where period_from <> make_date(year(period_end), 1, 1)),
    count(distinct entity)
from knf_insurance_observations where series = 'bulletin';
-- annotate ofe_ike_period_vs_entity
select count(*) filter (where series = 'ofe' and entity <> 'ofe_' || strftime(period_end, '%Y_%m')),
    count(*) filter (where series in ('ike', 'ikze') and entity <> series || '_' || year(period_end)
        || case frequency when 'S' then '_h1' else '' end)
from knf_insurance_observations;
-- annotate markers_in_modelled_sheets
with t as (
    select distinct entity, sheet_name from cells
    where ends_with(entity, '_a') and column_number = 1 and row_number <= 3
        and regexp_matches(trim(regexp_replace(value_text, '\s+', ' ', 'g')),
            '^Table A\.[0-9]+\. (Premiums|Claims) of (Life|Non-Life) Insurance Companies$')
    union all
    select distinct entity, sheet_name from cells
    where starts_with(entity, 'ofe_') and sheet_name in ('Aktywa netto', 'Członkowie', 'Składki', 'Jednostka rozrachunkowa')
    union all
    select distinct entity, sheet_name from cells
    where (starts_with(entity, 'ike_') or starts_with(entity, 'ikze_')) and ends_with(sheet_name, '_dane liczbowe')
)
select split_part(c.entity, '_', 1), trim(c.value_text), count(*)
from cells as c inner join t on c.entity = t.entity and c.sheet_name = t.sheet_name
where c.cell_type in ('s', 'inlineStr', 'str') and length(trim(c.value_text)) <= 4 and c.column_number >= 2
group by all order by 1, 3 desc;
-- ike_ikze_sex_rows_value (log only)
select series, period_label, item_code, value from knf_insurance_observations
where series in ('ike', 'ikze') and provider = '_T' and measure_code = 'value' and regexp_matches(item_label, 'kobiety|mężczyźni')
order by 1, 2, 3 limit 12;
-- zero_values_by_series (log only)
select series, measure_code, count(*) filter (where value = 0), count(*) from knf_insurance_observations
where is_total group by all having count(*) filter (where value = 0) > 0 order by 1, 2;
-- per quarter rows (log only)
select series, period_label, count(*), count(distinct table_code), count(*) filter (where is_total)
from knf_insurance_observations group by all order by 1, 2;
-- unknown headers (log only)
select series, table_code, measure_label, count(*) from knf_insurance_observations where measure_code is null or unit_code is null or provider is null
group by all
;
-- annotate ike_ikze_printed_zero_values_kept
select series, provider, left(item_label, 48), count(*), min(period_label), max(period_label)
from knf_insurance_observations as o
where series in ('ike', 'ikze') and measure_code = 'value' and value = 0
group by all order by 1, 4 desc limit 12
;
-- annotate printed_zeros_by_series_measure
select series, measure_code, count(*) filter (where value = 0), count(*) from knf_insurance_observations
group by all having count(*) filter (where value = 0) > 0 order by 1, 2
;
-- annotate text_in_value_positions_of_modelled_rows
with v as (
    select o.entity, o.source_row, o.series from knf_insurance_observations as o group by all
)
select c.series, trim(c.value_text), count(*) from (
    select *, split_part(entity, '_', 1) as series from cells
    where cell_type in ('s', 'inlineStr', 'str') and column_number >= 3 and length(trim(value_text)) <= 6
) as c
inner join v on c.entity = v.entity and c.row_number = v.source_row
group by all order by 1, 3 desc limit 20
;
-- annotate totals_vs_sum_of_parts
select series, period_label, table_code, measure_code,
    max(value) filter (where is_total) as total,
    sum(value) filter (where not is_total) as parts,
    round(max(value) filter (where is_total) - sum(value) filter (where not is_total), 2) as diff
from knf_insurance_observations
where (series = 'ofe' and measure_code in ('net_assets', 'members'))
    or (series = 'bulletin' and measure_code = 'gross_written_premium' and period_label in ('2023-Q4', '2026-Q2'))
group by all order by abs(diff) desc nulls last limit 12
;
-- annotate silver_nulls_and_periods
select series, frequency, count(*), count(distinct period_label), min(period_label), max(period_label),
    count(*) filter (where item_label is null or unit_code is null or measure_code is null or provider is null)
from knf_insurance_observations group by all order by 1, 2
;
-- layout dump of older workbooks (log only)
select entity, sheet_name, row_number, column_number, cell_type, left(value_text, 90) from cells
where (entity in ('bulletin_2023_q1_a', 'bulletin_2024_q4_a') and sheet_name like 'Tabl.A.3%' and row_number <= 7)
    or (entity in ('ike_2023', 'ike_2024', 'ikze_2023') and row_number between 5 and 10)
order by all
