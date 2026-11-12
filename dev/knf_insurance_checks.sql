-- annotate silver_by_series
select series, frequency, count(*), count(distinct entity), min(period_label), max(period_label)
from knf_insurance_observations group by all order by 1;
-- annotate grain_duplicates
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
    count(*) filter (where series in ('ike', 'ikze') and entity <> series || '_' || year(period_end))
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
-- annotate ike_ikze_sex_rows_value
select series, period_label, item_code, value from knf_insurance_observations
where series in ('ike', 'ikze') and provider = '_T' and measure_code = 'value' and regexp_matches(item_label, 'kobiety|mężczyźni')
order by 1, 2, 3 limit 12;
-- annotate zero_values_by_series
select series, measure_code, count(*) filter (where value = 0), count(*) from knf_insurance_observations
where is_total group by all having count(*) filter (where value = 0) > 0 order by 1, 2;
-- per quarter rows (log only)
select series, period_label, count(*), count(distinct table_code), count(*) filter (where is_total)
from knf_insurance_observations group by all order by 1, 2;
-- unknown headers (log only)
select series, table_code, measure_label, count(*) from knf_insurance_observations where measure_code is null or unit_code is null or provider is null
group by all
;
-- annotate ike_ikze_zero_value_markers_dropped
select split_part(v.entity, '_', 1), count(*) from cells as v
inner join cells as n on v.entity = n.entity and v.sheet_name = n.sheet_name and v.row_number = n.row_number
    and n.column_number = v.column_number - 1
where ends_with(v.sheet_name, '_dane liczbowe') and v.column_number in (4, 6, 8, 10, 12, 14) and v.row_number > 9
    and v.cell_type = 'n' and try_cast(v.value_text as double) = 0 and try_cast(n.value_text as double) > 0
group by 1 order by 1
