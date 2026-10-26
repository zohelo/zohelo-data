{{ config(alias='dim_frequency') }}
{# Gold PoC: SDMX frequency codes; a fact table holds one frequency, the column repeats it. #}
select * from (
    values
    ('A', 'Annual', 'roczna', 1, 'is_year_start'),
    ('S', 'Half-yearly', 'półroczna', 2, 'is_half_start'),
    ('Q', 'Quarterly', 'kwartalna', 3, 'is_quarter_start'),
    ('M', 'Monthly', 'miesięczna', 4, 'is_month_start'),
    ('W', 'Weekly', 'tygodniowa', 5, 'is_week_start'),
    ('D', 'Daily', 'dzienna', 6, null)
) as t (frequency, name_en, name_pl, sort_order, calendar_flag)
