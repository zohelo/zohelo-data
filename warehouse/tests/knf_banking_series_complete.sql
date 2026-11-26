-- knf_banking: every block (sheet x bank group) has a value for every period between its first and last period
-- (months, or quarter ends for the quarterly blocks), and the four bank groups of the balance sheet are there. A date
-- header that stops matching in a new release, or a lost block, fails here instead of thinning the series silently.
-- One test per Silver model (it reads only knf_banking_series).
with periods as (
    select
        sheet_name,
        group_label,
        frequency,
        count(distinct period_end) as found,
        min(period_end) as first_end,
        max(period_end) as last_end
    from {{ ref('knf_banking_series') }}
    group by all
),

expected as (
    select
        *,
        cast(
            (year(last_end) - year(first_end)) * 12 + month(last_end) - month(first_end)
            as integer
        ) // case when frequency = 'Q' then 3 else 1 end + 1 as wanted
    from periods
),

balance_groups as (
    select count(distinct group_label) as groups
    from {{ ref('knf_banking_series') }}
    where sheet_name = 'Bilans'
)

select
    sheet_name,
    group_label,
    found,
    wanted
from expected
where found <> wanted
union all
select
    'Bilans' as sheet_name,
    'bank groups' as group_label,
    groups as found,
    4 as wanted
from balance_groups
where groups <> 4
