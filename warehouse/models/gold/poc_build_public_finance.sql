{{ config(alias='build_public_finance') }}
{# Gold build stage: Polish public finance from the Ministry of Finance and KRUS (Silver public_debt,
   treasury_debt, budget_execution, krus_benefits), Poland only, no breakdown (the instrument, item or benefit
   is part of the measure). Blocks:
   mf_public_debt   debt_gdp      debt in relation to GDP (percent), State Treasury (SP), state public debt (PDP),
                                  general government EDP debt (GG); quarter ends (Q) and the 31 December value
                                  (A, measure key suffix ':YE', so the annual and the first-quarter row never share
                                  a series × date)
                    debt          the same debts in million zloty (Q)
                    edp_debt      general government (EDP) debt by ESA 2010 instrument, million zloty (Q)
   mf_treasury_debt debt_instrument  State Treasury debt by instrument, million zloty, month ends (M)
   mf_state_budget  execution_ytd    revenue, expenditure, balance and financing, cumulative from January,
                                     thousand zloty (M; the December value as the year, A, suffix ':A')
                    plan             the same lines in the budget act (A)
   krus             benefits         average monthly beneficiaries and benefit by benefit type (Q; A with suffix
                                     ':A'); periods with the KRUS footnote mark (from III quarter 2025: survivors'
                                     pensions include those paid with other pensions) have status 'break' #}
with debt_gdp_items as (
    select * from (
        values
        ('Dług Skarbu Państwa', 'SP', 'State Treasury debt'),
        ('Państwowy dług publiczny', 'PDP', 'State public debt (PDP)'),
        ('Dług sektora General Government', 'GG', 'General government debt (EDP)')
    ) as t (item_label, code, name)
),

debt_gdp as (
    -- the sheet lists the three debts twice under one header: first in million zloty (block debt), then as a
    -- fraction of GDP (block debt_gdp); the occurrence decides, and a row whose format disagrees is left out
    select
        'mf_public_debt' as source_system,
        i.code,
        i.name,
        d.period_end,
        d.source_version,
        d.raw_sha256,
        case when d.item_occurrence = 1 then 'debt' else 'debt_gdp' end as block_id,
        case when d.item_occurrence = 1 then d.value else d.value * 100 end as value
    from {{ ref('public_debt') }} as d
    inner join debt_gdp_items as i on d.item_label = i.item_label
    where
        d.sheet_name = 'Dług w relacji do PKB'
        and ((d.item_occurrence = 1 and d.unit = 'mln_pln') or (d.item_occurrence = 2 and d.unit = 'ratio'))
),

edp as (
    select
        'mf_public_debt' as source_system,
        'edp_debt' as block_id,
        coalesce(item_code, 'TOTAL') as code,
        item_label as name,
        period_end,
        value,
        source_version,
        raw_sha256
    from {{ ref('public_debt') }}
    where sheet_name = 'Dług EDP' and item_occurrence = 1 and unit = 'mln_pln'
),

treasury_items as (
    select * from (
        values
        ('Zadłużenie Skarbu Państwa%', 1, 'TOTAL', 'State Treasury debt, total'),
        ('I. Zadłużenie krajowe%', 1, 'DOMESTIC', 'Domestic debt'),
        ('1.Zadłużenie z tytułu SPW%', 1, 'DOMESTIC_TS', 'Domestic Treasury securities'),
        ('1.1.Rynkowe SPW%', 1, 'DOMESTIC_MARKETABLE', 'Domestic marketable Treasury securities'),
        ('bony skarbowe%', 1, 'TBILLS', 'Treasury bills'),
        ('1.2. Oszczędnościowe SPW%', 1, 'SAVINGS', 'Savings bonds'),
        ('1.3. Nierynkowe SPW%', 1, 'NONMARKETABLE', 'Non-marketable Treasury securities'),
        ('II. Zadłużenie zagraniczne%', 1, 'FOREIGN', 'Foreign debt'),
        ('1.Zadłużenie z tytułu SPW%', 2, 'FOREIGN_TS', 'Treasury securities issued abroad'),
        ('2. Kredyty i pożyczki zagraniczne%', 1, 'FOREIGN_LOANS', 'Foreign loans and credits')
    ) as t (label_like, occurrence, code, name)
),

treasury as (
    select
        'mf_treasury_debt' as source_system,
        'debt_instrument' as block_id,
        i.code,
        i.name,
        d.period_end,
        d.value,
        d.source_version,
        d.raw_sha256
    from {{ ref('treasury_debt') }} as d
    inner join treasury_items as i on d.item_label like i.label_like and d.item_occurrence = i.occurrence
    where d.sheet_name = 'zadłużenie wg instrumentów' and d.unit = 'mln_pln'
    -- a label pattern matching two different items of the sheet keeps the first row
    qualify row_number() over (partition by i.code, d.period_end order by d.source_row) = 1
),

budget as (
    select *
    from {{ ref('budget_execution') }}
    where item_key is not null
    -- one line per key: a report repeating a main line keeps its first labelled row (deterministic order)
    qualify
        row_number() over (
            partition by budget_year, measure, month_end, item_key
            order by item_code nulls last, item_label, item_occurrence
        ) = 1
),

krus as (
    select * from {{ ref('krus_benefits') }}
),

observations as (
    select
        d.source_system,
        d.block_id,
        'Public debt in relation to GDP (MF)' as block_name,
        'mf_public_debt:debt_gdp:PC_GDP:' || d.code || case when f.frequency = 'A' then ':YE' else '' end
            as measure_key,
        d.name || ', % of GDP' as measure_name,
        d.code as measure_codes,
        'PC_GDP' as unit_code,
        'percent of GDP' as unit_name,
        f.frequency,
        case
            when f.frequency = 'A' then make_date(year(d.period_end), 1, 1)
            else make_date(year(d.period_end), 3 * quarter(d.period_end) - 2, 1)
        end as date_key,
        case
            when f.frequency = 'A' then cast(year(d.period_end) as varchar)
            else year(d.period_end) || '-Q' || quarter(d.period_end)
        end as period_label,
        d.value,
        d.source_version,
        d.raw_sha256,
        'ok' as observation_status_key
    from debt_gdp as d
    cross join (values ('Q'), ('A')) as f (frequency)
    where d.block_id = 'debt_gdp' and (f.frequency = 'Q' or (month(d.period_end) = 12 and day(d.period_end) = 31))
    union all
    select
        source_system,
        block_id,
        'Public debt (MF)' as block_name,
        'mf_public_debt:debt:MIO_PLN:' || code as measure_key,
        name || ', million PLN' as measure_name,
        code as measure_codes,
        'MIO_PLN' as unit_code,
        'million PLN' as unit_name,
        'Q' as frequency,
        make_date(year(period_end), 3 * quarter(period_end) - 2, 1) as date_key,
        year(period_end) || '-Q' || quarter(period_end) as period_label,
        value,
        source_version,
        raw_sha256,
        'ok' as observation_status_key
    from debt_gdp
    where block_id = 'debt'
    union all
    select
        source_system,
        block_id,
        'General government (EDP) debt by instrument (MF)' as block_name,
        'mf_public_debt:edp_debt:MIO_PLN:' || code as measure_key,
        name || ', million PLN' as measure_name,
        code as measure_codes,
        'MIO_PLN' as unit_code,
        'million PLN' as unit_name,
        'Q' as frequency,
        make_date(year(period_end), 3 * quarter(period_end) - 2, 1) as date_key,
        year(period_end) || '-Q' || quarter(period_end) as period_label,
        value,
        source_version,
        raw_sha256,
        'ok' as observation_status_key
    from edp
    union all
    select
        source_system,
        block_id,
        'State Treasury debt by instrument (MF)' as block_name,
        'mf_treasury_debt:debt_instrument:MIO_PLN:' || code as measure_key,
        name || ', million PLN' as measure_name,
        code as measure_codes,
        'MIO_PLN' as unit_code,
        'million PLN' as unit_name,
        'M' as frequency,
        make_date(year(period_end), month(period_end), 1) as date_key,
        strftime(period_end, '%Y-%m') as period_label,
        value,
        source_version,
        raw_sha256,
        'ok' as observation_status_key
    from treasury
    union all
    select
        'mf_state_budget' as source_system,
        'execution_ytd' as block_id,
        'State budget execution, cumulative (MF)' as block_name,
        'mf_state_budget:execution_ytd:THS_PLN:' || b.item_key || case when f.frequency = 'A' then ':A' else '' end
            as measure_key,
        b.item_label || ', cumulative from January, thousand PLN' as measure_name,
        b.item_key as measure_codes,
        'THS_PLN' as unit_code,
        'thousand PLN' as unit_name,
        f.frequency,
        case when f.frequency = 'A' then make_date(b.budget_year, 1, 1) else b.period_month end as date_key,
        case when f.frequency = 'A' then cast(b.budget_year as varchar) else strftime(b.period_month, '%Y-%m') end
            as period_label,
        b.value_thousand_pln as value,
        b.source_version,
        b.raw_sha256,
        'ok' as observation_status_key
    from budget as b
    cross join (values ('M'), ('A')) as f (frequency)
    where b.measure = 'execution_ytd' and (f.frequency = 'M' or b.month_end = 12)
    union all
    select
        'mf_state_budget' as source_system,
        'plan' as block_id,
        'State budget act (MF)' as block_name,
        'mf_state_budget:plan:THS_PLN:' || item_key as measure_key,
        item_label || ', budget act, thousand PLN' as measure_name,
        item_key as measure_codes,
        'THS_PLN' as unit_code,
        'thousand PLN' as unit_name,
        'A' as frequency,
        make_date(budget_year, 1, 1) as date_key,
        cast(budget_year as varchar) as period_label,
        value_thousand_pln as value,
        source_version,
        raw_sha256,
        'ok' as observation_status_key
    from budget
    where measure = 'plan'
    union all
    select
        'krus' as source_system,
        'benefits' as block_id,
        'KRUS pensions and benefits' as block_name,
        'krus:benefits:' || case when measure = 'beneficiaries' then 'persons' else 'PLN' end || ':' || benefit_key
        || ':' || measure || case when period_type = 'A' then ':A' else '' end as measure_key,
        benefit_group || coalesce(' (' || subgroup || ')', '')
        || case
            when measure = 'beneficiaries' then ', average monthly number of beneficiaries'
            else ', average monthly benefit (PLN)'
        end as measure_name,
        benefit_key || ':' || measure as measure_codes,
        case when measure = 'beneficiaries' then 'persons' else 'PLN' end as unit_code,
        case when measure = 'beneficiaries' then 'persons' else 'PLN' end as unit_name,
        period_type as frequency,
        period_start as date_key,
        case when period_type = 'A' then cast(year as varchar) else year || '-Q' || quarter end as period_label,
        value,
        source_version,
        raw_sha256,
        case when footnote is not null then 'break' else 'ok' end as observation_status_key
    from krus
)

select
    source_system,
    block_id,
    block_name,
    measure_key,
    measure_name,
    measure_codes,
    unit_code,
    unit_name,
    cast(null as varchar) as geography_source_code,
    'PL' as geography_key,
    frequency,
    date_key,
    period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(value as double) as value,
    observation_status_key,
    source_version,
    raw_sha256
from observations
