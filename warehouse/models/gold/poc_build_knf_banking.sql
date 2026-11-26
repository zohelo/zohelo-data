{{ config(alias='build_knf_banking', tags=['gold_block']) }}
{# Gold build stage: KNF monthly banking-sector data (register PLG-017), observations of silver.knf_banking_series.
   Block 'banking_sector'. The smallest selection the register names, seven items picked by sheet and item path
   (never by row position); the test knf_banking_selected_items fails when one of them is missing for the banking
   sector, so a renamed label stops the build instead of dropping a measure:
   - total assets (Bilans 'Aktywa, razem'), M, zł;
   - net profit of the current year, year to date (RZiS 'Zysk (strata) roku bieżącego'), M, zł;
   - gross receivables from the non-financial sector without impairment (stages 1 and 2) and with impairment
     (stage 3), portfolio B (Należności_1), M, zł;
   - deposits of the non-financial sector, without blocked deposits (Zobowiązania), M, zł;
   - employment (Struktura 'Zatrudnienie > Liczba zatrudnionych'), M, persons;
   - total capital ratio (Adekwatność "Łączny współczynnik kapitałowy"), Q, a fraction as stored (0.215 = 21.5%).
   One measure per item, one frequency per measure. Poland only (geography 'PL'); the bank group of the block is the
   breakdown bank_group, 'Sektor bankowy' (the whole sector) is the total. Values as published (no rescaling); empty
   cells are absent in Silver, so no row here is a placeholder 0. #}
with selected (measure_code, sheet_name, item_path, unit_code, measure_name) as (
    values
    ('total_assets', 'Bilans', 'Aktywa, razem', 'pln', 'Total assets of banks (KNF)'),
    (
        'net_profit_ytd', 'RZiS', 'Zysk (strata) roku bieżącego', 'pln',
        'Net profit of banks, current year to date (KNF)'
    ),
    (
        'nonfin_receivables_not_impaired', 'Należności_1',
        'SEKTOR NIEFINANSOWY > Należności bez utraty wartości, w tym:', 'pln',
        'Gross receivables of banks from the non-financial sector without impairment, stages 1 and 2 (KNF)'
    ),
    (
        'nonfin_receivables_impaired', 'Należności_1',
        'SEKTOR NIEFINANSOWY > Należności ze stwierdzoną utratą wartości - Faza 3', 'pln',
        'Gross receivables of banks from the non-financial sector with impairment, stage 3 (KNF)'
    ),
    (
        'nonfin_deposits', 'Zobowiązania',
        'Zobowiązania wobec sektora niefinansowego > DEPOZYTY (bez depozytów zablokowanych) > '
        || 'Depozyty sektora niefinansowego',
        'pln', 'Deposits of the non-financial sector in banks, without blocked deposits (KNF)'
    ),
    ('employees', 'Struktura', 'Zatrudnienie > Liczba zatrudnionych', 'persons', 'Employees of banks (KNF)'),
    (
        'total_capital_ratio', 'Adekwatność', 'Łączny współczynnik kapitałowy', 'share',
        'Total capital ratio of banks (KNF)'
    )
),

bank_groups (group_label, group_code) as (
    values
    ('Sektor bankowy', 'banking_sector'),
    ('Banki komercyjne', 'commercial_banks'),
    ('Oddziały', 'credit_institution_branches'),
    ('Banki spółdzielcze', 'cooperative_banks')
),

rows_in as (
    select
        s.*,
        x.measure_code,
        x.unit_code,
        x.measure_name,
        g.group_code
    from {{ ref('knf_banking_series') }} as s
    inner join selected as x on s.sheet_name = x.sheet_name and s.item_path = x.item_path
    inner join bank_groups as g on s.group_label = g.group_label
    where s.cell_status = 'ok' and s.value is not null
)

select
    'knf_banking' as source_system,
    'banking_sector' as block_id,
    'KNF monthly banking-sector data' as block_name,
    'knf_banking:banking_sector:' || unit_code || ':' || measure_code as measure_key,
    measure_name,
    measure_code as measure_codes,
    unit_code,
    case unit_code when 'pln' then 'zł' when 'persons' then 'persons' else 'share (1 = 100%)' end as unit_name,
    'PL' as geography_source_code,
    'PL' as geography_key,
    frequency,
    case when frequency = 'Q' then cast(date_trunc('quarter', period_end) as date) else period_date end as date_key,
    case
        when frequency = 'Q' then year(period_end) || '-Q' || quarter(period_end)
        else strftime(period_end, '%Y-%m')
    end as period_label,
    case
        when group_code = 'banking_sector' then '{}'
        else cast(to_json(map(['bank_group'], [group_code])) as varchar)
    end as breakdown_json,
    case
        when group_code <> 'banking_sector' then cast(to_json(map(['bank_group'], [group_label])) as varchar)
    end as breakdown_label_json,
    cast(null as varchar) as sex_code,
    group_code = 'banking_sector' as is_total,
    value,
    'ok' as observation_status_key,
    source_version,
    raw_sha256
from rows_in
