{# ARiMR direct-payment statistics (register PLS-034): the monthly report "Płatności bezpośrednie" (gov.pl PDF,
   tables read from its text layer in Bronze, bronze.arimr_payments_cells). Three cumulative measures per campaign
   (year of application) and voivodeship, as of the report date: applications for area payments submitted, decisions
   granting area payments issued, amount of direct payments paid (PLN). "Razem" = Poland ('polska'). Every report
   restates all campaigns it shows; the value of the newest report holding a campaign is kept (ranked by report date,
   so two files with one date and one key fail the unique test instead of being picked from). Values as printed
   (decimal comma, digits grouped by spaces); footnote marks of the column header in column_note ('*' decisions:
   cases with a granting decision; '**' applications 2025+: unique producers who applied, amounts 2023+: including
   the CAP Strategic Plan payments). The column "Razem" (total over the campaigns, printed after the last campaign of
   the amounts) is left out. Grain: measure_code x area_name x campaign_year. #}
with cells as (
    select
        entity,
        table_title,
        as_of_text,
        column_label,
        row_label,
        value_text,
        source_version,
        raw_sha256,
        case
            when starts_with(table_title, 'Liczba złożonych wniosków o przyzna') then 'applications'
            when starts_with(table_title, 'Liczba wydanych decyzji o przyzna') then 'decisions'
            when starts_with(table_title, 'Kwota zrealizowanych płatności') then 'amount_paid'
        end as measure_code,
        cast(strptime(as_of_text, '%d.%m.%Y') as date) as as_of_date,
        cast(regexp_extract(column_label, '^(\d{4})', 1) as integer) as campaign_year,
        regexp_extract(column_label, '(\*+)$', 1) as column_note,
        lower(regexp_replace(trim(row_label), '\s*-\s*', '-', 'g')) as label_key
    from ({{ xlsx_newest_cells(source('arimr_payments', 'arimr_payments_cells')) }})
    -- campaign columns only: the total column "Razem Kampanie 2004-2025" of the last amounts table is a row sum
    where regexp_matches(column_label, '^\d{4}\**$')
)

select
    measure_code,
    case measure_code
        when 'amount_paid' then 'PLN'
        when 'applications' then 'applications'
        when 'decisions' then 'decisions'
    end as unit_code,
    case label_key when 'razem' then 'polska' else label_key end as area_name,
    campaign_year,
    as_of_date,
    column_note,
    cast(replace(replace(value_text, ' ', ''), ',', '.') as decimal(18, 2)) as value,
    value_text,
    table_title,
    entity,
    source_version,
    raw_sha256
from cells
qualify rank() over (partition by measure_code, label_key, campaign_year order by as_of_date desc) = 1
