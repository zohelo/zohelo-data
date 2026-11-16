{# NID register of immovable monuments: the register decisions of each item (register PLS-026), from
   silver.nid_monuments_items. Grain: list_code × item_number × decision_number (the decisions in the order the item
   gives them; an item of role same_as_above or component gets the decisions of its register_item_number).
   A decision is "<register number> z <date>" as printed in the register part, e.g. "A-142/2010 z 30.12.2010",
   "56 z 29.03.1949, 705/64 z 2.03.1964 i 698/64 z 10.01.1994", "dec. z 14.01.2026" (a decision without a number):
   - register_number: the text before " z ", notes in brackets removed, whitespace collapsed, a leading "i" removed; empty (another decision
     under the number named just before: "A/389 z 04.1963 i z 18.01.1969") takes the previous number of the item;
   - decision_date: the date (dd.mm.yyyy; '.', ',', '-' or '/' as separators) or null when it is not a full date
     (a year, a month and year, a two-digit year), decision_date_text keeping it as printed;
   - a register part with no " z <date>" at all is one decision whose number is the whole part (ensemble colon and
     notes in brackets removed) and whose date is null.
   register_key = list_code:register_number:date (ISO date, else the date as printed): one register entry (a
   decision) of one list; numbers alone repeat within a list, because each voivodeship list joins the registers of
   the voivodeships before 1999. #}
{# A decision: the number (lazily, no comma or semicolon) before " z " (optionally "dn."), then a date: dd.mm.yyyy
   with '.', ',', '/' or '-' and stray spaces, dd.mm.yy, mm.yyyy or a year. #}
{% set decision_re = '([^,;]*?)\\s?z\\s?(?:dn\\.\\s?)?('
    ~ '[0-9]{1,2}\\s?[.,/-]\\s?[0-9]{1,2}\\s?[.,/-]\\s?[0-9][0-9 ]{1,4}[0-9]'
    ~ '|[0-9]{1,2}[.,/-][0-9]{1,2}[.,/-][0-9]{2}\\b'
    ~ '|[0-9]{1,2}[.,/-][0-9]{4}'
    ~ '|[0-9]{4})' %}
with items as (
    select * from {{ ref('nid_monuments_items') }}
),

own as (
    select
        list_code,
        item_number,
        register_text,
        regexp_extract_all(
            regexp_replace(register_text, '\([^)]*\)', ' ', 'g'),
            '{{ decision_re }}',
            1
        ) as numbers,
        regexp_extract_all(
            regexp_replace(register_text, '\([^)]*\)', ' ', 'g'),
            '{{ decision_re }}',
            2
        ) as dates
    from items
    where register_role = 'numbered'
),

exploded as (
    select
        list_code,
        item_number,
        register_text,
        ord as decision_number,
        nullif(trim(regexp_replace(regexp_replace(numbers[ord], '\s+', ' ', 'g'), '^\s*i\s+|^\s*i$', '')), '')
            as number_text,
        regexp_replace(dates[ord], '\s', '', 'g') as decision_date_text
    from (
        select
            *,
            unnest(range(1, len(numbers) + 1)) as ord
        from own
    ) as o
    union all
    select
        list_code,
        item_number,
        register_text,
        1 as decision_number,
        trim(regexp_replace(regexp_replace(register_text, '\s*\([^)]*\)\s*|:\s*$', ' ', 'g'), '\s+', ' ', 'g'))
            as number_text,
        cast(null as varchar) as decision_date_text
    from own
    where len(numbers) = 0
),

numbered as (
    select
        list_code,
        item_number,
        decision_number,
        coalesce(
            number_text,
            last_value(number_text ignore nulls) over (
                partition by list_code, item_number order by decision_number
                rows between unbounded preceding and 1 preceding
            )
        ) as register_number,
        decision_date_text,
        case
            when regexp_full_match(decision_date_text, '[0-9]{1,2}[.,/-][0-9]{1,2}[.,/-][0-9]{4}')
                then cast(try_strptime(regexp_replace(decision_date_text, '[,/-]', '.', 'g'), '%d.%m.%Y') as date)
        end as decision_date
    from exploded
)

select
    i.list_code,
    i.woj_code,
    i.item_number,
    n.decision_number,
    i.register_role,
    i.register_item_number,
    n.register_number,
    n.decision_date,
    n.decision_date_text,
    i.list_code || ':' || coalesce(n.register_number, '?') || ':'
    || coalesce(strftime(n.decision_date, '%Y-%m-%d'), n.decision_date_text, '') as register_key,
    i.source_version,
    i.raw_sha256
from items as i
inner join numbered as n on i.list_code = n.list_code and i.register_item_number = n.item_number
