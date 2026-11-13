{# Senate of the Republic of Poland (register PLG-025, route D36): sitting days of term XI.
   Grain: sitting_id x day_number (sitting_id = the Senate's internal id in its page addresses; sitting_number = the
   printed number of the sitting). Source: bronze.senat_sittings (the sittings list: the days with a votes page and
   the dates as printed, e.g. '4, 5 i 6 listopada 2026 r.') and bronze.senat_day_votes (votes listed per day).
   Planned sittings have no days yet and are not here (a plan is not an event).
   day_date: the printed dates are split on ',', ' i ', ' oraz '; a date without a month or year takes them from the
   next date that has them; day n is the n-th date. A finished sitting must print as many dates as it has days; a
   running sitting (status other than 'Zakończone') prints the dates of the whole sitting while only its first days
   have a votes page, so it may have fewer days than dates. Otherwise, or when a date does not read as
   '<day> [<month in the genitive> [<year>]]', day_date stays empty and the tests fail: a new way of printing dates
   is never guessed. A sitting listed on two list pages (a run stopped between pages) is taken
   from the newest copy; two copies of the same time fail the unique test. #}
with listed as (
    select
        sitting_id,
        sitting_label,
        cast(sitting_number as integer) as sitting_number,
        date_text,
        status_text,
        day_numbers,
        source_version,
        raw_sha256
    from {{ source('senat', 'senat_sittings') }}
    qualify ingested_at = max(ingested_at) over (partition by sitting_id)
),

days as (
    select
        l.sitting_id,
        cast(d.day as integer) as day_number,
        len(string_split(l.day_numbers, '|')) as days_listed
    from listed as l
    cross join unnest(string_split(l.day_numbers, '|')) as d (day)
    where d.day <> ''
),

parts as (
    select
        sitting_id,
        unnest(parts) as part,
        generate_subscripts(parts, 1) as pos,
        len(parts) as dates_printed
    from (
        select
            sitting_id,
            regexp_split_to_array(
                trim(regexp_replace(date_text, '\s*\br\.', '', 'g')), '\s*,\s*|\s+i\s+|\s+oraz\s+'
            ) as parts
        from listed
    ) as split
),

read_dates as (
    select
        sitting_id,
        pos,
        dates_printed,
        regexp_full_match(part, '\d{1,2}( \p{L}+( \d{4})?)?') as readable,
        try_cast(regexp_extract(part, '^(\d{1,2})', 1) as integer) as day_of_month,
        nullif(regexp_extract(part, '^\d{1,2} (\p{L}+)', 1), '') as month_name,
        try_cast(nullif(regexp_extract(part, ' (\d{4})$', 1), '') as integer) as year
    from parts
),

filled as (
    select
        sitting_id,
        pos,
        dates_printed,
        readable,
        day_of_month,
        list_position(
            [
                'stycznia', 'lutego', 'marca', 'kwietnia', 'maja', 'czerwca', 'lipca', 'sierpnia', 'września',
                'października', 'listopada', 'grudnia'
            ],
            first_value(month_name ignore nulls) over (
                partition by sitting_id order by pos rows between current row and unbounded following
            )
        ) as month_number,
        first_value(year ignore nulls) over (
            partition by sitting_id order by pos rows between current row and unbounded following
        ) as year
    from read_dates
),

day_pages as (
    select
        cast(sitting_id as varchar) as sitting_id,
        cast(day_number as integer) as day_number,
        count(vote_id) as votes_listed,
        raw_sha256 as day_page_sha256
    from {{ source('senat', 'senat_day_votes') }}
    group by sitting_id, day_number, raw_sha256
)

select
    l.sitting_id,
    l.sitting_number,
    d.day_number,
    case
        when
            f.readable
            and (f.dates_printed = d.days_listed or (l.status_text <> 'Zakończone' and d.days_listed < f.dates_printed))
            then try_cast(make_date(f.year, f.month_number, f.day_of_month) as date)
    end as day_date,
    l.date_text as sitting_date_text,
    l.status_text,
    l.status_text = 'Zakończone' as is_finished,
    p.day_page_sha256 is not null as day_page_landed,
    p.votes_listed,
    l.source_version,
    l.raw_sha256
from days as d
inner join listed as l on d.sitting_id = l.sitting_id
left join filled as f on d.sitting_id = f.sitting_id and d.day_number = f.pos
left join day_pages as p on d.sitting_id = p.sitting_id and d.day_number = p.day_number
