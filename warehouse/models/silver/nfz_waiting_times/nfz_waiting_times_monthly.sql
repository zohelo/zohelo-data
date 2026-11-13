{# NFZ waiting times (register PLS-015): counts by voivodeship, case and month from every complete monthly snapshot
   (macro poc_nfz_waiting_times_snapshots); province_code '00' = Poland, the sum of the 16 voivodeships. These are
   zohelo calculations on the published values, which are not changed:
   - waiting_lists: the queue entries (provider x place x benefit) the snapshot lists; month = the snapshot month;
   - persons_awaiting: the sum of the month-end counts of persons awaiting that the providers published for the
     month before the snapshot month (statistics month); month = that statistics month. A count is summed once per
     waiting list, never once per place: an entry with many_places = 'N' is its own list; the entries with
     many_places = 'Y' of one voivodeship, provider, benefit and case are one list shown at several places and enter
     once when they all publish the same count and statistics month, and not at all when they differ (which place
     holds the list is then unknown). A person on several lists is counted on each. Missing or older counts are not
     summed (never taken as 0);
   - lists_reporting: the waiting lists whose count for that statistics month was summed.
   Grain: measure_code x province_code x case_code x period_month. raw_sha256: the Landing file of a voivodeship
   row; for Poland the SHA-256 of the sorted list of the 16 files' SHA-256. #}
with snapshots as ({{ poc_nfz_waiting_times_snapshots() }}),

keyed as (
    -- the waiting list of an entry: its own (many_places 'N' or not published), or for many_places 'Y' the list of
    -- its voivodeship, provider, benefit and case shown at several places
    select
        s.*,
        case
            when s.many_places = 'Y' then concat_ws('|', 'Y', s.provider_code, s.benefit)
            else 'N|' || s.queue_id
        end as list_key
    from snapshots as s
),

lists as (
    -- one row per waiting list; the count of a list shown at several places enters only when every place gives the
    -- same count for the same statistics month
    select
        snapshot_month,
        province_code,
        case_code,
        list_key,
        count(*) as entries,
        case
            when
                count(distinct awaiting) = 1
                and count(awaiting) = count(*)
                and count(distinct statistics_month) = 1
                and count(statistics_month) = count(*)
                then min(awaiting)
        end as awaiting,
        case
            when
                count(distinct statistics_month) = 1
                and count(statistics_month) = count(*)
                then min(statistics_month)
        end as statistics_month,
        min(raw_sha256) as raw_sha256,
        max(source_version) as source_version
    from keyed
    group by all
),

areas as (
    select
        l.snapshot_month,
        l.province_code,
        l.case_code,
        l.entries,
        l.awaiting,
        l.statistics_month,
        l.raw_sha256,
        l.source_version
    from lists as l
    union all
    select
        l.snapshot_month,
        '00' as province_code,
        l.case_code,
        l.entries,
        l.awaiting,
        l.statistics_month,
        l.raw_sha256,
        l.source_version
    from lists as l
),

counted as (
    select
        snapshot_month,
        province_code,
        case_code,
        sum(entries) as waiting_lists,
        sum(awaiting) filter (where statistics_month = snapshot_month - interval 1 month) as persons_awaiting,
        count(awaiting) filter (where statistics_month = snapshot_month - interval 1 month) as lists_reporting,
        sha256(string_agg(distinct raw_sha256, ',' order by raw_sha256)) as files_digest,
        min(raw_sha256) as raw_sha256_one,
        count(distinct raw_sha256) as files,
        max(source_version) as source_version
    from areas
    group by all
),

long as (
    select
        m.measure_code,
        m.unit_code,
        c.province_code,
        c.case_code,
        cast(c.snapshot_month - to_months(m.lag_months) as date) as period_month,
        c.snapshot_month,
        case m.measure_code
            when 'waiting_lists' then c.waiting_lists
            when 'persons_awaiting' then c.persons_awaiting
            when 'lists_reporting' then c.lists_reporting
        end as value,
        case when c.files = 1 then c.raw_sha256_one else c.files_digest end as raw_sha256,
        c.source_version
    from counted as c
    cross join (
        values
        ('waiting_lists', 'waiting_lists', 0),
        ('persons_awaiting', 'persons', 1),
        ('lists_reporting', 'waiting_lists', 1)
    ) as m (measure_code, unit_code, lag_months)
)

select
    measure_code,
    unit_code,
    province_code,
    case_code,
    period_month,
    snapshot_month,
    cast(value as bigint) as value,
    raw_sha256,
    source_version
from long
where value is not null
