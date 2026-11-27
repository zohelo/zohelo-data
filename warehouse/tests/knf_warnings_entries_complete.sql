-- knf_warnings (PLG-019): every copy in Silver holds what the page stated: its entries add up to the page's "Liczba
-- znalezionych ostrzeżeń", each section's entries to the section's count with numbers 1..n and no gap, and Silver is
-- not empty. A changed page layout that loses entries fails here instead of thinning the counts silently. One test
-- per Silver model (it reads no other model).
with sections as (
    select
        source_version,
        section_key,
        count(*) as entries,
        max(section_entries) as stated,
        count(distinct section_entries) as statements,
        min(lp) as first_lp,
        max(lp) as last_lp
    from {{ ref('knf_warnings_entries') }}
    group by source_version, section_key
),

copies as (
    select
        source_version,
        count(*) as entries,
        max(stated_total) as stated,
        count(distinct stated_total) as statements
    from {{ ref('knf_warnings_entries') }}
    group by source_version
)

select
    source_version || ' ' || section_key as item,
    entries || ' entries, ' || stated || ' stated, numbers ' || first_lp || '..' || last_lp as problem
from sections
where entries <> stated or statements > 1 or first_lp <> 1 or last_lp <> entries
union all
select
    source_version as item,
    entries || ' entries, the page stated ' || stated as problem
from copies
where entries <> stated or statements > 1
union all
select
    'all' as item,
    'no entries' as problem
from (select count(*) as n from {{ ref('knf_warnings_entries') }}) as t
where t.n = 0
