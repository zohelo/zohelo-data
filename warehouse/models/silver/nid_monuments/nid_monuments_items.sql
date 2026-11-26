{# NID register of immovable monuments (register book A), from bronze.nid_monuments_items: one row per item of the
   newest copy of each voivodeship list (17 PDF lists, Warsaw separate; register PLS-026). Grain: list_code ×
   item_number. Read from the item text as published: the object (text before the first comma or colon outside
   brackets), the register part (text after "nr rej"), its role and the list date; the area from the page header
   (powiat as printed, the Warsaw list by district), the locality from the heading above the item. Street addresses
   are not carried (they stay in Bronze): the register lists may locate private houses.
   register_role:
   - numbered: the item names its own decision(s) ("<number> z <date>", or a number without a date);
   - same_as_above: "nr rej.: jw." - the decision(s) of the nearest numbered item above it in the list;
   - component: no register part and not itself an ensemble heading, below a numbered item that ends with a colon
     (an ensemble and its parts) in the same powiat and locality, with nothing between them but other components:
     it shares that item's decision(s);
   - unnumbered: none of these (a heading-like item, an ensemble without a number of its own); counted, never
     given a number. #}
{% set tail_decision = ',\\s*[A-Za-z]{0,4}[-/.]?[0-9][0-9A-Za-z/.-]*\\s?z\\s?'
    ~ '[0-9]{1,2}[.,/-][0-9]{1,2}[.,/-]\\s?[0-9]{4}\\s*:?$' %}
with newest as (
    select *
    from {{ source('nid_monuments', 'nid_monuments_items') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

parsed as (
    select
        entity as list_code,
        page_number,
        item_number,
        page_header,
        line_count,
        source_version,
        raw_sha256,
        case entity
            when 'dln' then '02' when 'kuj' then '04' when 'lbl' then '06' when 'lbs' then '08'
            when 'ldz' then '10' when 'mal' then '12' when 'maz' then '14' when 'maz_war' then '14'
            when 'opo' then '16' when 'pdk' then '18' when 'pdl' then '20' when 'pom' then '22'
            when 'sls' then '24' when 'swt' then '26' when 'war' then '28' when 'wlk' then '30'
            when 'zpo' then '32'
        end as woj_code,
        case
            when entity = 'maz_war' then 'Warszawa'
            else trim(regexp_extract(page_header, '[-–]\s*pow\.\s*(.+)$', 1))
        end as powiat_text,
        case when entity = 'maz_war' then trim(regexp_extract(page_header, '[-–]\s*(.+)$', 1)) end as district_text,
        nullif(trim(heading_text), '') as locality_text,
        cast(try_strptime(regexp_extract(state_text, '([0-9]{1,2}\.[0-9]{1,2}\.[0-9]{4})', 1), '%d.%m.%Y') as date)
            as page_state_date,
        trim(regexp_extract(item_text, '^((?:[^,:(]|\([^)]*\))+)', 1)) as object_name,
        case
            when regexp_matches(item_text, 'nr\s?rej')
                then trim(regexp_extract(item_text, 'nr\s?rej[.,;:]*\s*(.*)$', 1))
            -- a decision without the words "nr rej" at the end of the item ("..., A/1013/96 z 21.06.1996")
            when regexp_matches(item_text, '{{ tail_decision }}')
                then trim(regexp_extract(item_text, ',\s*([A-Za-z]{0,4}[-/.]?[0-9][^,]*)$', 1))
        end as register_text,
        rtrim(item_text) like '%:' as is_group
    from newest
),

roles as (
    select
        *,
        case
            when register_text is null then null
            when regexp_matches(register_text, '^jw') then 'same_as_above'
            else 'numbered'
        end as own_role,
        -- the nearest item above with a register part of its own (same list)
        max(case when register_text is not null and not regexp_matches(register_text, '^jw') then item_number end)
            over (partition by list_code order by item_number rows between unbounded preceding and 1 preceding)
            as numbered_above
    from parsed
),

ensembles as (
    select
        r.*,
        coalesce(
            a.is_group
            and coalesce(a.powiat_text, '') = coalesce(r.powiat_text, '')
            and coalesce(a.locality_text, '') = coalesce(r.locality_text, ''),
            false
        ) as above_is_ensemble,
        -- an item between the numbered item above and this one that has a register part (jw.) or is an ensemble
        -- without a number of its own ends the ensemble
        count(case when r.register_text is not null or r.is_group then 1 end) over (
            partition by r.list_code, r.numbered_above order by r.item_number
            rows between unbounded preceding and 1 preceding
        ) as breaks_between
    from roles as r
    left join roles as a on r.list_code = a.list_code and r.numbered_above = a.item_number
),

classified as (
    select
        *,
        own_role is null
        and above_is_ensemble
        and coalesce(breaks_between, 0) = 0
        and not is_group as is_component
    from ensembles
)

select
    list_code,
    woj_code,
    page_number,
    item_number,
    powiat_text,
    district_text,
    locality_text,
    page_state_date,
    object_name,
    register_text,
    is_group,
    line_count,
    source_version,
    raw_sha256,
    max(page_state_date) over (partition by list_code) as list_state_date,
    case
        when own_role is not null then own_role
        when is_component then 'component'
        else 'unnumbered'
    end as register_role,
    case
        when own_role = 'numbered' then item_number
        when own_role = 'same_as_above' or is_component then numbered_above
    end as register_item_number
from classified
