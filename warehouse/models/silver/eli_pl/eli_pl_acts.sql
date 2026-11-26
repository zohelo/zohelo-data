{# ELI Poland (register PLG-026): acts of Dziennik Ustaw (DU) and Monitor Polski (MP), metadata only.
   Grain: eli_id (journal / year / [issue,] position: DU/2025/1900, MP/1946/0910170 = issue 91, position 170;
   also unique: publisher x year x volume x position, not publisher x year x position before 2012). Source:
   bronze.eli_pl_acts (one answer per journal year), with the number of acts /eli/acts lists for the journal
   (acts_listed, for the completeness test). Title, type and status as published (Polish labels of the API);
   promulgation_date = the date of publication in the journal, announcement_date = the date of the act. An empty
   date stays empty; any other text that is not a date fails the build. #}
with acts as (
    select
        eli_id,
        address,
        publisher,
        cast(year as integer) as year,
        cast(volume as integer) as volume,
        cast(position as integer) as position,
        display_address,
        title,
        act_type,
        status,
        cast(nullif(promulgation, '') as date) as promulgation_date,
        cast(nullif(announcement_date, '') as date) as announcement_date,
        cast(nullif(change_date, '') as timestamp) as changed_at,
        text_pdf = 'true' as has_text_pdf,
        text_html = 'true' as has_text_html,
        source_version,
        raw_sha256
    from {{ source('eli_pl', 'eli_pl_acts') }}
),

publishers as (
    select
        publisher,
        cast(acts_count as integer) as acts_listed
    from {{ source('eli_pl', 'eli_pl_publishers') }}
)

select
    a.*,
    p.acts_listed
from acts as a
left join publishers as p on a.publisher = p.publisher
