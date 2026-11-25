{{ config(severity='warn') }}
-- Warn, not error, while #3196 is open: one source's data check must not fail every source's build.
-- Open KRS API (#1362): every record of the newest Bronze copy of each part (extracts_00 … extracts_15) is one
-- Silver row, and every current extract has its register and name. A part whose rows Silver no longer takes, or
-- an answer layout whose header fields stop matching the allowlist paths (both would drop entities silently),
-- returns the part with the counts.
with bronze as (
    select
        entity,
        count(*) as bronze_rows,
        count(*) filter (where http_status = '200') as bronze_extracts
    from {{ source('ms_krs', 'ms_krs_extracts') }}
    group by entity, source_version, ingested_at
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

silver as (
    select
        entity,
        count(*) as silver_rows,
        count(*) filter (where has_current_extract and register is not null and name is not null) as silver_extracts
    from {{ ref('ms_krs_entities') }}
    group by entity
)

select
    b.entity,
    b.bronze_rows,
    coalesce(s.silver_rows, 0) as silver_rows,
    b.bronze_extracts,
    coalesce(s.silver_extracts, 0) as silver_extracts
from bronze as b
left join silver as s on b.entity = s.entity
where b.bronze_rows <> coalesce(s.silver_rows, 0) or b.bronze_extracts <> coalesce(s.silver_extracts, 0)
