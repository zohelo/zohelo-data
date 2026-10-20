-- Silver: GUS BDL observations of one subgroup, typed. Applied by the loader to ONE Bronze file
-- (all parts of the subgroup's newest complete download); `bronze` is that file, `variables` the
-- subgroup's rows of silver.bdl_variables. The result replaces the subgroup's partition in
-- silver.bdl_observations (design #875).
-- One row per unit_code x dimension_values x year. Parts of one download can overlap (their
-- territory selections share units); the rule, from the 20 Oct probe (#868, #875):
--   identical rows collapse; an empty value gives way to a published one;
--   two different published values for one key are a conflict: value_conflicts counts them
--   and the loader rejects the subgroup (listed for the owner, never silently collapsed).
with typed as (
    select
        b.*,
        nullif(trim(b.value_text), '') as published,
        try_cast(b.year as integer) as year_int
    from bronze as b
),

ranked as (
    select
        *,
        count(distinct published) over (partition by unit_code, dimension_values, year_int) as published_values,
        row_number() over (
            partition by unit_code, dimension_values, year_int
            order by published is null, part_index, source_row_number
        ) as pick
    from typed
)

select
    r.subgroup_id,
    v.variable_id,
    r.unit_code,
    case length(r.unit_code) when 12 then 'BDL' when 7 then 'TERC' end as unit_code_scheme,
    r.unit_name,
    r.year_int as year,
    try_cast(replace(r.published, ',', '.') as double) as value,
    r.value_text,
    r.measure_unit,
    r.attribute,
    r.dimension_values,
    r.channel,
    r.source_version,
    r.raw_sha256,
    r.published_values > 1 as value_conflict
from ranked as r
left join variables as v
    on r.dimension_values = v.dimension_key
where r.pick = 1
