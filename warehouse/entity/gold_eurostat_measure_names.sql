-- Gold build stage of one Eurostat dataset, step 2 (docs/gold-coverage.md): the names of the dataset's
-- measures, one row per frequency × unit × measure codes, from the dataset's code lists. Kept as the
-- table `measure_names` for the last step. Measures are few whatever the number of series.
-- Relations: silver, dimension_sets (step 1), dims and codes (silver.eurostat_dimensions / eurostat_codes).
with measure_shapes as (
    select distinct
        p.freq_code,
        p.unit_code,
        d.measure_dimension_ids,
        coalesce(
            array_to_string(
                list_transform(
                    d.measure_positions, lambda i: string_split(p.breakdown_codes, ',')[i]  -- noqa: RF02
                ),
                ':'
            ),
            ''
        ) as measure_codes
    from silver as p
    inner join dimension_sets as d on p.breakdown_dimensions = d.breakdown_dimensions
    where p.value is not null and len(d.measure_positions) > 0
),

pairs as (
    select
        m.freq_code,
        m.unit_code,
        m.measure_codes,
        unnest(m.measure_dimension_ids) as dimension_id,
        unnest(string_split(m.measure_codes, ':')) as code,
        generate_subscripts(m.measure_dimension_ids, 1) as pos
    from measure_shapes as m
),

code_names as (
    select
        codelist_id,
        code,
        any_value(name_en) as name_en
    from codes
    group by codelist_id, code
),

dimension_lists as (
    select
        dimension_id,
        any_value(codelist_id) as codelist_id
    from dims
    where upper(dataset_id) in (select distinct upper(s.dataset_id) from silver as s)
    group by dimension_id
)

select
    p.freq_code,
    p.unit_code,
    p.measure_codes,
    string_agg(coalesce(n.name_en, p.code), ' — ' order by p.pos) as codes_name
from pairs as p
left join dimension_lists as l on p.dimension_id = l.dimension_id
left join code_names as n on l.codelist_id = n.codelist_id and p.code = n.code
group by p.freq_code, p.unit_code, p.measure_codes
