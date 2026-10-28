{{ config(alias='build_eurostat') }}
{# Gold PoC build stage: the sampled Eurostat blocks as observations in the common shape
   (measure, geography, date, breakdown, value). One row per Silver row kept by the sample
   filters; in v1 this is the per-block builder. Measure = dataset × unit × the block's
   measure-defining dimensions; everything else in the series key is a breakdown. #}
with blocks as ({{ poc_eurostat_blocks() }}),

mdims as (
    select
        dataset_id,
        list(dimension_id) as measure_dims
    from ({{ poc_eurostat_measure_dims() }})
    group by dataset_id
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'eurostat'
),

flows as (
    select
        dataset_id,
        name_en
    from {{ ref('eurostat_dataflows') }}
),

dims as (
    select
        dataset_id,
        dimension_id,
        codelist_id
    from {{ ref('eurostat_dimensions') }}
),

codes as (
    select
        codelist_id,
        code,
        any_value(name_en) as name_en
    from {{ ref('eurostat_codes') }}
    group by codelist_id, code
),

silver as (
    select
        o.*,
        b.block_name,
        b.total_breakdown,
        coalesce(m.measure_dims, []) as measure_dims,
        string_split(o.breakdown_dimensions, ',') as dim_list,
        string_split(o.breakdown_codes, ',') as code_list
    from {{ ref('poc_src_eurostat') }} as o
    inner join blocks as b on upper(o.dataset_id) = b.dataset_id and o.freq_code = b.freq_code
    left join mdims as m on upper(o.dataset_id) = m.dataset_id
    where o.value is not null
),

sampled as (
    select
        *,
        code_list[list_position(dim_list, 'age')] as age_code,
        code_list[list_position(dim_list, 'sex')] as sex_code_raw,
        code_list[list_position(dim_list, 's_adj')] as s_adj_code,
        code_list[list_position(dim_list, 'na_item')] as na_item_code,
        code_list[list_position(dim_list, 'coicop')] as coicop_code,
        code_list[list_position(dim_list, 'indic_em')] as indic_em_code
    from silver
),

kept as (  -- the PoC sample: small, with breakdowns where they matter (sex), one unit per concept
    select *
    from sampled
    where
        case upper(dataset_id)
            when 'DEMO_PJAN' then age_code = 'TOTAL'
            when 'DEMO_R_PJANAGGR3' then age_code = 'TOTAL' and length(geo_code) <= 4
            when 'DEMO_GIND' then true
            when 'NAMA_10_GDP' then unit_code in ('CP_MEUR', 'CLV_PCH_PRE') and na_item_code = 'B1GQ'
            when 'NAMA_10_PC' then unit_code = 'CP_EUR_HAB' and na_item_code = 'B1GQ'
            when 'UNE_RT_A' then unit_code = 'PC_ACT' and age_code = 'Y15-74'
            when 'UNE_RT_M' then unit_code = 'PC_ACT' and age_code = 'Y15-74'
            when 'PRC_HICP_MANR' then unit_code = 'RCH_A' and coicop_code = 'CP00'
            when 'LFSI_EMP_A' then unit_code = 'PC_POP' and age_code = 'Y20-64' and indic_em_code = 'EMP_LFS'
            else false
        end
),

split as (
    select
        *,
        list_filter(list_zip(dim_list, code_list), lambda x: list_contains(measure_dims, x[1])) as measure_pairs,
        list_filter(list_zip(dim_list, code_list), lambda x: not list_contains(measure_dims, x[1])) as breakdown_pairs
    from kept
),

shaped as (
    select
        s.*,
        array_to_string(list_transform(s.measure_pairs, lambda x: x[2]), ':') as measure_codes,  -- noqa: RF02
        cast(
            to_json(
                map(
                    list_transform(s.breakdown_pairs, lambda x: x[1]),  -- noqa: RF02
                    list_transform(s.breakdown_pairs, lambda x: x[2])  -- noqa: RF02
                )
            ) as varchar
        )
            as breakdown_json,
        case
            when s.total_breakdown = '' then len(s.breakdown_pairs) = 0
            else
                list_sort(list_transform(s.breakdown_pairs, lambda x: x[2]))  -- noqa: RF02
                = list_sort(string_split(s.total_breakdown, ','))
        end as is_total,
        case s.freq_code
            when 'A' then make_date(cast(s.time_period as integer), 1, 1)
            when 'M' then cast(s.time_period || '-01' as date)
            when
                'Q'
                then
                    make_date(
                        cast(substr(s.time_period, 1, 4) as integer),
                        (cast(substr(s.time_period, 7, 1) as integer) - 1) * 3 + 1,
                        1
                    )
            when
                'S'
                then
                    make_date(
                        cast(substr(s.time_period, 1, 4) as integer),
                        (cast(substr(s.time_period, 7, 1) as integer) - 1) * 6 + 1,
                        1
                    )
            when 'D' then cast(s.time_period as date)
        end as date_key,
        coalesce(cu.name_en, s.unit_code, 'as named by the measure') as unit_name
    from split as s
    left join codes as cu on cu.codelist_id = 'UNIT' and s.unit_code = cu.code
),

measure_names as (  -- names of the measure-defining codes, from the dataset's code lists
    select
        p.dataset_id,
        p.measure_codes,
        string_agg(coalesce(c.name_en, p.pair[2]), ' — ' order by p.pos) as codes_name
    from (
        select distinct
            dataset_id,
            measure_codes,
            unnest(measure_pairs) as pair,
            generate_subscripts(measure_pairs, 1) as pos
        from shaped
    ) as p
    left join dims as d on upper(p.dataset_id) = upper(d.dataset_id) and d.dimension_id = p.pair[1]
    left join codes as c on d.codelist_id = c.codelist_id and c.code = p.pair[2]
    group by p.dataset_id, p.measure_codes
)

select
    'eurostat' as source_system,
    upper(s.dataset_id) as block_id,
    s.block_name,
    'eurostat:'
    || upper(s.dataset_id)
    || ':'
    || coalesce(s.unit_code, '_Z')
    || case when s.measure_codes = '' then '' else ':' || s.measure_codes end
        as measure_key,
    coalesce(f.name_en, s.block_name) || ' — ' || s.unit_name || coalesce(' — ' || n.codes_name, '') as measure_name,
    s.measure_codes,
    coalesce(s.unit_code, '_Z') as unit_code,
    coalesce(s.unit_name, 'as named by the measure') as unit_name,
    s.geo_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    s.freq_code as frequency,
    s.date_key,
    s.time_period as period_label,
    s.breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    case s.sex_code_raw when 'F' then 'F' when 'M' then 'M' when 'T' then 'T' end as sex_code,
    s.is_total,
    s.value,
    case substr(nullif(trim(s.status_code), ''), 1, 1)
        when 'p' then 'provisional' when 'e' then 'estimated' when 'c' then 'confidential' when 'b' then 'break'
        when 'u' then 'low_reliability' when 'd' then 'definition_differs' when 'f' then 'forecast'
        when 's' then 'estimated' when 'n' then 'not_significant' else 'ok'
    end as observation_status_key,
    s.source_version,
    s.raw_sha256
from shaped as s
left join geo_map as g on s.geo_code = g.source_code
left join flows as f on upper(s.dataset_id) = upper(f.dataset_id)
left join measure_names as n on s.dataset_id = n.dataset_id and s.measure_codes = n.measure_codes
where s.date_key is not null
