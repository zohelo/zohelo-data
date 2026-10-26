{{ config(alias='build_dbw') }}
{# Gold PoC build stage: sampled GUS DBW blocks (indicator × cross-section × presentation ×
   periodicity; one Landing file per year) as observations in the common shape. Labels come
   from each file's dictionary. The territorial dimension of a block is the one whose
   positions match areas most often; the other dimensions are breakdowns. Measure =
   indicator × presentation. #}
with blocks as ({{ poc_dbw_blocks() }}),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'gus_dbw'
),

dict as (
    select
        entity_id,
        lower(nazwa_kolumny) as column_name,
        id_elementu as element_id,
        any_value(opis) as label
    from {{ source('gus_dbw', 'dbw_dictionary_items') }}
    -- activated files only
    where regexp_replace(entity_id, '_[0-9]{4}_pl$', '') in (select b.block_id from blocks as b)
    group by entity_id, lower(nazwa_kolumny), id_elementu
),

silver as (
    select
        o.*,
        regexp_replace(o.entity_id, '_[0-9]{4}_pl$', '') as block_id,
        row_number() over () as rid
    from {{ ref('poc_src_dbw') }} as o
    where
        o.value is not null
        and regexp_replace(o.entity_id, '_[0-9]{4}_pl$', '') in (select b.block_id from blocks as b)
),

exploded as (  -- one row per observation × used dimension
    select
        s.rid,
        s.entity_id,
        s.block_id,
        unnest([
            {% for k in range(1, 10) %}
                struct_pack(
                    k := {{ k }}, dim_id := s.dimension_{{ k }}_id, pos_id := s.position_{{ k }}_id
                ){% if not loop.last %}
                    ,
                {% endif %}
            {% endfor %}
        ]) as p
    from silver as s
),

fields as (
    select
        exploded.rid,
        exploded.entity_id,
        exploded.block_id,
        p.k,
        p.dim_id,
        p.pos_id
    from exploded
),

long as (
    select
        e.rid,
        e.entity_id,
        e.block_id,
        e.k,
        e.dim_id,
        e.pos_id,
        coalesce(w.label, 'dim' || e.k) as dim_name,
        d.label as pos_label,
        g.geography_key
    from fields as e
    left join dict as d
        on
            e.entity_id = d.entity_id
            and d.column_name = 'id_pozycja_' || e.k
            and d.element_id = cast(e.pos_id as varchar)
    left join dict as w
        on
            e.entity_id = w.entity_id
            and w.column_name = 'id_wymiar_' || e.k
            and w.element_id = cast(e.dim_id as varchar)
    left join geo_map as g on upper(trim(d.label)) = g.source_code and g.matched
    where e.pos_id is not null
),

territory_dim as (  -- per block, the dimension whose positions are areas
    select
        block_id,
        k
    from (
        select
            block_id,
            k,
            count(geography_key) as matched,
            count(*) as total
        from long
        group by block_id, k
    ) as counted
    qualify row_number() over (partition by block_id order by matched desc, k asc) = 1 and matched > 0
),

per_row as (
    select
        l.rid,
        max(case when l.k = t.k then coalesce(l.geography_key, '_X') end) as geography_key,
        max(case when l.k = t.k then l.pos_label end) as geography_source_code,
        cast(to_json(map(
            list(l.dim_name order by l.k) filter (where t.k is null or l.k <> t.k),
            list(l.pos_label order by l.k) filter (where t.k is null or l.k <> t.k)
        )) as varchar) as breakdown_json,
        bool_and(lower(l.pos_label) in ('ogółem', 'razem')) filter (where t.k is null or l.k <> t.k) as all_total,
        count(*) filter (where t.k is null or l.k <> t.k) as breakdowns,
        bool_or(lower(l.pos_label) = 'mężczyźni') as is_men,
        bool_or(lower(l.pos_label) = 'kobiety') as is_women
    from long as l
    left join territory_dim as t on l.block_id = t.block_id and l.k = t.k
    group by l.rid
),

shaped as (
    select
        s.*,
        b.block_name,
        b.freq_code,
        coalesce(i.label, 'indicator ' || s.indicator_id) as indicator_name,
        coalesce(m.label, 'presentation ' || s.presentation_id) as presentation_name,
        pe.label as period_name,
        r.geography_key,
        r.geography_source_code,
        r.breakdown_json,
        coalesce(r.all_total, true) as is_total,
        r.breakdowns,
        case when r.is_men then 'M' when r.is_women then 'F' end as sex_code,
        case
            when b.freq_code = 'A' then make_date(s.year, 1, 1)
            when b.freq_code = 'M' then
                make_date(
                    s.year,
                    case lower(trim(pe.label))
                        when 'styczeń' then 1 when 'luty' then 2 when 'marzec' then 3 when 'kwiecień' then 4
                        when 'maj' then 5 when 'czerwiec' then 6 when 'lipiec' then 7 when 'sierpień' then 8
                        when 'wrzesień' then 9 when 'październik' then 10 when 'listopad' then 11 when
                            'grudzień'
                            then 12
                        else try_cast(regexp_extract(pe.label, '([0-9]{1,2})', 1) as integer)
                    end,
                    1
                )
        end as date_key
    from silver as s
    inner join blocks as b on s.block_id = b.block_id
    left join per_row as r on s.rid = r.rid
    left join
        dict as i
        on s.entity_id = i.entity_id and i.column_name = 'id_zmienna' and i.element_id = cast(s.indicator_id as varchar)
    left join dict as m
        on
            s.entity_id = m.entity_id
            and m.column_name = 'id_sposob_prezentacji_miara'
            and m.element_id = cast(s.presentation_id as varchar)
    left join
        dict as pe
        on s.entity_id = pe.entity_id and pe.column_name = 'id_okres' and pe.element_id = cast(s.period_id as varchar)
)

select
    'gus_dbw' as source_system,
    block_id,
    block_name,
    'gus_dbw:' || block_id || ':' || presentation_id as measure_key,
    indicator_name || ' — ' || presentation_name as measure_name,
    cast(presentation_id as varchar) as measure_codes,
    presentation_name as unit_code,
    presentation_name as unit_name,
    geography_source_code,
    coalesce(geography_key, '_X') as geography_key,
    freq_code as frequency,
    date_key,
    coalesce(period_name || ' ', '') || year as period_label,
    coalesce(breakdown_json, '{}') as breakdown_json,
    coalesce(breakdown_json, '{}') as breakdown_label_json,
    sex_code,
    is_total,
    value,
    case
        when confidentiality_id is not null and confidentiality_id <> 0 then 'confidential'
        when flag_id is not null and flag_id <> 0 then 'flagged'
        else 'ok'
    end as observation_status_key,
    source_version,
    raw_sha256
from shaped
where date_key is not null
