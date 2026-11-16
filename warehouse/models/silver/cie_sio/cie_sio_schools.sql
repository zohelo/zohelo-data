{# SIO schools and pupils (register PLS-022): the tidy workbook of the school year 2018/2019 (entity
   szkoly_i_uczniowie_wg_organow_prowadzacych_rokszkolny_20182019, data as of 30 September 2018). One row per
   voivodeship x school type x pupil category x publicness x governing body; the earlier years are cross-tabs per
   voivodeship sheet without totals and are not modelled yet (docs/sources/cie_sio.md). Columns are found by their
   header text in the row that holds 'Id WojGUS'; every other header is a measure, and a header this model does
   not know becomes measure_code 'unknown:<header>' (accepted_values fails). No Poland row is published and none is
   derived here. Grain: period_year x area_code x school_type_code x pupil_category_code x publicness x
   governing_body x measure_code. Values as published; an empty cell is no row. #}
with cells as (
    select
        entity,
        sheet_index,
        row_number as r,
        column_number as c,
        trim(regexp_replace(value_text, '\s+', ' ', 'g')) as v,
        raw_sha256,
        source_version
    from ({{ xlsx_newest_cells(source('cie_sio', 'cie_sio_cells')) }})
    where entity like 'szkoly_i_uczniowie_wg_organow_prowadzacych_rokszkolny%'
),

header_rows as (
    select
        entity,
        sheet_index,
        min(r) as header_row
    from cells
    where v = 'Id WojGUS'
    group by all
),

headers as (
    select
        h.entity,
        h.sheet_index,
        h.header_row,
        c.c,
        c.v as label
    from header_rows as h
    inner join cells as c on h.entity = c.entity and h.sheet_index = c.sheet_index and h.header_row = c.r
),

body as (
    select
        d.entity,
        d.sheet_index,
        d.r,
        h.label,
        d.v,
        d.raw_sha256,
        d.source_version
    from cells as d
    inner join headers as h on d.entity = h.entity and d.sheet_index = h.sheet_index and d.c = h.c
    where d.r > h.header_row
),

keys as (
    select
        entity,
        sheet_index,
        r,
        lpad(max(v) filter (where label = 'Id WojGUS'), 2, '0') as area_code,
        max(v) filter (where label = 'Województwo') as area_name,
        lpad(max(v) filter (where label = 'Id TypJedn'), 5, '0') as school_type_code,
        max(v) filter (where label = 'Typ jednostki') as school_type_name,
        max(v) filter (where label = 'Kategoria uczniów') as pupil_category,
        max(v) filter (where label = 'Publiczność') as publicness,
        max(v) filter (where label = 'Organ prowadzący') as governing_body
    from body
    group by all
)

select
    b.entity,
    cast(regexp_extract(b.entity, '(\d{4})\d{4}$', 1) as integer) as period_year,
    k.area_code,
    k.area_name,
    k.school_type_code,
    k.school_type_name,
    {{ poc_cie_sio_category('k.pupil_category') }} as pupil_category_code,
    k.pupil_category,
    k.publicness,
    k.governing_body,
    case b.label
        when 'Liczba szkół/placówek' then 'schools'
        when 'Liczba uczniów, wychow., słuchaczy' then 'pupils'
        when 'w tym w oddziałach przedszkolnych' then 'pupils_preschool_sections'
        when 'w tym w oddziałach innego typu' then 'pupils_other_type_sections'
        else 'unknown:' || b.label
    end as measure_code,
    b.label as measure_label,
    case b.label when 'Liczba szkół/placówek' then 'schools' else 'persons' end as unit_code,
    b.v as value_text,
    try_cast(b.v as double) as value,
    b.source_version,
    b.raw_sha256
from body as b
inner join keys as k on b.entity = k.entity and b.sheet_index = k.sheet_index and b.r = k.r
where b.label not in (
    'Id WojGUS', 'Województwo', 'Id TypJedn', 'Typ jednostki', 'Kategoria uczniów', 'Publiczność', 'Organ prowadzący'
)
