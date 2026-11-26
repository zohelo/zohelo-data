{# GUS BDL geography codes (D34 registration, docs/source-registration.md): the API's unit ids
   (gus_bdl_api) and the web export's TERYT codes (gus_bdl_terc). #}
{% macro poc_geography__gus_bdl() %}
-- BDL: the web export keys rows by a 7-digit zero-padded TERYT code (0000000 Polska, WW00000
-- województwo, WWPP000 powiat, WWPPGGT gmina); the API by a 12-character unit id (bdl_units),
-- mapped here by level and name (v1: the GUS KTS–TERYT correspondence table).
gus_bdl_terc_woj as (
    select
        terc_code,
        upper(name_pl) as name_u
    from {{ ref('teryt_terc') }}
    where unit_level = 'wojewodztwo'
),

gus_bdl_terc_pow as (
    select
        terc_code,
        woj_code,
        lower(name_pl) as name_l,
        unit_type_pl
    from {{ ref('teryt_terc') }}
    where unit_level = 'powiat'
),

gus_bdl_terc_gmi as (
    select
        terc_code,
        woj_code,
        pow_code,
        lower(name_pl) as name_l,
        gmina_type_code
    from {{ ref('teryt_terc') }}
    where unit_level = 'gmina'
),

gus_bdl_units as (select * from {{ ref('bdl_units') }}),

gus_bdl_api as (
    select
        'gus_bdl' as source_system,
        u.unit_id as source_code,
        case
            when u.level = 0 then 'PL'
            when u.level = 1
                then
                    case
                        when upper(u.name) like '%MAZOWIECKIE%' then 'TERC:14'
                        when upper(u.name) like '%POŁUDNIOWO-ZACHODNI%' then 'NUTS:PL5'
                        when upper(u.name) like '%PÓŁNOCNO-ZACHODNI%' then 'NUTS:PL4'
                        when upper(u.name) like '%POŁUDNIOWY%' then 'NUTS:PL2'
                        when upper(u.name) like '%PÓŁNOCNY%' then 'NUTS:PL6'
                        when upper(u.name) like '%CENTRALNY%' then 'NUTS:PL7'
                        when upper(u.name) like '%WSCHODNI%' then 'NUTS:PL8'
                        else '_X'
                    end
            when u.level = 2 then coalesce('TERC:' || w.terc_code, '_X')
            when u.level = 3 and upper(u.name) like '%WARSZAWSKI STO%' then 'NUTS:PL91'
            when u.level = 3 and upper(u.name) like '%MAZOWIECKI REGIONALNY%' then 'NUTS:PL92'
            when u.level = 3 then coalesce('TERC:' || w3.terc_code, '_X')
            when u.level = 4 then 'BDL:' || u.unit_id
            when u.level = 5 then coalesce('TERC:' || p.terc_code, '_X')
            when u.level = 6 then coalesce('TERC:' || g.terc_code, '_X')
            else '_X'
        end as geography_key
    from gus_bdl_units as u
    left join gus_bdl_terc_woj as w on w.name_u = upper(u.name) and u.level = 2
    left join gus_bdl_terc_woj as w3 on w3.name_u = upper(regexp_replace(u.name, '^(?i)region ', '')) and u.level = 3
    left join gus_bdl_terc_pow as p
        on
            u.level = 5
            and p.woj_code = substr(u.unit_id, 3, 2)
            and p.name_l = lower(regexp_replace(u.name, '^(?i)powiat (m\.\s*st\.\s*|m\.\s*)?', ''))
    left join gus_bdl_terc_gmi as g
        on
            u.level = 6
            and g.woj_code = substr(u.unit_id, 3, 2)
            and g.name_l
            = lower(
                regexp_replace(u.name, '\s*\((miasto|obszar wiejski|gmina (miejska|wiejska|miejsko-wiejska))\)$', '')
            )
            and g.gmina_type_code = substr(u.unit_id, 12, 1)
),

gus_bdl_type_digits as (select unnest(['1', '2', '3']) as digit),

gus_bdl_terc as (  -- the 7-digit zero-padded codes of the web export, generated from TERYT
    select
        'gus_bdl' as source_system,
        rpad(terc_code, 7, '0') as source_code,
        'TERC:' || terc_code as geography_key
    from {{ ref('teryt_terc') }}
    union all
    select
        'gus_bdl' as source_system,
        '0000000' as source_code,
        'PL' as geography_key
    union all
    -- a gmina whose type digit changed since (2 → 3 when a village got town rights): the current unit
    select
        'gus_bdl' as source_system,
        substr(g.terc_code, 1, 6) || t.digit as source_code,
        'TERC:' || g.terc_code as geography_key
    from {{ ref('teryt_terc') }} as g
    cross join gus_bdl_type_digits as t
    where g.unit_level = 'gmina' and g.gmina_type_code in ('1', '2', '3') and t.digit <> g.gmina_type_code
),

gus_bdl as (
    select * from gus_bdl_api
    union all
    select * from gus_bdl_terc
)
{% endmacro %}
