{# Eurostat geography codes (decision D34, docs/source-registration.md): the codes of Eurostat's GEO code
   list, mapped by code. The curated Gold block of Eurostat (poc_build_eurostat) is older than D34 and stays in
   the lists of macros/poc_blocks.sql; the per-block full coverage (warehouse/observations.py, D35) reads this
   map through gold.map_geography.

   Mapped: EL and UK to their ISO codes; Polish NUTS 1 and NUTS 2 through poc_nuts_pl() (a NUTS 2 region that
   is a voivodeship maps to its TERC key); countries; NUTS regions of other countries; Eurostat aggregates.

   Polish NUTS 3 (PLxxx, 73 podregions): dim_geography keys a podregion by its GUS BDL unit id (`BDL:<id>`), which
   carries no NUTS code, and no table we load holds the NUTS–KTS correspondence. So a code is mapped by Eurostat's
   own name for it to the podregion of the same name in the voivodeship of the code's NUTS 2 parent (the first
   four characters, through poc_nuts_pl()). Names are compared in upper case without the prefixes `PODREGION`,
   `MIASTO`, `M.` and `M. ST.`, the rule the GUS DBW map uses for the same areas. A code that matches no
   podregion or more than one stays unmatched (`_X`) and is counted by the build check. #}
{% macro poc_geography__eurostat() %}
eurostat_nuts_pl as ({{ poc_nuts_pl() }}),

eurostat_codes as (
    select code from {{ ref('eurostat_codes') }}
    where codelist_id = 'GEO'
    group by code
),

eurostat_names as (  -- a code can carry one name per code list version
    select
        code,
        regexp_replace(upper(trim(name_en)), '^(M\. ST\. |M\. |MIASTO )', '') as name_u
    from {{ ref('eurostat_codes') }}
    where codelist_id = 'GEO' and regexp_matches(code, '^PL[0-9A-Z]{3}$') and name_en is not null
    group by all
),

eurostat_podregion as (
    select
        c.code,
        min(g.geography_key) as geography_key
    from eurostat_names as c
    inner join eurostat_nuts_pl as n on substr(c.code, 1, 4) = n.nuts_code and n.woj_code is not null
    inner join geo as g
        on
            g.level_name = 'podregion'
            and g.parent_key = 'TERC:' || n.woj_code
            and regexp_replace(regexp_replace(upper(g.name_pl), '^PODREGION ', ''), '^(M\. ST\. |M\. |MIASTO )', '')
            = c.name_u
    group by c.code
    having count(distinct g.geography_key) = 1
),

eurostat as (
    select
        'eurostat' as source_system,
        c.code as source_code,
        coalesce(
            case when c.code in ('EL', 'UK') then (case c.code when 'EL' then 'GR' else 'GB' end) end,
            case when n.woj_code is not null and n.level_name = 'wojewodztwo' then 'TERC:' || n.woj_code end,
            case when n.nuts_code is not null then 'NUTS:' || n.nuts_code end,
            g_country.geography_key,
            g_nuts.geography_key,
            g_agg.geography_key,
            p.geography_key,
            '_X'
        ) as geography_key
    from eurostat_codes as c
    left join eurostat_nuts_pl as n on c.code = n.nuts_code
    left join geo as g_country on c.code = g_country.geography_key and g_country.level_name = 'country'
    left join geo as g_nuts on g_nuts.geography_key = 'NUTS:' || c.code
    left join geo as g_agg on g_agg.geography_key = 'EUROSTAT:' || c.code
    left join eurostat_podregion as p on c.code = p.code
)
{% endmacro %}
