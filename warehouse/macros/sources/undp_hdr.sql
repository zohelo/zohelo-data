{# UNDP Human Development Report measures built (gold.build_undp_hdr, #2861): source indicator (column name
   without the year) → measure, sex and unit. The file has no unit column: units are those of the HDR technical
   notes. Not built: the Gender Development Index (gdi, a female-to-male ratio), ranks and groups, the
   planetary-pressures indices, the inequality components, the Gender Inequality Index components (abr, mmr,
   pr_*, se_*, lfpr_*), co2_prod, mf and pop_total. #}
{% macro poc_undp_hdr_measures() %}
    select * from (
        values
        ('hdi', 'hdi', 'T', 'index', 'index (0 to 1)', 'Human Development Index (HDI)'),
        ('hdi_f', 'hdi', 'F', 'index', 'index (0 to 1)', 'Human Development Index (HDI)'),
        ('hdi_m', 'hdi', 'M', 'index', 'index (0 to 1)', 'Human Development Index (HDI)'),
        ('le', 'le', 'T', 'years', 'years', 'Life expectancy at birth'),
        ('le_f', 'le', 'F', 'years', 'years', 'Life expectancy at birth'),
        ('le_m', 'le', 'M', 'years', 'years', 'Life expectancy at birth'),
        ('eys', 'eys', 'T', 'years', 'years', 'Expected years of schooling'),
        ('eys_f', 'eys', 'F', 'years', 'years', 'Expected years of schooling'),
        ('eys_m', 'eys', 'M', 'years', 'years', 'Expected years of schooling'),
        ('mys', 'mys', 'T', 'years', 'years', 'Mean years of schooling'),
        ('mys_f', 'mys', 'F', 'years', 'years', 'Mean years of schooling'),
        ('mys_m', 'mys', 'M', 'years', 'years', 'Mean years of schooling'),
        ('gnipc', 'gnipc', 'T', '2021 PPP $', '2021 PPP $ per person', 'Gross national income per capita'),
        ('gni_pc_f', 'gnipc', 'F', '2021 PPP $', '2021 PPP $ per person', 'Gross national income per capita'),
        ('gni_pc_m', 'gnipc', 'M', '2021 PPP $', '2021 PPP $ per person', 'Gross national income per capita'),
        ('ihdi', 'ihdi', '', 'index', 'index (0 to 1)', 'Inequality-adjusted Human Development Index (IHDI)'),
        ('gii', 'gii', '', 'index', 'index (0 to 1)', 'Gender Inequality Index (GII)')
    ) as t (indicator, measure, sex_code, unit_code, unit_name, measure_name)
{% endmacro %}

{# D34 registration (docs/source-registration.md): the Gold block and the geography of undp_hdr (#2861). #}
{% macro poc_source__undp_hdr() %}
    {{ return({
        'block_model': 'poc_build_undp_hdr',
        'blocks': [['composite_indices', 'UNDP Human Development Report composite indices, complete time series']],
    }) }}
{% endmacro %}

{% macro poc_geography__undp_hdr() %}
-- UNDP HDR: countries by ISO 3166 alpha-3 (dim_geography.iso3), from Gold's own list; ZZK.WORLD is World. The HDR
-- groups and regions (ZZA … ZZJ) are not listed; the build stage builds the EU27 members and World.
undp_hdr as (
    select
        'undp_hdr' as source_system,
        g.iso3 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 is not null
    union all
    select
        'undp_hdr' as source_system,
        'ZZK.WORLD' as source_code,
        'WB:WLD' as geography_key
)
{% endmacro %}
