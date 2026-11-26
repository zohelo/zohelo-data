{# arimr_payments (register PLS-034): Gold registration, decision D34 (docs/source-registration.md). Block 'direct_payments'
   (models/gold/poc_build_arimr_payments.sql); areas by name as printed (lower case, spaced hyphens closed), matched to the
   TERYT voivodeship names of dim_geography, 'polska' (the reports' "Razem") = Poland. #}
{% macro poc_source__arimr_payments() %}
    {{ return({'block_model': 'poc_build_arimr_payments', 'blocks': [['direct_payments', 'ARiMR direct payments (applications, decisions, amounts paid by campaign)']]}) }}
{% endmacro %}

{% macro poc_geography__arimr_payments() %}
arimr_payments as (
    select
        'arimr_payments' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'arimr_payments' as source_system,
        'polska' as source_code,
        'PL' as geography_key
)
{% endmacro %}
