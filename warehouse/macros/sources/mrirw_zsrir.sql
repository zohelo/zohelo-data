{# mrirw_zsrir (register PLS-032): Gold registration, decision D34 (docs/source-registration.md). Block
   'purchase_prices' (models/gold/poc_build_mrirw_zsrir.sql); the bulletins' monthly sheets are national, 'PL' = Poland.
   Static: the map never waits for this source. #}
{% macro poc_source__mrirw_zsrir() %}
    {{ return({'block_model': 'poc_build_mrirw_zsrir', 'blocks': [['purchase_prices', 'MRiRW ZSRIR monthly purchase prices (cereals, raw milk, pigs)']]}) }}
{% endmacro %}

{% macro poc_geography__mrirw_zsrir() %}
mrirw_zsrir as (
    select
        'mrirw_zsrir' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
