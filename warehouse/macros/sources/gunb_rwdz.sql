{# gunb_rwdz (register PL-ENV-033): Gold registration, decision D34 (docs/source-registration.md). Block 'permits'
   (models/gold/poc_build_gunb_rwdz.sql): building-permit decisions per month of the GUNB register (RWDZ). Areas are
   'PL' for Poland, the 2-digit TERC code of the voivodeship of the file and the 4-digit TERC code of the powiat of
   the record's TERYT code, matched to the TERYT units of dim_geography (a static rule, no Silver table read). #}
{% macro poc_source__gunb_rwdz() %}
    {{ return({'block_model': 'poc_build_gunb_rwdz', 'blocks': [['permits', 'GUNB RWDZ building-permit decisions']]}) }}
{% endmacro %}

{% macro poc_geography__gunb_rwdz() %}
gunb_rwdz as (
    select
        'gunb_rwdz' as source_system,
        g.terc_code as source_code,
        g.geography_key
    from geo as g
    where g.level_name in ('wojewodztwo', 'powiat')
    union all
    select
        'gunb_rwdz' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
