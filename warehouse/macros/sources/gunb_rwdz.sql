{# gunb_rwdz (register PL-ENV-033): Gold registration, decision D34 (docs/source-registration.md). Block 'permits'
   (models/gold/poc_build_gunb_rwdz.sql): monthly counts of building-permit applications and decisions of the GUNB
   register (RWDZ). Areas are TERYT codes as the register publishes them: 'PL' for Poland, the 2-digit voivodeship
   code and the 4-digit powiat code, matched to the TERYT units of dim_geography. #}
{% macro poc_source__gunb_rwdz() %}
    {{ return({'block_model': 'poc_build_gunb_rwdz', 'blocks': [['permits', 'GUNB RWDZ building-permit applications and decisions']]}) }}
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
