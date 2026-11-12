{# nfz_contracts (register PLS-014): Gold registration, decision D34 (docs/source-registration.md). Block 'agreements'
   (models/gold/poc_build_nfz_contracts.sql). Areas: the 16 NFZ regional branches, one per voivodeship (branch codes
   01-16 in the alphabetical order of the voivodeships, as the API documents them: 01 Dolnośląski ... 16
   Zachodniopomorski), a static list matched to the TERYT voivodeship keys of dim_geography, so the map never waits
   for the source; 'all_branches' = Poland (zohelo's sum over the branches). #}
{% macro poc_source__nfz_contracts() %}
    {{ return({
        'block_model': 'poc_build_nfz_contracts',
        'blocks': [['agreements', 'NFZ contracts API: agreements by contract year and branch']],
    }) }}
{% endmacro %}

{% macro poc_geography__nfz_contracts() %}
nfz_contracts as (
    select
        'nfz_contracts' as source_system,
        b.branch_code as source_code,
        g.geography_key
    from (
        values
        ('01', '02'), ('02', '04'), ('03', '06'), ('04', '08'),
        ('05', '10'), ('06', '12'), ('07', '14'), ('08', '16'),
        ('09', '18'), ('10', '20'), ('11', '22'), ('12', '24'),
        ('13', '26'), ('14', '28'), ('15', '30'), ('16', '32')
    ) as b (branch_code, woj_code)
    inner join geo as g on g.level_name = 'wojewodztwo' and g.geography_key = 'TERC:' || b.woj_code
    union all
    select
        'nfz_contracts' as source_system,
        'all_branches' as source_code,
        'PL' as geography_key
)
{% endmacro %}
