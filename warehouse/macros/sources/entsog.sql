{# entsog (register PL-ENV-021): Gold registration, decision D34 (docs/source-registration.md). One block
   'physical_flow' (models/gold/poc_build_entsog.sql): GAZ-SYSTEM daily physical flows by point group and the totals,
   Poland (operator PL-TSO-0002 → PL). #}
{% macro poc_source__entsog() %}
    {{ return({'block_model': 'poc_build_entsog', 'blocks': [['physical_flow', 'ENTSOG TP physical gas flows, GAZ-SYSTEM (daily; monthly sums)']]}) }}
{% endmacro %}

{# Point groups: a reviewed static list keyed by the platform's pointKey, one group per point (both directions).
   Built from the points GAZ-SYSTEM published in 2021-2026 with the API's own pointType and the adjacent country of
   /api/v1/operatorpointdirections (Dev probe 43320099570). A point that publishes flows and is not listed here goes
   to group 'unmapped' in Gold and is reported by tests/entsog_point_groups.sql (warning); it still counts in the
   entry and exit totals, which sum every point of the direction. #}
{% macro poc_entsog_point_groups() %}
    select * from (
        values
        ('LNG-00006', 'lng_terminal', 'LNG terminal (Świnoujście)'),
        ('ITP-10009', 'baltic_pipe', 'Baltic Pipe (Faxe, DK)'),
        ('ITP-00497', 'interconnector_de', 'Interconnector Germany'),
        ('ITP-00158', 'interconnector_cz', 'Interconnector Czechia'),
        ('ITP-00177', 'interconnector_sk', 'Interconnector Slovakia'),
        ('ITP-00556', 'interconnector_lt', 'Interconnector Lithuania (GIPL)'),
        ('ITP-10008', 'interconnector_ua', 'Interconnector Ukraine'),
        ('ITP-00293', 'yamal_system', 'Yamal transit system (point of interconnection PWP)'),
        ('DIS-00195', 'cross_border_distribution', 'Cross-border distribution points (CZ)'),
        ('DIS-00204', 'cross_border_distribution', 'Cross-border distribution points (CZ)'),
        ('PRD-00153', 'domestic_production', 'Domestic gas fields'),
        ('PRD-00218', 'domestic_production', 'Domestic gas fields'),
        ('UGS-00322', 'storage', 'Storage'),
        ('UGS-00424', 'storage', 'Storage'),
        ('UGS-00425', 'storage', 'Storage'),
        ('UGS-00426', 'storage', 'Storage'),
        ('FNC-00007', 'conversion', 'H-gas/L-gas conversion (nitrogen removal, gas mixing)'),
        ('FNC-00008', 'conversion', 'H-gas/L-gas conversion (nitrogen removal, gas mixing)'),
        ('PRD-00219', 'conversion', 'H-gas/L-gas conversion (nitrogen removal, gas mixing)'),
        ('DIS-00013', 'distribution', 'Distribution systems'),
        ('DIS-00193', 'distribution', 'Distribution systems'),
        ('FNC-00002', 'final_consumers', 'Final consumers connected to transmission'),
        ('FNC-00040', 'final_consumers', 'Final consumers connected to transmission')
    ) as t (point_key, group_key, group_label)
{% endmacro %}

{% macro poc_geography__entsog() %}
-- ENTSOG: the operator key is the area code (one operator per country today; GAZ-SYSTEM = Poland, no regional grain).
entsog as (
    select
        'entsog' as source_system,
        o.operator_key as source_code,
        g.geography_key
    from (values ('PL-TSO-0002', 'PL')) as o (operator_key, iso2)
    inner join geo as g on g.iso2 = o.iso2 and g.level_name = 'country'
)
{% endmacro %}
