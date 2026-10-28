{# Gold PoC activation lists (design draft 2, "activation.yaml" in v1): which blocks of each
   source the PoC builds, how a block's codes split into measure and breakdown, and the
   cross-source concepts. Natural string keys everywhere so the model reads without lookups. #}

{% macro poc_eurostat_blocks() %}
    select * from (
        values
        ('DEMO_PJAN', 'Population on 1 January', 'A', 'TOTAL,T'),
        ('DEMO_R_PJANAGGR3', 'Population on 1 January by NUTS 3 region', 'A', 'TOTAL,T'),
        ('DEMO_GIND', 'Population change: demographic balance and crude rates', 'A', ''),
        ('NAMA_10_GDP', 'GDP and main components', 'A', ''),
        ('NAMA_10_PC', 'Main GDP aggregates per capita', 'A', ''),
        ('UNE_RT_A', 'Unemployment by sex and age, annual', 'A', 'Y15-74,T'),
        ('UNE_RT_M', 'Unemployment by sex and age, monthly', 'M', 'Y15-74,SA,T'),
        ('PRC_HICP_MANR', 'HICP, monthly annual rate of change', 'M', 'CP00'),
        ('LFSI_EMP_A', 'Employment and activity by sex and age, annual', 'A', 'Y20-64,T')
    ) as t (dataset_id, block_name, freq_code, total_breakdown)
{% endmacro %}

{# Eurostat dimensions that define the measure (with unit); every other dimension is a breakdown. #}
{% macro poc_eurostat_measure_dims() %}
    select * from (
        values
        ('DEMO_GIND', 'indic_de'),
        ('NAMA_10_GDP', 'na_item'),
        ('NAMA_10_PC', 'na_item'),
        ('LFSI_EMP_A', 'indic_em')
    ) as t (dataset_id, dimension_id)
{% endmacro %}

{% macro poc_bdl_blocks() %}
    select * from (
        values
        ('P2137', 'Ludność wg grup wieku i płci', 'LUDNOŚĆ / STAN LUDNOŚCI'),
        ('P1873', 'Ruch naturalny wg płci', 'LUDNOŚĆ / URODZENIA I ZGONY'),
        ('P2425', 'Gęstość zaludnienia oraz wskaźniki', 'LUDNOŚĆ / STAN LUDNOŚCI'),
        ('P2392', 'Stopa bezrobocia rejestrowanego', 'RYNEK PRACY / BEZROBOCIE REJESTROWANE'),
        ('P2497', 'Przeciętne miesięczne wynagrodzenia brutto', 'WYNAGRODZENIA I ŚWIADCZENIA SPOŁECZNE / WYNAGRODZENIA'),
        ('P3824', 'Mieszkania oddane do użytkowania', 'PRZEMYSŁ I BUDOWNICTWO / BUDOWNICTWO MIESZKANIOWE'),
        ('P1410', 'Powierzchnia', 'PODZIAŁ TERYTORIALNY / POWIERZCHNIA GEODEZYJNA KRAJU (DANE GUGIK)')
    ) as t (subgroup_id, block_name, subject_path)
{% endmacro %}

{# DBW blocks: indicator × section × presentation × periodicity; the files (one per year) share the prefix. #}
{% macro poc_dbw_blocks() %}
    select * from (
        values
        ('581_57_93_4', 'Ludność — makroregiony, regiony, podregiony; charakter miejscowości; płeć; grupy wieku', 'A'),
        ('581_16_2_1', 'Ludność — Polska, dane miesięczne', 'M')
    ) as t (block_id, block_name, freq_code)
{% endmacro %}

{% macro poc_wdi_blocks() %}
    select * from (
        values
        ('SP.POP.TOTL'), ('SP.DYN.LE00.IN'), ('SM.POP.NETM'), ('NY.GDP.MKTP.CD'), ('NY.GDP.PCAP.CD'),
        ('NY.GDP.MKTP.KD.ZG'), ('FP.CPI.TOTL.ZG'), ('SL.UEM.TOTL.ZS'), ('SL.EMP.TOTL.SP.ZS'), ('AG.LND.TOTL.K2')
    ) as t (indicator_code)
{% endmacro %}

{# NUTS 2016/2021 regions of Poland and the TERC voivodeship they equal; PL9 is Mazowieckie
   itself (NUTS 1) and its two NUTS 2 regions are children of the voivodeship. #}
{% macro poc_nuts_pl() %}
    select * from (
        values
        ('PL2', 'Makroregion południowy', null, 'makroregion'),
        ('PL4', 'Makroregion północno-zachodni', null, 'makroregion'),
        ('PL5', 'Makroregion południowo-zachodni', null, 'makroregion'),
        ('PL6', 'Makroregion północny', null, 'makroregion'),
        ('PL7', 'Makroregion centralny', null, 'makroregion'),
        ('PL8', 'Makroregion wschodni', null, 'makroregion'),
        ('PL9', 'Makroregion województwo mazowieckie', '14', 'wojewodztwo'),
        ('PL21', 'Małopolskie', '12', 'wojewodztwo'),
        ('PL22', 'Śląskie', '24', 'wojewodztwo'),
        ('PL41', 'Wielkopolskie', '30', 'wojewodztwo'),
        ('PL42', 'Zachodniopomorskie', '32', 'wojewodztwo'),
        ('PL43', 'Lubuskie', '08', 'wojewodztwo'),
        ('PL51', 'Dolnośląskie', '02', 'wojewodztwo'),
        ('PL52', 'Opolskie', '16', 'wojewodztwo'),
        ('PL61', 'Kujawsko-pomorskie', '04', 'wojewodztwo'),
        ('PL62', 'Warmińsko-mazurskie', '28', 'wojewodztwo'),
        ('PL63', 'Pomorskie', '22', 'wojewodztwo'),
        ('PL71', 'Łódzkie', '10', 'wojewodztwo'),
        ('PL72', 'Świętokrzyskie', '26', 'wojewodztwo'),
        ('PL81', 'Lubelskie', '06', 'wojewodztwo'),
        ('PL82', 'Podkarpackie', '18', 'wojewodztwo'),
        ('PL84', 'Podlaskie', '20', 'wojewodztwo'),
        ('PL91', 'Warszawski stołeczny', '14', 'region'),
        ('PL92', 'Mazowiecki regionalny', '14', 'region')
    ) as t (nuts_code, name_pl, woj_code, level_name)
{% endmacro %}

{# Which makroregion (NUTS 1) a voivodeship belongs to. #}
{% macro poc_woj_makroregion() %}
    select * from (
        values
        ('12', 'PL2'), ('24', 'PL2'), ('30', 'PL4'), ('32', 'PL4'), ('08', 'PL4'), ('02', 'PL5'), ('16', 'PL5'),
        ('04', 'PL6'), ('28', 'PL6'), ('22', 'PL6'), ('10', 'PL7'), ('26', 'PL7'), ('06', 'PL8'), ('18', 'PL8'),
        ('20', 'PL8')
    ) as t (woj_code, nuts1_code)
{% endmacro %}

{# Cross-source concepts: which measures mean the same thing. measure_key patterns are LIKE
   patterns against poc dim_measure.measure_key. #}
{% macro poc_concepts() %}
    {# KPI definitions are seeds (warehouse/seeds/kpi.csv, kpi_measure.csv, unit.csv). #}
    select
        k.kpi_key as concept_key,
        k.name_en,
        k.name_pl,
        u.name_en as unit,
        m.source_system,
        m.measure_key_pattern,
        m.definition
    from {{ ref('kpi_measure') }} as m
    inner join {{ ref('kpi') }} as k on m.kpi_key = k.kpi_key
    inner join {{ ref('unit') }} as u on k.unit_key = u.unit_key
    order by k.kpi_key, m.priority
{% endmacro %}

{# All PoC observations in the common shape (the build stages of the five sources). #}
{% macro poc_observations() %}
    select * from {{ ref('poc_build_eurostat') }}
    union all select * from {{ ref('poc_build_bdl') }}
    union all select * from {{ ref('poc_build_dbw') }}
    union all select * from {{ ref('poc_build_wdi') }}
    union all select * from {{ ref('poc_build_nbp') }}
{% endmacro %}

{# series_key: measure × geography × breakdown codes, readable; an unmatched area keeps its source code so
   two unmatched units never share a series. #}
{% macro poc_series_key() %}
    measure_key || '|' || geography_key
    || case when geography_key = '_X' then ':' || coalesce(geography_source_code, '') else '' end
    || '|' || breakdown_json
{% endmacro %}

{# Every activated block of every source: the list build_check reports against, so a block that
   yields no rows (not in Silver yet, or filtered out) is visible. #}
{% macro poc_activated_blocks() %}
    select 'eurostat' as source_system, dataset_id as block_id, block_name from ({{ poc_eurostat_blocks() }})
    union all
    select 'gus_bdl', subgroup_id, block_name from ({{ poc_bdl_blocks() }})
    union all
    select 'gus_dbw', block_id, block_name from ({{ poc_dbw_blocks() }})
    union all
    select 'world_bank_wdi', indicator_code, 'WDI ' || indicator_code from ({{ poc_wdi_blocks() }})
    union all
    select * from (
        values
        ('nbp', 'fx_A', 'NBP exchange rates, table A'),
        ('nbp', 'fx_C', 'NBP exchange rates, table C'),
        ('nbp', 'gold', 'NBP gold fixings')
    ) as t (source_system, block_id, block_name)
{% endmacro %}
