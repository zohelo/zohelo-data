{# Eurostat geography codes (decision D34, docs/source-registration.md): the codes of Eurostat's GEO code
   list, mapped by code. The curated Gold block of Eurostat (poc_build_eurostat) and its lists are at the end of
   this file; the per-block full coverage (warehouse/observations.py, D35) reads this map through
   gold.map_geography.

   Mapped: EL and UK to their ISO codes; Polish NUTS 1 and NUTS 2 through poc_nuts_pl() (a NUTS 2 region that
   is a voivodeship maps to its TERC key); countries; NUTS regions of other countries; Eurostat aggregates (a code of
   NUTS shape whose first two characters are not a NUTS country, such as EA19, EU15 or EFTA, is an aggregate; #2775).

   Polish NUTS 3 (PLxxx, 73 podregions): dim_geography keys a podregion by its GUS BDL unit id (`BDL:<id>`), which
   carries no NUTS code, and no table we load holds the NUTS–KTS correspondence. So a code is mapped by Eurostat's
   own name for it to the podregion of the same name in the voivodeship of the code's NUTS 2 parent (the first
   four characters, through poc_nuts_pl()). Names are compared in upper case without the prefixes `PODREGION`,
   `MIASTO`, `M.` and `M. ST.`, the rule the GUS DBW map uses for the same areas. A code that matches no
   podregion or more than one stays unmatched (`_X`) and is counted by the build check: so do the codes of
   earlier NUTS versions the list still carries (`PL127 Miasto Warszawa (NUTS 2013)`), which are other areas.
   Dev trial 43330997519: 73 of 73 podregions matched, each by one code. #}
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

{% macro poc_eurostat_blocks() %}
    select * from (
        values
        ('DEMO_PJAN', 'Population on 1 January', 'A', 'TOTAL,T'),
        ('DEMO_R_PJANAGGR3', 'Population on 1 January by NUTS 3 region', 'A', 'TOTAL,T'),
        ('DEMO_GIND', 'Population change: demographic balance and crude rates', 'A', ''),
        -- zohelo-bi first slice, Polska w skrócie (seeds/kpi_v2_map.csv, #2584)
        ('TRNG_LFSE_01', 'Participation in education and training (last 4 weeks)', 'A', 'Y25-64,T'),
        ('ILC_PEPS01N', 'Persons at risk of poverty or social exclusion by age and sex', 'A', 'T'),
        ('ISOC_SK_DSKL_I21', 'Individuals by level of digital skills (from 2021)', 'A', 'IND_TOTAL'),
        ('IRT_LT_MCBY_M', 'EMU convergence criterion bond yields, monthly', 'M', ''),
        ('NAMA_10R_2GDP', 'GDP at current market prices by NUTS 2 region', 'A', ''),
        ('GOV_10DD_EDPT1', 'Government deficit/surplus, debt and associated data', 'A', 'S13'),
        ('ENV_AIR_GGE', 'Greenhouse gas emissions by source sector', 'A', 'TOTX4_MEMO'),
        ('RD_E_GERDTOT', 'GERD by sector of performance', 'A', 'TOTAL'),
        ('DEMO_MLEXPEC', 'Life expectancy by age and sex', 'A', 'Y_LT1,T'),
        ('DEMO_R_MLIFEXP', 'Life expectancy by age, sex and NUTS 2 region', 'A', 'Y_LT1,T'),
        ('LFSI_NEET_A', 'Young people neither in employment nor in education and training', 'A', 'Y15-29,T'),
        ('TIPSII10', 'Net international investment position, % of GDP', 'A', 'NSA,FA,S1,S1,N_LE,WRL_REST'),
        ('DEMO_PJANIND', 'Population structure indicators at national level', 'A', ''),
        ('DEMO_R_PJANIND2', 'Population structure indicators by NUTS 2 region', 'A', ''),
        ('CEI_WM011', 'Recycling rate of municipal waste', 'A', ''),
        ('ERT_EFF_IC_M', 'Effective exchange rate indices, monthly', 'M', ''),
        ('EDAT_LFSE_03', 'Population by educational attainment level, sex and age (%)', 'A', 'Y25-34,T'),
        ('DEMO_FIND', 'Fertility indicators', 'A', ''),
        ('DEMO_R_FIND2', 'Fertility indicators by NUTS 2 region', 'A', ''),
        ('NAMA_10_GDP', 'GDP and main components', 'A', ''),
        ('NAMA_10_PC', 'Main GDP aggregates per capita', 'A', ''),
        ('UNE_RT_A', 'Unemployment by sex and age, annual', 'A', 'Y15-74,T'),
        ('UNE_RT_M', 'Unemployment by sex and age, monthly', 'M', 'SA,T'),
        ('PRC_HICP_MINR', 'HICP (COICOP 2018), monthly data', 'M', 'TOTAL'),
        ('LFSI_EMP_A', 'Employment and activity by sex and age, annual', 'A', 'Y20-64,T'),
        -- zohelo-bi first slice, every source per KPI (O31, seeds/kpi_v2_map.csv, #2592)
        ('EI_BSIN_M_R2', 'Industry: monthly business survey data', 'M', 'SA'),
        ('EI_BSIN_Q_R2', 'Industry: quarterly business survey data', 'Q', 'SA'),
        ('EI_BSCO_M', 'Consumers: monthly survey data', 'M', 'SA'),
        ('EI_BSSI_M_R2', 'Sentiment indicators, monthly', 'M', 'SA'),
        ('STS_INPR_M', 'Production in industry, monthly', 'M', 'B-D,CA'),
        ('STS_COPR_M', 'Production in construction, monthly', 'M', 'F,CA'),
        ('STS_INPP_M', 'Producer prices in industry, domestic market, monthly', 'M', 'B-E36,NSA'),
        ('STS_TRTU_M', 'Turnover and volume of sales in wholesale and retail trade, monthly', 'M', 'G47,CA'),
        ('BOP_GDP6_Q', 'Main balance of payments and IIP components, % of GDP, quarterly', 'Q', 'NSA,WRL_REST'),
        ('BOP_C6_Q', 'Balance of payments by country, quarterly', 'Q', 'S1,S1,WRL_REST'),
        ('BOP_FDI6_FLOW', 'Direct investment flows by industry, annual', 'A', 'FDI,TOTAL,WRL_REST'),
        ('NAMQ_10_GDP', 'GDP and main components, quarterly', 'Q', 'SCA'),
        ('NAMQ_10_LP_ULC', 'Labour productivity and unit labour costs, quarterly', 'Q', 'SCA'),
        ('JVS_Q_NACE2', 'Job vacancy statistics by NACE Rev. 2, quarterly', 'Q', 'NSA,B-S,TOTAL'),
        ('LFSI_EMP_Q', 'Employment and activity by sex and age, quarterly', 'Q', 'SA,T,Y20-64'),
        ('UNE_RT_Q', 'Unemployment by sex and age, quarterly', 'Q', 'SA,T'),
        ('LFST_R_LFU3RT', 'Unemployment rates by sex, age, education and NUTS 2 region', 'A', 'TOTAL,T,Y15-74'),
        ('UNE_LTU_A', 'Long-term unemployment by sex, annual', 'A', 'Y15-74,T'),
        ('EARN_GR_GPGR2', 'Gender pay gap in unadjusted form', 'A', 'B-S_X_O'),
        ('NAMA_10_AN6', 'Gross fixed capital formation by asset type', 'A', 'N11G'),
        ('NAMA_10_A10', 'Gross value added and income by A*10 industry', 'A', 'TOTAL'),
        ('ILC_DI12', 'Gini coefficient of equivalised disposable income', 'A', 'TOTAL'),
        ('NAMA_10R_2GVAGR', 'Real growth rate of regional gross value added', 'A', ''),
        ('GOV_10A_EXP', 'General government expenditure by function (COFOG)', 'A', 'S13,GF10'),
        -- slice needs-block rows (O36, #2743): half-years go to gold.fact_semiannual
        ('EARN_MW_CUR', 'Monthly minimum wages, bi-annual data', 'S', ''),
        ('ERT_BIL_EUR_M', 'Euro/ECU exchange rates, monthly data', 'M', '')
    ) as t (dataset_id, block_name, freq_code, total_breakdown)
{% endmacro %}

{# Eurostat dimensions that define the measure (with unit); every other dimension is a breakdown. #}
{% macro poc_eurostat_measure_dims() %}
    select * from (
        values
        ('DEMO_GIND', 'indic_de'),
        ('NAMA_10_GDP', 'na_item'),
        ('NAMA_10_PC', 'na_item'),
        ('LFSI_EMP_A', 'indic_em'),
        ('ILC_PEPS01N', 'age'),
        ('ISOC_SK_DSKL_I21', 'indic_is'),
        ('IRT_LT_MCBY_M', 'int_rt'),
        ('GOV_10DD_EDPT1', 'na_item'),
        ('ENV_AIR_GGE', 'airpol'),
        ('DEMO_PJANIND', 'indic_de'),
        ('DEMO_R_PJANIND2', 'indic_de'),
        ('CEI_WM011', 'wst_oper'),
        ('ERT_EFF_IC_M', 'exch_rt'),
        ('EDAT_LFSE_03', 'isced11'),
        ('DEMO_FIND', 'indic_de'),
        ('DEMO_R_FIND2', 'indic_de'),
        ('UNE_RT_M', 'age'),
        ('EI_BSIN_M_R2', 'indic'),
        ('EI_BSIN_Q_R2', 'indic'),
        ('EI_BSCO_M', 'indic'),
        ('EI_BSSI_M_R2', 'indic'),
        ('STS_INPR_M', 'indic_bt'),
        ('STS_COPR_M', 'indic_bt'),
        ('STS_INPP_M', 'indic_bt'),
        ('STS_TRTU_M', 'indic_bt'),
        ('BOP_GDP6_Q', 'bop_item'),
        ('BOP_GDP6_Q', 'stk_flow'),
        ('BOP_C6_Q', 'currency'),
        ('BOP_C6_Q', 'bop_item'),
        ('BOP_C6_Q', 'stk_flow'),
        ('BOP_FDI6_FLOW', 'currency'),
        ('BOP_FDI6_FLOW', 'stk_flow'),
        ('BOP_FDI6_FLOW', 'fdi_item'),
        ('NAMQ_10_GDP', 'na_item'),
        ('NAMQ_10_LP_ULC', 'na_item'),
        ('JVS_Q_NACE2', 'indic_em'),
        ('LFSI_EMP_Q', 'indic_em'),
        ('UNE_RT_Q', 'age'),
        ('UNE_LTU_A', 'indic_em'),
        ('NAMA_10_A10', 'na_item'),
        ('ILC_DI12', 'statinfo'),
        ('NAMA_10R_2GVAGR', 'na_item'),
        ('GOV_10A_EXP', 'na_item'),
        ('EARN_MW_CUR', 'currency'),
        ('ERT_BIL_EUR_M', 'statinfo'),
        ('ERT_BIL_EUR_M', 'currency')
    ) as t (dataset_id, dimension_id)
{% endmacro %}

{# Gold block (D34; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__eurostat() %}
    {{ return({
        'block_model': 'poc_build_eurostat',
        'activated': "select 'eurostat', dataset_id, block_name from (" ~ poc_eurostat_blocks() ~ ")",
    }) }}
{% endmacro %}
