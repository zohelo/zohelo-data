{# World Bank WDI geography codes (D34 registration, docs/source-registration.md). #}

{# The World Bank economy codes of wdi_countries under `source_system`: a country by its ISO alpha-2 code, an
   aggregate as WB:<code>; a code dim_geography does not know is _X. WDI's own map, and the map of the World Bank
   sources that publish the same codes (IDS, WGI, Findex, ICP), built from wdi_countries only so that the shared
   map never waits for their Silver tables. Reads the CTE `geo` of poc_map_geography. #}
{% macro poc_world_bank_economies(source_system) %}
    select
        '{{ source_system }}' as source_system,
        w.economy_code as source_code,
        coalesce(g.geography_key, g_agg.geography_key, '_X') as geography_key
    from {{ ref('wdi_countries') }} as w
    left join geo as g on g.level_name = 'country' and w.iso2_code = g.iso2
    left join geo as g_agg on g_agg.geography_key = 'WB:' || w.economy_code
{% endmacro %}

{% macro poc_geography__world_bank_wdi() %}
world_bank_wdi as ({{ poc_world_bank_economies('world_bank_wdi') }})
{% endmacro %}

{% macro poc_wdi_blocks() %}
    select * from (
        values
        ('SP.POP.TOTL'), ('SP.DYN.LE00.IN'), ('SM.POP.NETM'), ('NY.GDP.MKTP.CD'), ('NY.GDP.PCAP.CD'),
        ('NY.GDP.MKTP.KD.ZG'), ('FP.CPI.TOTL.ZG'), ('SL.UEM.TOTL.ZS'), ('SL.EMP.TOTL.SP.ZS'), ('AG.LND.TOTL.K2'),
        -- zohelo-bi first slice, every source per KPI (O31, #2592)
        ('SL.TLF.ACTI.ZS'), ('SL.UEM.1524.ZS'), ('SL.UEM.NEET.ZS'), ('GB.XPD.RSDV.GD.ZS'), ('SP.DYN.TFRT.IN'),
        ('SP.POP.DPND.OL'), ('SE.TER.CUAT.BA.ZS'), ('GC.NLD.TOTL.GD.ZS'), ('EN.GHG.ALL.MT.CE.AR5'),
        ('BN.CAB.XOKA.GD.ZS'), ('BX.KLT.DINV.CD.WD'), ('SI.POV.GINI'), ('NE.GDI.FTOT.ZS'), ('PX.REX.REER'),
        ('NY.GDP.PCAP.PP.CD'), ('NY.GDP.MKTP.CN')
    ) as t (indicator_code)
{% endmacro %}

{# Gold block (D34; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__world_bank_wdi() %}
    {{ return({
        'block_model': 'poc_build_wdi',
        'activated': "select 'world_bank_wdi', indicator_code, 'WDI ' || indicator_code from (" ~ poc_wdi_blocks() ~ ")",
    }) }}
{% endmacro %}
