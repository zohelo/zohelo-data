{# GIOŚ groundwater quality monitoring (register PL-ENV-012): one row per sample row of the result workbooks
   (inorganic 1991-, organic 2009-, the 2025 nitrate study and its per-indicator classes), newest version per workbook
   (bronze.gios_groundwater_cells). Grain: entity x row_number. Point identifiers and location as published (no TERYT
   code), sampling date (Excel serial date as a date; text dates parsed, other text in sample_date_text), year of the
   study, the monitoring network named by the workbook's link text, for 2007 the monitoring column and for the
   organic results of 2023 the laboratory's analysis number. The values
   of a row are in gios_groundwater_results_* and gios_groundwater_indicator_classes (same entity and row_number). #}
select
    entity,
    workbook,
    year,
    {{ gios_groundwater_network('link_text') }} as monitoring_network,
    row_number,
    lp,
    point_no_monbada,
    point_no_soh,
    point_no_cbdh,
    point_id_monitoring,
    eu_point_id_161,
    eu_point_id_172,
    eu_point_id_174,
    gwb_number_161,
    gwb_code_161,
    gwb_number_172,
    gwb_code_172,
    gwb_number_174,
    gwb_code_174,
    x_puwg1992,
    y_puwg1992,
    voivodeship,
    powiat,
    gmina,
    locality,
    river_basin,
    rzgw,
    stratigraphy,
    aquifer_top_depth,
    screen_interval,
    water_table,
    aquifer_type,
    point_type,
    land_use,
    study_year,
    sample_date,
    sample_date_text,
    monitoring_type,
    lab_analysis_no,
    link_text,
    source_version,
    raw_sha256
from ({{ gios_groundwater_rows(['inorganic', 'organic', 'nitrate_inorganic', 'nitrate_classes']) }}) as r
