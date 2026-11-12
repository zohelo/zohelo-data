{# GIOŚ groundwater quality monitoring (register PL-ENV-012): final groundwater quality class of each monitoring point
   in each "Klasy jakości wód podziemnych" workbook (2004-), newest version per workbook
   (bronze.gios_groundwater_cells, entity classes_<year>). Grain: entity x row_number (one workbook row = one point in
   that year). Point identifiers (MONBADA, SOH/SOBWP, CBDH, ID Monitoring numbers, EU point ids per groundwater-body
   division) and location names as published; no TERYT code is published (voivodeship, powiat, gmina by name).
   monitoring_network: the network the year's inorganic-results link names (diagnostic or operational), else the
   network the classes link names; monitoring_network_stated: the classes link's own (2012: "opercyjny", while the
   2012 results link and its 1,040 points are diagnostic). #}
with classes as (
    {{ gios_groundwater_rows(['classes']) }}
),

results_network as (  -- the single network the year's inorganic-results link names
    select
        year,
        {{ gios_groundwater_network('link_text') }} as network
    from (
        select distinct
            {{ gios_groundwater_year('entity') }} as year,
            link_text
        from ({{ gios_groundwater_newest(['inorganic']) }}) as c
    ) as l
)

select
    c.entity,
    c.year,
    case
        when r.network in ('diagnostic', 'operational') then r.network
        else {{ gios_groundwater_network('c.link_text') }}
    end as monitoring_network,
    {{ gios_groundwater_network('c.link_text') }} as monitoring_network_stated,
    c.row_number,
    c.lp,
    c.point_no_monbada,
    c.point_no_soh,
    c.point_no_cbdh,
    c.point_id_monitoring,
    c.eu_point_id_161,
    c.eu_point_id_172,
    c.eu_point_id_174,
    c.gwb_code_161,
    c.gwb_code_172,
    c.gwb_code_174,
    c.x_puwg1992,
    c.y_puwg1992,
    c.voivodeship,
    c.powiat,
    c.gmina,
    c.locality,
    c.river_basin,
    c.rzgw,
    c.stratigraphy,
    c.aquifer_type,
    c.point_type,
    c.land_use,
    c.study_year,
    c.sample_date,
    c.final_class,
    c.link_text,
    c.source_version,
    c.raw_sha256
from classes as c
left join results_network as r on c.year = r.year
