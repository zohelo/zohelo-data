{# IRENASTAT electricity statistics (register GL-035): installed electrical capacity (MW, a stock at the end of the
   year) and electricity generation (GWh, a flow over the year) by country or area (ISO3 codes as IRENA publishes
   them), technology (26 published technologies, renewable and non-renewable, parents such as 'Total Renewable' and
   'Renewable hydropower' next to their children, never summed here) and grid connection (All, On-grid, Off-grid),
   annual, from the newest copy of entity country_elecstat (bronze.irena_stat_electricity, table
   Country_ELECSTAT_<release>_PX). IRENA's technology, data type, grid and year codes are positions in the release
   ('0' … '25'), so the keys are the published labels; the codes are kept beside them. measure and unit_code come
   from the data type label ('Electrical Installed Capacity (MW)', 'Electricity Generation (GWh)'); a label of another
   shape gives null and fails the not_null tests. value is the published number; a cell without a value keeps value
   null with its status and never becomes 0 (cells marked '..' without a value are not in Bronze). Release 2026 H2
   (Dev trial 45868633684): 908,544 cells, 718,615 of them '..' without a value, 187,374 numbers and 2,555 published
   zeros, no other status; capacity 2000-2025 (23 technologies with values), generation 2000-2024 (25); 'All' equals
   On-grid + Off-grid within 1 % for every one of the 85,542 cells that have both.
   Grain: country_code x technology x data_type x grid_connection x year. #}
with records as (
    select *
    from {{ source('irena_stat', 'irena_stat_electricity') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    entity,
    table_id,
    split_part(source_version, '_', 1) || '_' || split_part(source_version, '_', 2) as release,
    country_code,
    country_label as country_name,
    technology_label as technology,
    technology_code,
    data_type_label as data_type,
    data_type_code,
    case
        when regexp_full_match(data_type_label, 'Electrical Installed Capacity \(MW\)') then 'capacity'
        when regexp_full_match(data_type_label, 'Electricity Generation \(GWh\)') then 'generation'
    end as measure,
    case
        when regexp_full_match(data_type_label, 'Electrical Installed Capacity \(MW\)') then 'MW'
        when regexp_full_match(data_type_label, 'Electricity Generation \(GWh\)') then 'GWh'
    end as unit_code,
    grid_label as grid_connection,
    grid_code,
    case when regexp_full_match(year_label, '(19|20)[0-9]{2}') then cast(year_label as integer) end as year,
    case when regexp_full_match(value, '-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?') then cast(value as double) end as value,
    value as value_text,
    status,
    raw_sha256,
    source_version
from records
