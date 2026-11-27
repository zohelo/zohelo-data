{# GIOŚ surface-water status assessment 2019-2024 (register PL-ENV-011, #3312): the status classification of each
   surface water body (JCWP) in each layer of the PJWP ArcGIS services (bronze.gios_surface_water_features, newest copy
   of each page <category>_<assessment>_p<nn>). water_category: rivers, reservoirs (river bodies that are dam
   reservoirs), lakes, transitional, coastal; assessment: ecological_status, ecological_potential, chemical_status,
   overall_status (monitoring 2019-2024) and overall_status_statistical (monitoring and statistical methods, rivers and
   lakes). status_class = the layer's class field as published (S_P_EKO for the ecological layers, STAN_CHEM for
   chemical, OCENA for overall); the three fields, the body's STATUS (NAT natural, SZCW heavily modified, SCW
   artificial) and its name are kept as published. A body without a class in its layer keeps status_class NULL and
   fails the not_null test (never a chosen class).
   Grain: water_category x assessment x kod_jcwp. #}
with features as (
    select
        *,
        regexp_extract(entity, '^(.*)_p([0-9]{2})$', 1) as layer,
        cast(regexp_extract(entity, '_p([0-9]{2})$', 1) as integer) as page
    from {{ source('gios_surface_water', 'gios_surface_water_features') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    split_part(layer, '_', 1) as water_category,
    substr(layer, strpos(layer, '_') + 1) as assessment,
    kod_jcwp,
    nazwa_jcwp,
    case split_part(layer, '_', 2)
        when 'ecological' then s_p_eko
        when 'chemical' then stan_chem
        when 'overall' then ocena
    end as status_class,
    status as water_body_status,
    s_p_eko,
    stan_chem,
    ocena,
    '2019-2024' as assessment_period,
    cast(fid as integer) as fid,
    page,
    entity,
    raw_sha256,
    source_version
from features
