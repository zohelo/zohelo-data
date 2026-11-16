{# ENTSOG point dimension (from silver.entsog_physical_flow). Grain: operator × point × direction. Label, point type
   and the operator's item identifier are those of the newest gas day that carries them (the API sends no point type
   on isNA records); first and last gas day, the number of gas days and of days with a value. reports_flows is false
   for point-directions the operator never publishes a physical flow for (virtual and trading points: isNA on every
   day). The point group used in Gold is a reviewed static list keyed by point_key (macro poc_entsog_point_groups,
   tests/entsog_point_groups.sql warns on an unmapped point). #}
select
    operator_key,
    point_key,
    direction_key,
    arg_max(point_label, gas_day) as point_label,
    arg_max(point_type, gas_day) filter (where point_type is not null) as point_type,
    arg_max(id_point_type, gas_day) filter (where point_type is not null) as id_point_type,
    arg_max(tso_item_identifier, gas_day) as tso_item_identifier,
    arg_max(operator_label, gas_day) as operator_label,
    min(gas_day) as first_gas_day,
    max(gas_day) as last_gas_day,
    count(*) as gas_days,
    count(value_kwh_d) as days_with_value,
    count(value_kwh_d) > 0 as reports_flows,
    arg_max(raw_sha256, gas_day) as raw_sha256
from {{ ref('entsog_physical_flow') }}
group by operator_key, point_key, direction_key
