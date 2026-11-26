-- entsog (silver.entsog_points): every point-direction that publishes a physical flow has a label and the API's own
-- point type (the Gold point groups are reviewed against it), and its span holds at least one day with a value.
-- Points published only as isNA (virtual and trading points) carry no type and are not checked. Reads only this model.
select
    operator_key,
    point_key,
    direction_key,
    point_label,
    point_type,
    days_with_value
from {{ ref('entsog_points') }}
where reports_flows and (point_type is null or point_label is null or days_with_value = 0)
