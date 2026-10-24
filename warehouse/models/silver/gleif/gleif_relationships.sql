{# GLEIF relationship records (RR-CDF): one row per start LEI × end LEI × relationship type of
   the newest golden copy (direct / ultimate accounting-consolidation parent, fund relations).
   The first period, qualifier and quantifier are typed; the others stay in Bronze. #}
select
    relationship_startnode_nodeid as child_lei,
    relationship_startnode_nodeidtype as child_id_type,
    relationship_endnode_nodeid as parent_lei,
    relationship_endnode_nodeidtype as parent_id_type,
    relationship_relationshiptype as relationship_type,
    nullif(relationship_relationshipstatus, '') as relationship_status,
    try_cast(nullif(relationship_period_1_startdate, '') as timestamptz) as period_1_start_at,
    try_cast(nullif(relationship_period_1_enddate, '') as timestamptz) as period_1_end_at,
    nullif(relationship_period_1_periodtype, '') as period_1_type,
    nullif(relationship_qualifiers_1_qualifierdimension, '') as qualifier_1_dimension,
    nullif(relationship_qualifiers_1_qualifiercategory, '') as qualifier_1_category,
    nullif(relationship_quantifiers_1_measurementmethod, '') as quantifier_1_method,
    try_cast(nullif(relationship_quantifiers_1_quantifieramount, '') as double) as quantifier_1_amount,
    nullif(relationship_quantifiers_1_quantifierunits, '') as quantifier_1_units,
    nullif(registration_registrationstatus, '') as registration_status,
    try_cast(nullif(registration_initialregistrationdate, '') as timestamptz) as registered_at,
    try_cast(nullif(registration_lastupdatedate, '') as timestamptz) as registration_updated_at,
    try_cast(nullif(registration_nextrenewaldate, '') as timestamptz) as next_renewal_at,
    nullif(registration_managinglou, '') as managing_lou,
    nullif(registration_validationsources, '') as validation_sources,
    source_version,
    raw_sha256
from {{ source('gleif', 'gleif_relationship_records') }}
