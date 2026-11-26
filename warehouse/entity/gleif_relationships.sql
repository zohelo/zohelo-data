-- GLEIF relationship records (RR-CDF): one row per start LEI × end LEI × relationship type of
-- the newest golden copy (direct / ultimate accounting-consolidation parent, fund relations).
-- The first period, qualifier and quantifier are typed; the others stay in Bronze.
-- Applied by the loader to ONE Bronze file; `bronze` is that file (design #875).
-- Dates: a published date whose UTC instant is outside years 1 to 9999 (e.g. 0001-01-01T00:00:00+01:00
-- is 31 Dec 1 BC in UTC) is null here and kept as published in Bronze. Parquet statistics of such a value
-- cannot be read by PyArrow/PyIceberg (OverflowError: date value out of range), so one record stopped
-- the whole file from loading (#2610).
with typed as (
    select
        *,
        try_cast(nullif(relationship_period_1_startdate, '') as timestamptz) as period_1_start_raw,
        try_cast(nullif(relationship_period_1_enddate, '') as timestamptz) as period_1_end_raw,
        try_cast(nullif(registration_initialregistrationdate, '') as timestamptz) as registered_raw,
        try_cast(nullif(registration_lastupdatedate, '') as timestamptz) as registration_updated_raw,
        try_cast(nullif(registration_nextrenewaldate, '') as timestamptz) as next_renewal_raw
    from bronze
)

select
    entity,
    relationship_startnode_nodeid as child_lei,
    relationship_startnode_nodeidtype as child_id_type,
    relationship_endnode_nodeid as parent_lei,
    relationship_endnode_nodeidtype as parent_id_type,
    relationship_relationshiptype as relationship_type,
    nullif(relationship_relationshipstatus, '') as relationship_status,
    case
        when period_1_start_raw between '0001-01-01 00:00:00+00' and '9999-12-31 23:59:59+00' then period_1_start_raw
    end as period_1_start_at,
    case
        when period_1_end_raw between '0001-01-01 00:00:00+00' and '9999-12-31 23:59:59+00' then period_1_end_raw
    end as period_1_end_at,
    nullif(relationship_period_1_periodtype, '') as period_1_type,
    nullif(relationship_qualifiers_1_qualifierdimension, '') as qualifier_1_dimension,
    nullif(relationship_qualifiers_1_qualifiercategory, '') as qualifier_1_category,
    nullif(relationship_quantifiers_1_measurementmethod, '') as quantifier_1_method,
    try_cast(nullif(relationship_quantifiers_1_quantifieramount, '') as double) as quantifier_1_amount,
    nullif(relationship_quantifiers_1_quantifierunits, '') as quantifier_1_units,
    nullif(registration_registrationstatus, '') as registration_status,
    case
        when registered_raw between '0001-01-01 00:00:00+00' and '9999-12-31 23:59:59+00' then registered_raw
    end as registered_at,
    case
        when registration_updated_raw between '0001-01-01 00:00:00+00' and '9999-12-31 23:59:59+00'
            then registration_updated_raw
    end as registration_updated_at,
    case
        when next_renewal_raw between '0001-01-01 00:00:00+00' and '9999-12-31 23:59:59+00' then next_renewal_raw
    end as next_renewal_at,
    nullif(registration_managinglou, '') as managing_lou,
    nullif(registration_validationsources, '') as validation_sources,
    source_version,
    raw_sha256
from typed
