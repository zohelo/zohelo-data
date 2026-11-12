{# ENTSOG Transparency Platform (bronze.entsog_operational_data): daily physical gas flows as the transmission
   operator publishes them. Grain: operator × point × direction × indicator × period type × periodFrom (one gas day).
   Source: /api/v1/operationaldatas, indicator Physical Flow, periodType day, timezone WET, one answer per gas month.
   Versions: the newest Landing copy of each gas month (entity); within a key the record with the newest
   lastUpdateDateTime wins (rank, so two records of one key with the same lastUpdateDateTime both stay and fail the
   unique test instead of one being picked). Gas day = the date of periodFrom as published in WET: the gas day starts
   at 05:00 WET (06:00 Polish time) at most points and at 03:00 or 04:00 at some, so it is read per record, never
   assumed (tests/entsog_gas_days.sql). value_kwh_d is the published value in kWh/d; a record with isNA = 1 or an
   empty value has no value (null, never 0) and no flowStatus; any other non-numeric value fails the build. #}
with newest_month as (
    select *
    from {{ source('entsog', 'entsog_operational_data') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

typed as (
    select
        *,
        coalesce(is_na in ('1', 'true'), false) as missing_flag,
        cast(last_update_date_time as timestamptz) as last_update_utc
    from newest_month
),

newest_record as (
    select *
    from typed
    qualify rank() over (
        partition by operator_key, point_key, direction_key, indicator, period_type, period_from
        order by last_update_utc desc nulls last
    ) = 1
)

select
    operator_key,
    point_key,
    direction_key,
    indicator,
    period_type,
    period_from,
    period_to,
    cast(left(period_from, 10) as date) as gas_day,
    cast(period_from as timestamptz) as period_from_utc,
    cast(period_to as timestamptz) as period_to_utc,
    case
        when missing_flag or coalesce(value_text, '') = '' then null
        else coalesce(
            try_cast(value_text as double),
            error('entsog_physical_flow: non-numeric value ' || value_text || ' at ' || point_key || ' ' || period_from)
        )
    end as value_kwh_d,
    unit,
    nullif(flow_status, '') as flow_status,
    missing_flag as is_na,
    last_update_utc,
    point_label,
    nullif(point_type, '') as point_type,
    id_point_type,
    tso_item_identifier,
    operator_label,
    tso_eic_code,
    record_id,
    nullif(item_remarks, '') as item_remarks,
    entity,
    raw_sha256,
    source_version,
    ingested_at
from newest_record
