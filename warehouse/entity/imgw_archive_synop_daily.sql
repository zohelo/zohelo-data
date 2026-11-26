-- Silver: IMGW observation archive, daily synoptic station data (dobowe/synop, s_d files), long.
-- Applied by the loader to ONE Bronze file; `bronze` is that file (design #875, ingestion/to_bronze/imgw_archive/).
-- Grain: station_code x observation_date x parameter. Seven parameters of s_d with their status field:
-- TMAX, TMIN, STD (daily max, min and mean air temperature, °C), TMNG (minimum at the ground, °C), SMDB (daily
-- precipitation, mm), PKSN (snow cover depth, cm), USL (sunshine, hours). IMGW status (Opis.txt, s_d_format.txt):
-- blank = measured value, "8" = no measurement, "9" = phenomenon absent. Older files write 0 / .0 in the value field
-- with status 8 or 9, newer ones leave it empty (UWAGA!.txt): the value is kept only for a blank status, so a missing
-- or absent observation is never a 0 here. Any other status is 'unknown' and rejects the file (loader check), as
-- does a blank status without a number. The date is the observation day as published (no time of day in the file).
with pairs as (
    select
        entity,
        nsp,
        post,
        rok,
        mc,
        dz,
        raw_sha256,
        source_version,
        unnest([
            {'parameter': 'TMAX', 'unit_code': 'degC', 'value': tmax, 'status': wtmax},
            {'parameter': 'TMIN', 'unit_code': 'degC', 'value': tmin, 'status': wtmin},
            {'parameter': 'STD', 'unit_code': 'degC', 'value': std, 'status': wstd},
            {'parameter': 'TMNG', 'unit_code': 'degC', 'value': tmng, 'status': wtmng},
            {'parameter': 'SMDB', 'unit_code': 'mm', 'value': smdb, 'status': wsmdb},
            {'parameter': 'PKSN', 'unit_code': 'cm', 'value': pksn, 'status': wpksn},
            {'parameter': 'USL', 'unit_code': 'h', 'value': usl, 'status': wusl}
        ]) as p
    from bronze
)

select
    entity,
    trim(nsp) as station_code,
    trim(post) as station_name,
    cast(try_strptime(trim(rok) || '-' || trim(mc) || '-' || trim(dz), '%Y-%m-%d') as date) as observation_date,
    p.parameter,
    p.unit_code,
    case when coalesce(trim(p.status), '') = '' then try_cast(trim(p.value) as double) end as value,
    nullif(trim(p.status), '') as status_code,
    case coalesce(trim(p.status), '')
        when '' then 'ok'
        when '8' then 'no_measurement'
        when '9' then 'phenomenon_absent'
        else 'unknown'
    end as observation_status,
    raw_sha256,
    source_version
from pairs
