{# ICP 2021 (World Bank API v2, source 90), typed: one row per economy × classification × series × year
   of the newest page of every series. series = expenditure category (1000000 GDP, 9100000 actual
   individual consumption, …); classification = the measure (CN expenditure in local currency billions,
   PPPGlob PPP, PCAP.PP expenditure per capita in PPP international $, PX.WL price level index with
   world = 100, …). Benchmark years 2017 and 2021, interpolated years in between. #}
select
    country_code as economy_code,
    country_name as economy_name,
    classification_code,
    classification_name,
    series_code,
    series_name,
    cast(time_value as integer) as year,
    try_cast(value_text as double) as value,
    value_text,
    raw_sha256,
    source_version
from {{ source('world_bank_icp', 'icp_observations') }}
