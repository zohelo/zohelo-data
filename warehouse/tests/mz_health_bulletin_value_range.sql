-- MZ statistical bulletin (gold.build_mz_health_bulletin): every value lies within a plausible range for its unit and
-- statistic, for Poland and the voivodeships. The bounds catch a column taken from the wrong header, a rank read as a
-- value or a unit error (e.g. patient-days not in thousands), not small mistakes. Measured ranges: Dev trial on the
-- final head (ingestion/sources/mz_health_bulletin.yaml). A live test on the build stage: no metrics model of this
-- source is in tests/metrics_value_range.sql's union. Returns the rows outside.
select
    measure_key,
    geography_key,
    date_key,
    value
from {{ ref('poc_build_mz_health_bulletin') }}
where
    value < 0
    or (unit_code = 'persons' and value > 500000)
    or (unit_code = 'per_10k_population' and value > 200)
    or (unit_code = 'hospitals' and (value < 1 or value > 2000))
    or (unit_code = 'beds' and (value < 1000 or value > 300000))
    or (unit_code = 'cases' and (value < 10000 or value > 15000000))
    or (unit_code = 'cases_per_bed' and (value < 10 or value > 120))
    or (unit_code = 'thousand_patient_days' and (value < 100 or value > 80000))
    or (measure_codes = 'hospital_activity.length_of_stay.mean' and (value < 1 or value > 20))
    or (measure_codes = 'hospital_activity.bed_occupancy.days' and (value < 100 or value > 366))
    or (measure_codes = 'hospital_activity.bed_occupancy.pct' and (value < 25 or value > 100))
