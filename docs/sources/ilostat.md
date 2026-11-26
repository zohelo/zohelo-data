# ILOSTAT (bulk indicator files) (`ilostat`)

**Built (11 Nov 2026, register GL-023).** ILOSTAT's bulk download page
(`https://rplumber.ilo.org/files/website/bulk/indicator.html`) links one gzip CSV per indicator and
frequency with every country, ILO region and period (`https://rplumber.ilo.org/data/indicator/?id=<id>&format=.csv.gz`).
`ilostat.yaml` lands five through the shared `bulk_file` pattern, one request per entity and run:
`une_deap_sex_age_rt_a` / `_q` (unemployment rate by sex and age), `emp_dwap_sex_age_rt_a`
(employment-to-population ratio), `eap_dwap_sex_age_rt_a` (labour force participation rate),
`eip_neet_sex_rt_a` (youth NEET rate): 1,718,986 observations, all 27 EU countries, Poland from the
LFS (source BA:432) 2000–2025 annual and 2000Q1–2026Q2 quarterly (Dev trial 43091444753). National
(reported) series only; ILO modelled estimates are separate indicators. No Last-Modified, so version =
download time; the bytes are stable between downloads, so an unchanged file is skipped. Bronze:
`bronze.ilostat_observations` (the generic SDMX format of the OECD onboarding: dimension columns
ref_area, source, indicator, sex, classif1 as series_key; obs_status and notes as attributes). Silver:
`silver.ilostat_observations` (ref_area ISO alpha-3 or ILO aggregate X…, typed value, frequency from the
entity). Refresh full (D32): a narrow table rebuilt weekly in full, ceiling exception #2832. Terms:
CC BY 4.0 for releases from 3 May 2023, licence row `cc_by_4_0_ilo`. Restart unit: one indicator file.
Gold (#2861): `gold.build_ilostat` — the five files for the EU27 members by sex, age bands 15+, 15-24, 25+ and
15-64 as measures, the annual and the quarterly unemployment rate as separate measures; additional source of KPIs
`unemployment_rate`, `youth_unemployment`, `employment_rate`, `activity_rate` and `neet_rate`. The survey
(`source`) is not part of the series: the ILO publishes one survey per area, indicator and period.
