# OECD PISA through the World Bank Education Statistics (`world_bank_edstats`)

**Built (12 Nov 2026, register GL-004).** The OECD publishes the PISA country mean scores only in web tools and
report workbooks on www.oecd.org, which answers HTTP 403 to GitHub runners (not worked around); the OECD Data
Explorer has no PISA dataflow, and the PISA databases are microdata (means need survey weights and plausible values,
not computed here). The World Bank's Education Statistics database (API source 12) republishes the OECD's means as
indicators `LO.PISA.REA`, `LO.PISA.MAT`, `LO.PISA.SCI` with `.FE`/`.MA` variants. `world_bank_edstats.yaml` lands one
World Bank API answer per subject (every economy, from 2000, one page) through the shared `bulk_file` pattern,
version = download time. Cycles 2000–2018 (source lastupdated 2024-06-25); PISA 2022 is not republished yet: the
register asks the owner about a manual load. Bronze: `bronze.edstats_observations`. Silver: `silver.edstats_pisa`
(grain entity × indicator × economy × year, sex from the indicator code; 3,777 values, 85 economies). Gold:
`gold.build_world_bank_edstats` — one measure per subject with a sex breakdown, EU27 members; KPIs `pisa_reading`,
`pisa_mathematics`, `pisa_science` (draft, unit `pisa_score`). Refresh full (D32). Terms: CC BY 4.0 (World Bank Data
Catalog 0038480), licence row `cc_by_4_0_world_bank_edstats`. Restart unit: one subject.
