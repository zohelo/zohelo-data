# World Bank Global Economic Monitor (`world_bank_gem`)

**Built (12 Nov 2026, register GL-006).** DataBank's bulk file GemDataEXTR.zip is stale (last modified 2024-03-05;
the source was updated 2026-09-08), so GEM comes from the World Bank API v2, source 15, like ICP (decision of
12 Nov 2026). The API does not finish a request for every economy or for several series (HTTP 524 after 300 s);
one series for the 27 EU members answers in about 70 s. `ingestion/to_landing/world_bank_gem/` (the ICP extractor's
fetch, JSON check and write) lands one file per declared series (`series_<id>`: CPI % y-o-y and index, core CPI,
industrial production, unemployment rate, retail sales volume, LCU per US$, real GDP), version = the source's
`lastupdated`, so a weekly run makes two catalogue requests and downloads only after an update. Bronze:
`bronze.gem_observations` (one row per record with a value, number text as published). Silver:
`silver.gem_observations` (grain series × country × period code; frequency A/Q/M from the code; 84,618 rows,
1987–2026). Gold: `gold.build_world_bank_gem` — each series at one frequency only (monthly; real GDP quarterly),
EU27 members; KPIs `cpi_annual_rate` and `unemployment_rate_monthly_sa` (draft). Retail sales cover 16 members, not
Poland. Refresh full (D32). Terms: CC BY 4.0 (Data Catalog record 0037798), licence row `cc_by_4_0_world_bank_gem`.
Restart unit: one series.
