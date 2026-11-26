# UNHCR Refugee Statistics (`unhcr`)

**Built (12 Nov 2026, register GL-029).** The Refugee Statistics API (`https://api.unhcr.org/population/v1/`, public,
no key) returns a whole selection in one request when `limit` exceeds its rows; `unhcr.yaml` lands two fixed
`/population/` selections through the shared `bulk_file` pattern, version = download time (an unchanged response is
skipped): `population_by_asylum` (`coa_all=true`: every country of asylum, origins summed; 7,802 rows, 1951–2025)
and `poland_by_origin` (`coa=POL&coo_all=true`: Poland by country of origin; 1,373 rows, 1991–2025), ISO alpha-3
codes (`cf_type=ISO`). Default aggregation guard: the API sums every dimension left out into one row (`-`), so each
selection names its dimensions, Bronze rejects a response over more than one page, and
`tests/unhcr_selection.sql` checks each entity. The third-party `idmc` and `unrwa` endpoints are not landed. Bronze:
`bronze.unhcr_population` (values as text, `-` kept). Silver: `silver.unhcr_population` (grain entity × year × coa
× coo, end-of-year stocks in persons, `-` → null). Gold: `gold.build_unhcr` — refugees, asylum seekers, stateless
and other people in need of international protection for the EU27 members as countries of asylum, and Poland by
origin (`{"origin": <ISO3>}`); KPI `refugees_hosted` (draft). Refresh full (D32). Terms: CC BY 4.0 with UNHCR's
supplemental terms (credit "UNHCR Refugee Population Statistics Database" and link the terms), licence row
`cc_by_4_0_unhcr`. Restart unit: one selection.
