# ECB Data Portal (`ecb`)

**Extraction (2 Nov 2026).** The ECB Data Portal has no bulk archive per dataflow, but its SDMX
REST API (`https://data-api.ecb.europa.eu/service/data/<flow>/<key>?format=csvdata`) returns a
whole selection, every series with its full history, in one response. The declaration `ecb.yaml`
lands four fixed selections through the shared `bulk_file` pattern, one request per entity and
run: `exr_daily` (EXR, euro reference rates of every currency), `fm_key_rates` (FM, key ECB
interest rates), `est_estr` (EST, €STR) and `icp_annual_rate` (ICP, HICP annual rate, every area).
Version = Last-Modified, download time when absent; written only when the SHA-256 changed.
Restart unit: one entity (one request). Entity: the selection, one file of SDMX CSV
observations with their attributes. Re-use under the ECB statistics usage policy (register
GL-016): free, also commercial, attribution "Source: ECB statistics", statistics not modified.
The queries are proven from Actions by the Dev trial (#2393).

**Implemented (11 Nov 2026).** Three more selections: `hicp_annual_rate` (HICP, the COICOP 2018 dataflow
that replaced ICP on 4 Feb 2026; ICP stops at 2025-12 and stays as the old method's history),
`irs_long_term` (IRS, long-term interest rates for convergence purposes, every EU member incl. Poland
from 2001-01) and `bsi_monetary_aggregates` (BSI, euro area M1–M3 stocks, index and annual growth,
seasonally adjusted, from 1980). Bronze: `bronze.ecb_observations`, the generic SDMX format of
`bronze.oecd_observations` (KEY split into `dataflow` and `series_key`). Silver: `silver.ecb_observations`
(all selections, typed, dimensions JSON, period_start). Gold: `gold.build_ecb` (blocks in
`poc_ecb_blocks`: 13 EXR currencies, key rates, €STR, HICP/ICP, IRS, BSI); KPIs fx_rate (EUR/PLN),
inflation (HICP), bond_yield_10y (IRS), m3_growth and ecb_deposit_rate. Areas: ISO two-letter countries
(GR), U2 → EA, I8 → EA19, I9 → EA20, B6 → EU27_2020 (fixed composition), B0/D0 → EU (changing composition). Weekly schedule
(Thursday 05:40 UTC), full rebuild (D32: about 300,000 rows).
