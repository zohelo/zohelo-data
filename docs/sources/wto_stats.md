# WTO Stats: OECD-WTO Balanced Trade in Services (`wto_stats`)

**Built (27 Nov 2026, register GL-020, #3317).** Route: the WTO page "Bulk download of select statistics"
(`https://www.wto.org/english/res_e/statis_e/trade_datasets_e.htm`, robots.txt allows it) links the BaTIS BPM6 ZIP
at a fixed URL, `…/daily_update_e/OECD-WTO_BATIS_data_BPM6-1.zip` (534 MB, Last-Modified 25 Nov 2026), landed through
the shared `bulk_file` pattern; version = Last-Modified, an unchanged file is not written again. The ZIP holds the
bulk CSV (3.1 GB, 52,096,926 rows: 202 reporters × 203 partners × exports and imports × 31 EBOPS 2010 items ×
2005-2025), the codes workbook and the methodology PDF. Entity: `batis_bpm6`; restart unit: the ZIP.

Bronze (`ingestion/to_bronze/wto_stats/`): `bronze.wto_stats_batis` keeps the CSV rows with Poland as reporter or
partner and those of the country-group reporters (World, EU, EU extra-trade), every value as text, DuckDB on the
extracted CSV (the load annotation states rows read and kept); `bronze.wto_stats_batis_codes` keeps every cell of the
codes workbook (Dev trial 45935187389: 523,466 of 52,096,926 rows kept, 842 workbook cells). An unknown member, CSV header or workbook header rejects the file. Silver:
`silver.wto_stats_services_trade` (grain reporter × partner × flow × item × year; reported, final and balanced values
side by side, the final value's methodology code and label; labels from the workbook, which does not list the
codes R_OECD, E3 and E4 that 3,215 rows carry). 2005-2025, release November2026. Gold: `gold.build_wto_stats`,
block `batis`: Poland as reporter, every partner (breakdown; World `WL` is the total), total services and the twelve
EBOPS main items, exports and imports, final and balanced values as separate measures, in millions of US dollars as
published (`USD_MN` → `mln_usd`; 218,972 rows, 52 measures, 203 partners). Status: final values reported by the reporter (`R_*`) are `ok`, aggregated ones
(`A`) `flagged`, estimated or modelled ones and every balanced value `estimated`. KPIs: `exports_services` (next to
UNCTAD) and `imports_services` (new, draft), final value with the World.

Left out: Poland as partner, the group reporters, sub-items and derived items stay in Silver only; the frozen BPM5
BaTIS (2017); TiSMoS (services by mode of supply, the next selection); the Timeseries API (key required). Refresh full
(D32): one file replaced about once a year. Schedule: monthly, the 16th, 04:45 UTC.

Terms: WTO "Copyright and permissions" (https://www.wto.org/english/info_e/copyrights_permissions_e.htm): "This site
and its contents are the copyright of the World Trade Organization."; material "can be reproduced for non-commercial
purposes provided attribution is given to the WTO and the WTO is informed. Commercial use of materials from the
website requires written permission from the WTO." No terms for the data files; BaTIS is a joint OECD-WTO dataset.
Licence `unresolved`: internal only.
