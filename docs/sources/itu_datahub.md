# ITU DataHub (`itu_datahub`)

**Built (26 Nov 2026, register GL-031, #2418–#2420).** The International Telecommunication Union publishes its ICT
indicators in the ITU DataHub (https://datahub.itu.int/). The producer's hosts datahub.itu.int and api.datahub.itu.int
refuse runners (robots.txt HTTP 403) and are not used. The World Bank republishes the DataHub as Data360 dataset
`ITU_DH`, and that copy is the route.

Route (bulk first; shared `bulk_file` pattern, no extractor code): the Data360 metadata of every ITU_DH indicator
names a CSV (`csv_link`), and the same folder holds the whole dataset in one file,
`https://data360files.worldbank.org/data360-data/data/ITU_DH/ITU_DH.csv` (65,676,828 bytes, Last-Modified 3 Apr 2026;
Dev probe 45872579778). It is SDMX-CSV with labels: STRUCTURE … OBS_CONF, then a `*_LABEL` column per code. Entity
`itu_dh` = the file; version = its Last-Modified; a copy lands only when its SHA-256 changed. Restart unit: the file.
The Data360 REST API (`/data360/data?DATABASE_ID=ITU_DH`, 1,000 rows a page, `skip`) is not used: a full read (Dev
probe 45871438953) returned 153,289 rows in 154 pages, every PRI_HU_VD row twice (9,436 against 4,718 in its CSV), so
its paging is no reliable full load.

Bronze `bronze.itu_datahub_observations`: one row per CSV row, all 41 columns as published text (names in lower
case), read by DuckDB. A file with another header, a row of another width or no row is rejected with the reason
(`bronze.itu_datahub_rejected_files`). Silver `silver.itu_datahub_observations`: grain indicator x economy (ISO3) x
sex x age x urbanisation x the three indicator-specific breakdowns x unit x year. The unit is part of the grain: one
indicator can be published in several units (subscriptions `SB` and per 100 inhabitants `SB_10P2_HB`; prices in
`USD`, `PPP` and `PT_GNI_PS`, % of GNI per person). Breakdowns as published: sex, age and urbanisation for the
internet-use indicators, skill level (`BASIC`, `ABOVE_BASIC`, no total) for the ICT skills; `_T` is the source's
total, `_Z` not applicable. The four `IT_COMP_*` indicators (unit `CAT`) publish text (`Monopoly`, `Full competition
(year when competition was introduced)`, `N/A`, …): kept in `value_text`, value null, never 0. Size (Dev trial 45874151208, 26 Nov 2026): one Landing file
of 65.7 MB, 144,549 rows (every row of the file), 39 indicators, 236 economies, 2000-2025, every row OBS_STATUS `A`
(normal value), OBS_CONF `PU`, UNIT_MULT `0`; 1,910 published zeros kept; no empty value.

Gold block `itu_dh` (`poc_build_itu_datahub`): one measure per indicator and published unit,
`itu_datahub:itu_dh:<UNIT_MEASURE>:<INDICATOR>`, annual; values and units as published (D33; UNIT_MULT is 0 in
every row, and another value would stay in the unit code, unmapped). Breakdowns in `breakdown_json` (source codes,
`_Z` left out), sex also in `sex_code`; `is_total` only where every breakdown is `_T` or `_Z`. Not loaded: the text
indicators, rows flagged as estimate (E) or forecast (F) (none in the trial), years after the build year. Economies
by ISO3 through `dim_geography` (213 matched); 23 codes without a Gold country stay `_X` with their code (2,606
rows: historic ANT and YUG, territories such as AIA, ASC, COK, FLK, GLP, GUF, REU, SHN, TWN, WLF). Trial: 139,238
rows, 49 measures, 12,091 series, 0 duplicates by series_key x date_key. KPIs: `internet_users`
(existing draft KPI; ITU joins UNdata's Statistical Yearbook copy as a second source, O31) and the new drafts
`fixed_broadband_subscriptions` (IT_NET_BBND per 100 inhabitants) and `mobile_broadband_subscriptions` (ACT_MOB_SB
per 100 inhabitants), with metrics models, value bounds and v2 map rows. Poland 2024: internet users 88.58 %, fixed broadband 25.36 and
active mobile broadband 196.42 per 100 inhabitants.

Left out: `ITU_DH_INT_CONN_BAND_MBIT` (international bandwidth to 2022), which the Data360 API lists but the file
does not hold; `INT_CONN_BAND` (the same concept, Mbit/s, to 2024) is in the file. The text indicators stay in Silver.

Overlap: WDI carries several of these ITU series (IT.NET.USER.ZS, IT.NET.BBND.P2, IT.CEL.SETS.P2); UNdata SYB 314
prints internet use for selected years. ITU is the upstream; each copy stays its own source (O31), never added.

Schedule: monthly, on the 16th at 06:35 UTC (no release calendar; ITU updates about once a year; O46 allows weekly at
most). Licence: `unresolved` (internal; the licence gate keeps it out of v1 and v2). Two statements disagree.
(1) ITU, https://datahub.itu.int/about: "The Data is licensed to the public through a Creative Commons
Attribution-Non-Commercial-ShareAlike 3.0 IGO license." and "Under the terms of this licence, you may copy,
redistribute, and adapt the Data for non-commercial purposes, provided the work is appropriately cited." (2) The World
Bank catalogue page https://datacatalog.worldbank.org/search/dataset/0067055/itu-data-hub: "This dataset is licensed
under Creative Commons Attribution 4.0", while https://data.worldbank.org/summary-terms-of-use says "Some datasets and
indicators are provided by third parties, and may not be redistributed or reused without the consent of the original
data provider". The owner's question is in the register entry. Attribution if published: "International
Telecommunication Union, ITU DataHub, https://datahub.itu.int/."
