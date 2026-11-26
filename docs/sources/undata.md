# UNdata (UN Statistical Yearbook tables) (`undata`)

**Built (12 Nov 2026, register GL-017).** UNdata's legacy data-mart download handlers now return the
UN System Data Commons page, so the maintained product is used: the UN Statistical Yearbook CSV tables
linked from `https://data.un.org/Default.aspx` (`_Docs/SYB/CSV/SYB68_<table>_202511_<title>.csv`). The
file names change per edition, so each entity finds its CSV link by table number (`url_from`); a copy is
written only when the SHA-256 changed. 27 tables of one layout (title line, then
`Region/Country/Area,,Year,Series,Value,Footnotes,Source`); five tables with extra columns (SYB 123,
130, 176, 317, 330) are left out until modelled. The files are Windows-1252 with a few undefined bytes,
decoded losslessly (Latin-1 plus the Windows-1252 0x80–0x9F characters). Bronze:
`bronze.undata_syb_values` (rows as published). Silver: `silver.undata_syb_values` (M49 area code,
year, series with its unit in the name, value without thousands separators): 126,703 rows, 1975–2025
(Dev trial 43163356932). Each row cites its source agency. Terms: UNdata conditions of use (free reuse
citing UNdata; commercial use not addressed), licence row `undata_terms` with `commercial_ok` false.
Monthly check. Restart unit: one table.
Gold (#2861): `gold.build_undata` — six series of four tables (GDP in current prices, GDP per capita, real GDP
growth; GERD; internet users; current health expenditure) for the EU27 members and World, values as published;
additional source of KPIs `gdp_current_prices`, `gdp_per_capita`, `gdp_volume_growth` and `gerd_gdp`, draft KPIs
`internet_users` and `health_expenditure_gdp`. Tables from UN WPP or ILO estimates are not built (their newest
year is a projection or an estimate ahead of the data).
