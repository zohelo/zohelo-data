# UN Global SDG Indicators Database (`un_sdg`)

**Built (27 Nov 2026, register GL-018, #3316).** The UN DESA Statistics Division publishes the Global SDG Indicators
Database: every series of the global SDG indicator framework by area, year and disaggregation, compiled from the custodian
agencies. Its data portal (https://unstats.un.org/sdgs/dataportal) is a JavaScript page; the SDG API v1
(`https://unstats.un.org/SDGAPI/v1/sdg`) serves one series at a time.

Route (`extraction.kind: bulk_file`, no extractor code): the database archive page
https://unstats.un.org/sdgs/indicators/database/archive lists one CSV ZIP per release state, newest first:
`<release>_AllData_After_<date>_CSV.zip` is the database as published after that date's update, and
`*_Before_*` is the state the update replaced. The newest `*_After_*` file is the current release. The pattern takes the
first matching link. Release 2026.Q3.G.01 is `2026_Q3.1_AllData_After_20260930_CSV.zip`: 68,233,529 bytes, Last-Modified
1 Oct 2026, one CSV of 1.83 GB, 3,198,008 rows, 713 series and 406 areas. Its `ReleaseName` equals the API's
`Series/List` release. The API is not used for the load: Poland and the EU took 713 `Series/Data` requests and 68
minutes, and the answers repeat each row once per indicator (Dev probe 45924741004). robots.txt disallows only `/wiki/`.
Entity `alldata_csv`; version = Last-Modified; a copy is written only when the SHA-256 changed. Restart unit: the file.

Bronze `bronze.un_sdg_observations`: every row of the CSV (all 406 areas), values as published text, the CSV's empty mark
`NA` included. The header is 26 fixed columns (`""`, `ID`, `Goal` … `ObservationID`), then the attributes (`Nature`,
`Units`, `UnitMultiplier`, `Observation Status`) and one column per disaggregation dimension. The dimension columns follow
the series of the release (57 in 2026.Q3.G.01), so Bronze keeps them as one JSON text `dimensions` (name → code, keys
sorted, `NA` left out). A file is rejected with the reason when the fixed columns differ, an attribute column is missing,
a column name repeats, a row has another width, or it is not UTF-8.

Silver `silver.un_sdg_observations`: the 27 EU countries (Poland among them, M49 codes of `poc_m49_eu27`), one row per
`ObservationID`. The CSV repeats a record once per indicator it is listed under (28,532 records on two or three rows),
and those rows differ only in goal, target and indicator, so Silver lists the indicators on one row. Grain: series code ×
area × year × dimensions. `value` is the published number. `NaN` (no value; every row under nature N and observation
status M, O or Q carries it) and censored values (`<2.5`, `<100`, `<0.5`, `<0.1`) keep `value` null with `value_kind`
`no_value` or `below`, never 0. Units, nature (C, CA, E, G, M, N, NA), observation status and dimensions are kept as
published. Size (Dev trial 45939006900, 27 Nov 2026): one Landing file of 68.2 MB, Bronze 3,198,008 rows in one
Parquet file of 36 MB, Silver 442,595 rows (618 series, 1970-2026; Poland 17,193 rows, 514 series with a value), 19,009
published zeros kept.

Gold block `sdg_observations` (`poc_build_un_sdg`): one measure per series code,
`un_sdg:sdg_observations:<Units>:<SeriesCode>` (each series has one unit), annual, with the dimensions as the breakdown.
`is_total` is true when every dimension holds a total code (BOTHSEX, ALLAGE, ALLAREA, `_T`) or is the only code of its
series; Sex gives `sex_code`. These rows are not loaded: rows without a number, missing-value statuses, nature N, a
period of several years (`Time_Detail` 2013-2017 under year 2017; 93 series), and years after the release year.
Observation status B, D, E/I, U and P become break, definition_differs, estimated, low_reliability and provisional.
Otherwise nature E, M and G (estimated, modelled, global monitoring data) are estimated, and C, CA and NA are ok. Units
as published (D33); nine unit codes are mapped in `unit_map` (PERCENT, PER_100000_POP, PER_10000_POP, PER_100_POP,
KMSQ, TONNES, CU_USD, CU_USD_M, CON_USD_M); the other 35 codes in Gold stay unmapped and are reported. Gold holds
390,962 rows, 548 measures and 42,275 series in the trial, with all 27 areas matched and 0 duplicates by series × date.
Draft KPI
`women_in_parliament` (SG_GEN_PARL, indicator 5.5.1, IPU Parline), with its metrics model, value bound and v2 map row.

Left out: the 379 other areas (other countries and the UN regional aggregates) stay in Bronze. They do not reach Silver
and Gold because the register's smallest Gold selection is Poland and the EU, and all areas in one dbt table would be
3.2 M rows, above the 1 M ceiling of D28. All areas need the per-block Gold load of D35 (`gold.observations`) first.
Multi-year periods also stay in Silver only. The archive's older release states are not loaded (only the current
release). KPIs other than `women_in_parliament` are not mapped. The register says series loaded from their custodian
stay published from the custodian.

Overlap: many SDG series are compiled from sources already loaded, each kept as its own source (O31). These include WHO
(health), ILOSTAT (labour: SL_TLF_UEM), FAOSTAT (food and agriculture), UNESCO UIS (education), World Bank WDI (poverty
SI_POV_DAY1, access to electricity; WDI also carries IPU's women in parliament as SG.GEN.PARL.ZS), IRENASTAT (renewable
capacity EG_EGY_RNEW), ITU DataHub (ICT), UNHCR and Eurostat's own SDG datasets (`SDG_*`), which use EU definitions and
NUTS areas. No KPI of those sources is remapped to this source.

Schedule: monthly, on the 6th at 04:40 UTC (UNSD updates the database several times a year without a calendar: 27 Mar,
4 Jun, 7 Jul, 24 Aug and 30 Sep 2026). Licence: `unresolved`, so the licence gate keeps the source internal. The database
states no terms; the UN website terms (https://www.un.org/en/about-us/terms-of-use: personal, non-commercial use) are
website terms. The UNdata terms cover data.un.org, not this database, and many series carry their custodian agency's
terms. Attribution if published: "Source: UN DESA, Global SDG Indicators Database, https://unstats.un.org/sdgs/dataportal".
