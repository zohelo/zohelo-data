# Our World in Data chart data (`owid`)

**Built (#2982, register CD-031).** Three charts of third-party data that the platform has from no direct
source, as the register entry selects: `consumption-co2-per-capita` (Global Carbon Budget, tonnes per person),
`electoral-democracy-index` (V-Dem, 0 to 1) and `gdp-per-capita-maddison-project-database` (Maddison Project
Database, international-$ in 2011 prices). Not taken: the human development index and life expectancy (loaded from
their producers), the happiness and corruption charts (their origin licences do not allow free reuse), any other
chart.

Route: the chart API's fixed URLs, through the shared `bulk_file` pattern (no extractor code):
`https://ourworldindata.org/grapher/<slug>.csv?csvType=full&useColumnShortNames=true` and
`<slug>.metadata.json`, one Landing entity each (`<chart>`, `<chart>_metadata`), exact bytes, a new copy only when
the SHA-256 changed. Version: download time (the CSV's Last-Modified is the request time and metadata.json carries
the request day); the source's own version is the column's `lastUpdated` in metadata.json, kept in Silver
`owid_charts` and used as `source_version` in Gold. A CSV may answer 403 "Data is non-redistributable": the run
fails on it and the chart is not taken another way.

Bronze: `bronze.owid_values` (one row per CSV row × non-id column with a non-empty cell, text as published; the
header of each chart is declared and another header rejects the file), `bronze.owid_charts` (one row per column of
metadata.json). Silver: `silver.owid_observations` (grain chart × OWID entity × year, every entity: countries,
OWID's own areas, regions and income groups without a code, historical states) and `silver.owid_charts` (grain
chart × column short name). An empty cell is no row, never a zero. Gold: `gold.build_owid`, one block and one
annual measure per chart (`owid:<chart>:<unit>`), every country by ISO alpha-3, World (`OWID_WRL` → `WB:WLD`) and
Kosovo (`OWID_KOS` → XKX); continents, income groups, OWID's historical states and every year before 1900 (Maddison from year 1, V-Dem
from 1789; the conformed calendar starts in 1900) stay in Silver. Draft KPIs
`co2_consumption_per_capita` and `electoral_democracy_index` (no published KPI has these concepts); the
Maddison series is a fourth source of the published KPI `gdp_per_capita` (O31, its own unit
`intl_usd_2011_per_person`, priority 4), contract v2 only (`V2_ONLY_SOURCES`), so zohelo.com's first value does
not change.

Licence: per chart, from the origins of the indicator metadata (`origins[].license` of
`https://api.ourworldindata.org/v1/indicators/<id>.metadata.json`, read on a runner in Dev run 43340826282):
consumption-co2-per-capita — Global Carbon Project and "Various sources" (population), CC BY 4.0;
electoral-democracy-index — V-Dem, CC BY 4.0; gdp-per-capita-maddison-project-database — Bolt and van Zanden,
CC BY 4.0. Every origin allows reuse and modification, so the source row in `source.csv` carries `cc_by_4_0_owid`
(`licence.csv`). A chart whose origins are not all open stays out: adding a chart means reading its origins first,
and an origin that is not CC BY (or as open) gives the source `unresolved` until the owner decides. Attribution: the chart's `citationShort` in metadata.json (Silver `owid_charts.citation_short`). Refresh full (D32);
schedule monthly (14th, 05:40 UTC). Restart unit: one chart file.
