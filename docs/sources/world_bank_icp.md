# World Bank International Comparison Program (ICP) (`world_bank_icp`)

**Extraction (2 Nov 2026).** ICP has no bulk feature: DataBank publishes no bulk file for it
(`ICP_CSV.zip` and `ICP_2021_CSV.zip` return 404) and the ICP pages link only DataBank views. The
route is the World Bank API v2, source 90 (ICP 2021), tested for a full load by a Dev probe:
48 series, 1,182,384 observations, about 380 MB of JSON at 50,000 records per page. The
declaration `world_bank_icp.yaml` uses its own extractor (`ingestion/to_landing/world_bank_icp/`):
entity = one series for every economy and year
(`/v2/sources/90/country/all/series/<id>/time/all?format=json&per_page=50000`, one file per
page when a series is larger), version = the source's `lastupdated` date, so a series is
downloaded again only when the World Bank updates ICP. Restart unit: one series. Licence CC BY
4.0 (register GL-011). A series page can take minutes to generate when the API has no cached copy (prod run
43157799551): 7-minute timeout, six retries with backoff; a series that still fails is recorded and the
others still land, the run ends red and the next run fetches the missing series.

**Bronze and Silver (11 Nov 2026, #2736).** Bronze `bronze.icp_observations`
(`ingestion/to_bronze/world_bank_icp/`, shared loader `bulk_bronze.py` over the series folders in
Landing): one row per record with a value, the codes and labels of Country, Classification, Series and
Time, the value as the exact JSON number text. Silver `silver.icp_observations`. 231,174 observations,
207 economies and benchmark groups (ECB, HIB, LIB, LMB, UMB, WLD; no EU aggregate), 48 series × 17
classifications (expenditure in LCU and US$, PPP, per capita PPP, price level index world = 100, …),
2017–2023 (Dev trial 43085820597). Archive retention: newest version + 90 days. Weekly check Thursday 02:50 UTC (two catalogue requests while unchanged).
