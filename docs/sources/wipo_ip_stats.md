# WIPO IP Statistics, patents and industrial designs by office and origin (`wipo_ip_stats`)

**Built (26 Nov 2026, register GL-032, #1262).** Route: the bulk ZIPs linked from
`https://www.wipo.int/en/web/ip-statistics`, declared as two files of the shared `bulk_file` pattern (no extractor
code) and found on that page by file name (`url_from`), because their paths change with releases:
`wipo-data-patent-indicators.zip` (entity `patent_indicators`) and `docs-en-wipo-data-design-indicators.zip`
(`design_indicators`). The interactive Data Center (`www3.wipo.int`) is disallowed for all agents by its robots.txt
and is never used. Entity = one ZIP; version = its Last-Modified (the files on the page date from 11 April and
21 June 2024); a copy lands only when its SHA-256 changed; restart unit one ZIP.

- Bronze `bronze.wipo_ip_stats_counts` (`ingestion/to_bronze/wipo_ip_stats/load.py`, DuckDB CSV reader): one row per
  CSV row (`year,office,origin,tec_id|class_id,count`, values as text) and one per technology field of
  `patent_technology_field_names.xlsx` (sheet `technology_fields` by name). A ZIP with another member set, another CSV
  header or a names workbook of another shape is rejected into `bronze.wipo_ip_stats_rejected_files`.
- Silver: `silver.wipo_ip_stats_patent_publications` and `silver.wipo_ip_stats_patent_grants` (office x origin x
  technology field x year; 35 WIPO fields and 0 Unknown, field names from the ZIP) and `silver.wipo_ip_stats_designs`
  (design counts in applications and registrations, measure x office x origin x Locarno class x year). Codes as
  published: ISO 3166-1 alpha-2 offices and origins, regional offices (EP, EM, EA, AP, OA, GC, BX), historical codes
  (CS, YU, DD, AN) and `**` (not stated). The files have no zero, missing or flagged cells. 2000-2022.
- Gold block `gold.build_wipo_ip_stats` (`warehouse/macros/sources/wipo_ip_stats.sql`, blocks `patent_publications`,
  `patent_grants`, `design_applications`, `design_registrations`): each count by filing office (summed over applicant
  origins) and, as a separate measure, by applicant origin (summed over offices), per technology field or Locarno
  class; patents also as the total of the fields. Designs get no total: the design files hold only the offices that
  report by class (the Polish office has at most 61 designs a year in applications before 2018), so a sum is not an
  office total. Offices that are not countries, historical codes and `**` map to `_X` with their code kept (Dev trial
  45875926273: 15,168 of 375,863 rows; `**`, EA, EP, AP, YU, BX, OA, EM, CS, GC, DD, EU). KPIs (draft, own
  concepts, because a sum of technology fields is not the office's own annual total):
  `patent_grants_filing_office`, `patent_grants_applicant_origin`.
- Schedule: monthly, the 19th at 05:35 UTC (WIPO updates the files once a year at most; O46). Refresh full (D32).
- Licence: restricted. WIPO IP Statistics Data Center conditions of use
  (`https://www.wipo.int/en/web/ip-statistics/about`): "By using WIPO's statistical data, users agree not to republish
  or commercially re-sell WIPO's statistical datasets."; written work cites "WIPO Statistics Database" as the source.
  `source.csv` licence key `unresolved`: the data stay internal (licence gate); derived analysis citing the source is
  allowed. Attribution: "Source: WIPO Statistics Database, <download date>."
- Left out: patent applications (not in the patent ZIP; only on the Data Center), the trademark ZIP (class counts by
  Nice class, 2.4 M rows per file: above the 1 M ceiling of a full-rebuild model, needs an incremental design, D28),
  `wipo_ip_historical_data.zip` (pre-1980, PCT 1985-1999), `ipc_technology.xlsx`, the top-applicant workbooks, and
  the office x origin pairs in Gold (they stay in Silver).
