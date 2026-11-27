# GIOŚ surface-water status assessment (`gios_surface_water`)

**Built (Nov 2026, register PL-ENV-011, #3312).** Route: the status classification of the surface water bodies (JCWP)
of the State Environmental Monitoring (PMŚ), assessment 2019-2024, as GIOŚ publishes it in the public ArcGIS
MapServer services of its water portal, https://wody.gios.gov.pl/arcgis/rest/services/PJWP (capabilities
Map,Query,Data; no robots.txt, the host answers the gov.pl page). Five services `Ocena_stanu_JCWP_<category>_2019_2024`,
19 layers (Dev probes 45922872272, 45924431424):

| Service | Layers (features) |
|---|---|
| `rzeczne` (rivers) | 0 ecological status (2,340), 1 ecological potential (652), 2 chemical status (2,229), 3 overall status (2,941), 4 overall status with statistical methods (3,116) |
| `rzeczne_zbiorniki` (dam reservoirs) | 0 ecological potential, 1 chemical status, 2 overall status (45 each) |
| `jeziorne` (lakes) | 0 ecological status (700), 1 ecological potential (105), 2 chemical status (747), 3 overall status (840), 4 overall status with statistical methods (1,068) |
| `przejsciowe` (transitional) | 0 ecological status, 1 chemical status, 2 overall status (7 each) |
| `przybrzezne` (coastal) | 0 ecological status, 1 chemical status, 2 overall status (4 each) |

Fields: `FID`, `KOD_JCWP`, `NAZWA_JCWP`, `STATUS` and the class field of the layer (`S_P_EKO` ecological, `STAN_CHEM`
chemical, `OCENA` overall; the reservoir, transitional and coastal layers carry all three). No voivodeship or other
area code, so Gold is Poland only.

Landing (`ingestion/to_landing/gios_surface_water/`): the services answer no pagination (`resultOffset`: HTTP 400
"Pagination is not supported") and at most 1,000 features a query; a where clause with a FID range was answered HTTP
403 (probe 45922872272) and is not used. Each layer is read as ArcGIS documents for such services: its object ids
(`returnIdsOnly`), then its features by `objectIds` in pages of 1,000, attributes only, `f=json`, one request at a
time, 3 s apart, through the shared bulk_file pattern (exact bytes, receipt after the file, a page lands only when its
SHA-256 changed). Entity = one page of one layer, `<category>_<assessment>_p<nn>`; version = download time (the
services send no Last-Modified); restart unit: one page.

Bronze `bronze.gios_surface_water_features` (`ingestion/to_bronze/gios_surface_water/load.py`, DuckDB JSON): one row
per feature, attributes as published text. An answer with `error` or `exceededTransferLimit`, a field list other than
the layer's, no features, or other object ids than the page asked for rejects the file
(`bronze.gios_surface_water_rejected_files`).

Silver `silver.gios_surface_water_status`: water category x assessment x water body (`KOD_JCWP`), `status_class` =
the layer's class as published, the three class fields, `STATUS` (natural, heavily modified, artificial) and the name
kept. A test fails when one of the 19 layers is missing, a layer's FIDs have a gap or a repeat, or a page other than
the last holds fewer than 1,000 features.

Gold: block `status` of `poc_build_gios_surface_water`, Poland, per water category (and `all`, the five together) x
assessment (ecological status, ecological potential, chemical status, overall status; monitoring): assessed water
bodies, bodies and share per status class (breakdown `status_class` as published), and for chemical and overall
status, where the layer publishes two classes, the share in good status. The assessment covers six years: one annual
row, `date_key` 2024-01-01 (the first day of the final year), `period_label` `2019-2024`. The overall status with
statistical methods (an estimate for unmonitored bodies) stays in Silver. KPI `surface_water_good_status_share`
(draft, environment.condition, v2 map ready): the share of all assessed surface water bodies in good overall status.

Left out: the yearly physico-chemical results of publication 367 (84 XLSX workbooks, the current-year river workbook
48 MB, provisional values refreshed monthly; https://wody.gios.gov.pl/pjwp/api/publications/publication/367) and the
result services `Wyniki_pomiarow_chemicznych_i_fizykochemicznych_<year>`; they need their own Landing, Bronze and
limit-of-quantification handling, beyond this first selection.

Counts (Dev probe 45924431424, 27 Nov 2026): 29 pages, 14,906 Silver rows, no water body in two services; Gold 145
rows. Poland: 43 of 3,837 assessed bodies in good overall status (1.12 %; rivers 22 of 2,941, lakes 21 of 840),
411 of 3,032 in good chemical status (13.56 %).

Schedule: weekly, Thursday 05:35 UTC (O46; about 50 small requests; the assessment changes once per six-year cycle).
Refresh full (D32). Terms: the portal's regulations (https://wody.gios.gov.pl/pjwp/api/terms_and_conditions: reuse in
whole or in part, source given as „Źródło danych: GIOŚ – Państwowy Monitoring Środowiska”, processing stated) and the
dane.gov.pl record of the portal (dataset 608, CC BY 4.0); licence key `cc_by_4_0_gios_surface_water`, status
open-conditional.
