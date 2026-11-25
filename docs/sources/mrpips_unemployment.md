# MRPiPS registered unemployment (`mrpips_unemployment`)

**Built (11 Nov 2026, register PLS-029).** The Public Employment Services portal (psz.praca.gov.pl, "Bezrobocie
rejestrowane") publishes one ZIP per month, "statystyki strukturalne <month> <year>" (0.4-3 MB). Route: the listing
page links the newest ZIPs and, per year, a category page (`categoryId`) that links the year's asset page
("Statystyki strukturalne <year>"), which links the twelve ZIPs (`/documents/d/urzad-pracy/statystyki-strukturalne-…`
since 2023, `/documents/10828/<folder>/statystyki+strukturalne+….zip/<uuid>` before); months are read from the link
text. Entity = one month (`stat_YYYY_MM`), version = Last-Modified, a changed ZIP lands as a new version; restart
unit: one month. Each run lands the missing months and re-downloads the newest two (MRPiPS corrects recent rates);
older months are not downloaded again. Bronze `bronze.mrpips_unemployment_cells`: one row per cell (shared
`ingestion/common/xlsx_cells.py`) of the rate workbook ("Stopa bezrobocia rejestrowanego", until 2023
"stopa-pow-…": registered unemployed in thousands and the rate, Poland, voivodeships, powiats, TERYT WOJ./POW.)
and MRPiPS-01 sheets TAB 1 and TAB 2; a ZIP without a rate workbook in XLSX (January 2019 - August 2020: `.xls`
only) is rejected with that reason. Silver `silver.mrpips_registered_unemployment` (month x area x measure x sex):
`unemployed_thousands`, `unemployment_rate` (Poland, voivodeships, powiats) and `unemployed_persons` (TAB 2, total
and women, Poland and voivodeships). Gold block `registered_unemployment` (KPIs `registered_unemployed`,
`registered_unemployment_rate`, next to GUS BDL and DBW). Weekly schedule (Tuesday). Licence: CC BY 3.0 PL (portal
footer). The ZIPs since 2001, the half-yearly occupation and vacancy tables and the yearly MRPiPS-01 data files
(SPR1, about 27 MB) are not built yet.
