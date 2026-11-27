# Barometr Zawodów (`barometr_zawodow`)

**Built (27 Nov 2026, register PLS-030, Epic #454).** Barometr Zawodów is the annual expert forecast of demand for
workers by occupation, commissioned by the Ministry of Family, Labour and Social Policy and coordinated by the
Voivodeship Labour Office in Kraków (WUP Kraków). Experts in every powiat classify each occupation of the Barometr's
own list (about 170, grouped occupations, not KZiS codes) as deficit (`deficyt`), balance (`równowaga`) or surplus
(`nadwyżka`) for the coming year; the results are aggregated to voivodeships and Poland. The survey runs in Q3/Q4 of
the year before the forecast year; the edition for year N is published in December or January. A forecast is not an
observation: it is kept as its own table and never merged with vacancy or unemployment counts.

Route (`ingestion/sources/barometr_zawodow.yaml`, shared `bulk_file` pattern with `ingestion/to_landing/barometr_zawodow/`):
there is no bulk file, API or table download (the portal's tables and maps load their data by script). Each forecast
year has fixed publication pages (robots.txt allows all): the national page
`https://barometrzawodow.pl/modul/publikacje?publication=national&year=<year>` links the national report and the
national poster; the page of a voivodeship, `...?publication=province&province=<1-16>&year=<year>` (the fields of the
page's own publication form), links that voivodeship's poster. The directory `/forecast-card-zip/` answers 403, so
files are read only by the pages' own links. The mazowieckie page also links the posters of its subregions
(2019-2025); the link pattern leaves them out, and a page with more than one matching poster fails that file. The national report has no occupation x voivodeship table: it reprints
the 17 posters, with hidden remnants of other posters on some pages (2018-2023), so the posters themselves are the
source. Entity = poster and forecast year: `plakat_<year>` (Polska), `plakat_p<NN>_<year>` (voivodeship page NN);
version = Last-Modified, a copy is written only when the SHA-256 changed. Restart unit: one file. The two newest years
are downloaded again (a corrected file lands as a new version), older years only while missing, and the next year's
pages are read as optional. Every request (page, HEAD, download) waits 3 s after the previous one.

Bronze `bronze.barometr_zawodow_occupations` (`ingestion/to_bronze/barometr_zawodow/`): one row per occupation
printed on a poster. The category is not printed beside the occupations: each category is a coloured block whose tab
carries the category name as rotated text. `posters.py` walks the page's content and Form XObjects in drawing order
(shapes with colour and box, every text operation with its own position), leaves out text that a later box covers,
finds the blocks and their tab labels, and reads the lines inside each block, column by column, joining wrapped
lines. A file is rejected with the reason (`bronze.barometr_zawodow_rejected_files`) when a page does not read
cleanly: a block with text but no label, a label in no tab, a line between the blocks but in none of them, an
occupation in two categories, a category block with a tab whose label is not read, a poster for another year or area
than the file's. A label drawn twice in place is read once, as the copy drawn last (śląskie 2022 draws its DEFICYT
label under its tab and again on top). One poster prints a line twice in
the same block (łódzkie 2021, "Administratorzy stron internetowych"): Bronze keeps both rows and marks the second
(`repeat_of_item`); Silver keeps the first.

Silver `silver.barometr_zawodow_forecasts` (forecast year x area x occupation name): the category as printed and as
a key (`deficyt`, `rownowaga`, `nadwyzka`), the area as `PL` or the voivodeship's TERC code (from the printed name),
the file and page. Occupation names are the Barometr's own, as printed in each edition; the list changes between
editions, so there is no cross-year mapping. The 2016 national poster lists only occupations assessed in at least
half of the powiats, so a voivodeship can list occupations the Poland poster does not.

Gold block `forecast` (`poc_build_barometr_zawodow`): counts of published forecast classifications, not
observations: `barometr_zawodow:forecast:occupations:<category_key>` = the number of occupations an edition's poster
puts in the category, per area (Poland and the 16 voivodeships) and forecast year, annual, `date_key` = 1 January of
the forecast year, unit `occupations`, observation status `forecast` on every row. Every category an edition uses gets a row for every area of that edition, 0
when the area has none (each poster lists every occupation assessed for it). Voivodeship counts do not add up to
Poland (each area has its own list). Draft KPI `occupations_deficit_forecast` (metrics model, value bound, v2 map row).

Left out: the powiat posters (the publication form's powiat option; not read), the voivodeship and national reports'
commentary, the occupation-to-KZiS correspondence printed in the reports' occupation list, and the Barometr's second
forecast kind on the portal ("zmiana zapotrzebowania").

Schedule: monthly, on the 13th at 05:50 UTC (one edition a year; O46 allows weekly at most). Licence: `unresolved`
(internal). The national report states "Copyright by: Wojewódzki Urząd Pracy w Krakowie", "Kopiowanie i
rozpowszechnianie może być dokonywane z podaniem źródła." and "Publikacja jest dystrybuowana bezpłatnie."; the site
footer reads "All rights reserved". Copying and distribution with the source are allowed; commercial reuse and
modification are not stated. Attribution: "Źródło: Barometr zawodów <year>, Wojewódzki Urząd Pracy w Krakowie".
