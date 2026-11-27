# GIS sanitary state of the country report (`gis_sanitary`)

**Built (26 Nov 2026, register PLS-018, Epic #442).** Główny Inspektorat Sanitarny publishes one report a year on
the fixed page https://www.gov.pl/web/gis/raport---stan-sanitarny-kraju (robots.txt allows all): "Stan Sanitarny
Kraju" 2006 to 2023 and, from 2024, "Raport z działań Państwowej Inspekcji Sanitarnej" (2024, 2025), each one PDF
attachment (`https://www.gov.pl/attachment/<uuid>`, 2 to 38 MB, all with a text layer; Dev probe 45894316074).
No table, workbook or dane.gov.pl dataset is published.

Landing: shared `bulk_file` pattern with a seed list of 14 editions, 2012 to 2025 (247 MB). Entity = one edition
(`ssk_<year>`, `pis_<year>`), version = the attachment's Last-Modified, written only when the SHA-256 changes.
`landing.extract` HEADs each file and downloads only a missing or newer one, 5 s apart. Each run compares the
page's attachments with the seed list and `extraction.excluded` (the 2006-2011 editions) and warns "attachment on
the report page not in the seed list" for a new one; the seed row is added by hand. Restart unit: one edition.

Bronze `bronze.gis_sanitary_cells`: the PDF text layer read with pypdf (plain mode for every page, layout mode for
the pages that name "krajowe"). One table is kept, "Jakość zdrowotna środków spożywczych": one row per printed year
(the report year and up to eleven years before it), samples examined and per cent disqualified for all, domestic,
imported and EU foodstuffs, one Bronze row per cell as printed with page, row label and column header. The page is
found by title and header words; a year row is a year and eight number cells; the years must run without gaps up to
the edition's year; domestic + imported + EU samples must equal the total (to 1 / 1,000; 2020 is printed 50 short
of 58,883 in every edition) and, for the report year, the total share must equal the groups' weighted share (to
0.06 points). A file failing any check goes to `bronze.gis_sanitary_rejected_files` with the reason. The table has
no suppression or missing marker. No text, photographs or names are stored.

Silver `silver.gis_sanitary_food_samples`: edition x reference year x origin (total, domestic), typed, every printed
year kept with `is_report_year`. Imported and EU stay in Bronze only: their column order changes between editions
and the 2019 edition prints its 2018 and 2019 rows against its own header.

Gold block `food_samples` (annual, Poland): samples examined and per cent disqualified, all and domestic
foodstuffs, each year from its own report, 2012 to 2025. The 2024 and 2025 reports print the same table with the
same columns and the same values for 2020 to 2023 as the 2023 edition, so they continue the series. Reprints are
not used: from 2021 they give 2020 as 3,2 % where the 2020 edition and its components give 3,09 %. KPI (draft):
`food_samples_disqualified_share`. Schedule: monthly on the 13th (07:50 UTC), the report has no fixed date.

Dev trial (#442, 26 Nov 2026; run ids in the pull request): 14 files, 247,352,188 bytes; 824 Bronze cells (103
printed years x 8), 0 rejected; Silver 206 rows, editions 2012-2025, reference years 2010-2025; Gold 56 rows
(14 years x 4 measures); a second Landing (nothing downloaded), Bronze and dbt run changed nothing.

Licence: gov.pl terms (https://www.gov.pl/web/gov/prawa-autorskie): text CC BY-SA 4.0, official materials free to
use; the report files carry no licence of their own. `licence_key` `gov_pl_terms_gis`, attribution "Źródło: Główny
Inspektorat Sanitarny, Stan sanitarny kraju (<rok>)".

Left out: every other table of the reports (inspections and establishments, water supplies "Struktura wodociągów":
its layout and units change between editions, the 2013 and 2015 text layers break its numbers and the 2020 SUMA
does not add up; by food group; bathing waters; epidemiology), the regional tables, the 2006-2011 editions (years
2000-2009) and all text.
