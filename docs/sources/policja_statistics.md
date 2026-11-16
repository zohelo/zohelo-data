# Police statistics portal (`policja_statistics`)

**Built (16 Nov 2026, register PLG-033, #2969; waiting for the scheduled runs).** statystyka.policja.pl (Komenda Główna Policji) links, on each
offence-category page, an XLSX "<category> wg jednostek podziału administracyjnego kraju - przestępstwa stwierdzone,
przestępstwa wykryte, % wykrycia (1999 - <end year>)" of about 30 kB. The file name carries the end year
(`…-do-2021.xlsx`), so the bulk_file pattern finds the link on the category page (`url_from`). Entity = one category
workbook (`ogolem` all offences, `wybrane_7` the seven selected categories), version = the server's Last-Modified;
restart unit: one file; robots.txt disallows only `/ftp/`. Bronze `bronze.policja_statistics_cells` (every non-empty
cell, `ingestion/common/xlsx_cells.py`). Silver `silver.policja_statistics_offences`: offences recorded, offences
detected and the detection rate per category, area as printed and year; columns found by their header text, never by
position; `n.d.` and empty cells kept with a null value and `cell_status`. Areas as printed: Polska, the 16
voivodeships ("woj. mazowieckie (KWP z/s w Radomiu i KSP Warszawa)" for Mazowieckie) and the two police areas that
split Mazowieckie ("teren działania KSP Warszawa", "teren działania KWP z/s w Radomiu"); the 16 voivodeships add up to
Poland in every year (Dev trial 43806377026). Workbook note: from 2013 offences found in proceedings of the central
units (BSWP, CBŚP) are assigned to the voivodeship units; earlier years count them differently. Gold takes Poland and
the voivodeships only (blocks `ogolem`, `wybrane_7`, registered in `warehouse/macros/sources/policja_statistics.sql`).
KPIs `recorded_offences` and `offence_detection_rate` (draft). Aggregates only, no data about persons. Monthly check
(9th, 06:50 UTC); the workbooks change about once a year.

Not built yet: the other eight category workbooks of the same layout (criminal, homicide, rape, drugs, economic,
corruption, road, drink-driving), the "postępowania wszczęte" workbooks by police unit, the road-accident reports,
and a rate per 100,000 residents. GUS BDL also publishes Police crime statistics by voivodeship and powiat (O31: both
kept).

Licence: `unresolved` (internal). The Police copyright note (https://policja.pl/pol/wolnytekst/46616,dok.html) asks
for the source ("prosimy o podanie ich źródła: policja.pl") and says nothing on commercial reuse or modification;
owner question in the register.
