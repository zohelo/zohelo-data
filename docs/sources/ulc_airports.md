# ULC airport traffic statistics (`ulc_airports`)

**Built (Nov 2026, register PL-ENV-026, #2967).** The Civil Aviation Authority (Urząd Lotnictwa Cywilnego) lists on
https://ulc.gov.pl/statystyki-analizy/statystyki-wg-portow-lotniczych one PDF per quarter (Q1 2010 on; no XLSX, CSV
or API). Route: that page is read once per run and its PDF links are taken as published, one request every 4
seconds with the platform's user agent (`ingestion/to_landing/ulc_airports/`). The site carries Imperva Incapsula
bot protection: an HTTP refusal or a challenge page stops the run and is recorded, never retried around. Entity =
the reporting quarter from the link text (`<year>_q<n>`; "Cały 2020 rok" is the Q4 file), version = Last-Modified,
a new copy only when the SHA-256 changed; missing quarters, quarters whose link changed and the newest two are
downloaded. Restart unit: one quarter's PDF.

Bronze `bronze.ulc_airports_rows`: the PDF text layer (pypdf, layout mode), one row per printed table row (title,
column labels, airport and measure labels, five values as text). Two layouts: until Q1 2023 one table holds both
measures under a numbered airport line, from Q2 2023 each measure has its own table. A table without a title,
column years or a "Suma" row, a row without five values, an unknown line or a file whose quarter table differs from
the listing's quarter rejects the file (`bronze.ulc_airports_rejected_files`).

Silver `silver.ulc_airports_traffic`: passengers and passenger flight operations per airport and in total ('_T'),
per quarter, Q1 2008 - Q1 2026. A quarter is printed in its own file and as the comparison column of the next two
years' files: the value comes from the newest file (it carries ULC's corrections). Q1 files print their table twice;
both copies must agree. "-" and "b/d" cells are not loaded; printed zeros are kept (airports before opening or
closed). The cumulative tables (half year, three quarters, whole year) are left out. Airports are matched to ICAO
codes and voivodeships by the correspondence `poc_ulc_airports_list()` in `warehouse/macros/sources/ulc_airports.sql`
(a new airport or spelling fails the build until added); `silver.ulc_airports_airports` is the airport entity table.

Gold block `airport_traffic` (`build_ulc_airports`, quarterly): the airport is the breakdown in its voivodeship, the
total row is Poland. KPIs `air_passengers` and `air_passenger_operations` (draft). Scheduled on the 9th, 16th and 23rd
(UTC 05:40, 14:10, 20:25): the listing answers GitHub runners with an Incapsula challenge page at irregular times
(production run 43843091936). A refused run is a warning while Landing is current and red when nothing has landed
or the next quarter is more than 210 days past its end (O44: the refusal is never worked around). Eurostat
`avia_paoa` also carries passengers of Polish airports; both stay as their own series (O31). Licence: unresolved (no
reuse terms on ulc.gov.pl; internal only).
