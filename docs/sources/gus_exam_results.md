# GUS mean eighth-grade exam results (`gus_exam_results`)

**Built (26 Nov 2026, register PLS-025, #449).** The CKE/OKE exam results are read through GUS's own republication,
never from cke.gov.pl (its robots.txt disallows every agent but Scrapy). The GUS page "Średnie wyniki uczniów na
egzaminie ósmoklasisty" (stat.gov.pl, `...,18,1.html`; dane.gov.pl dataset 2585) links one CSV ("Tablice w formacie
CSV") at a fixed address: the whole series in one file of about 340 kB, UTF-8 with a byte order mark, `;` separated,
header `nazwa_zmiennej; kraj; wojewodztwo; przedmiot; plec; typ_informacji_z_jednostka_miary; rok; wartosc; flaga`.
Route: shared `bulk_file` pattern, no extractor code. Entity = the file (`eighth_grade_means`); version = download
time (no Last-Modified or ETag), written only when the SHA-256 changed; restart unit: the file.

Bronze `bronze.gus_exam_results_means`: one row per CSV row, every value as published text, the value and the GUS sign
side by side; a file with another header, not UTF-8 or of uneven width is rejected
(`bronze.gus_exam_results_rejected_files`).

Silver `silver.gus_exam_results_eighth_grade`: mean result in % of the maximum number of points by area (Poland
`Ogółem` and the 16 voivodeships), subject (Polish, mathematics, English, German, French, Spanish, Russian, Italian)
and sex, 2019-2025; grain area × subject × sex × year. GUS sends value `0` with both of its signs: `(.)` (no or
unreliable data) and `(–)` (did not occur, e.g. no pupil of the area took that language). Such rows keep value null
and the sign (`observation_status` `missing` / `nil`), never 0. Gold block `exam8`, registered in
`warehouse/macros/sources/gus_exam_results.sql`: one measure per subject, sex as the breakdown, signed rows not
loaded. Values are kept as published: in 2 of 731 cells with all three sex groups the total lies outside the
girls'/boys' range by more than 1 point (2025: Podkarpackie French 91 vs 95/94, Zachodniopomorskie Russian 93 vs
95/98; Dev trial 45901699841). Draft KPIs `exam8_polish`, `exam8_mathematics`, `exam8_english` (unit `pct_max_points`). Monthly check on the
14th, 09:40 UTC (results in July; GUS republishes later in the year).

Licence: GUS terms (https://stat.gov.pl/copyright; copying allowed with the source given; commercial reuse not
addressed; no named licence on stat.gov.pl, D23); dane.gov.pl lists the dataset under CC BY 4.0. Attribution: "Źródło:
GUS na podstawie danych OKE, Średnie wyniki uczniów na egzaminie ósmoklasisty".

Left out: the matura means (GUS `publikacja,17`, dataset 2621): another shape (Poland only, by exam formula, kind and
level), not one Bronze block of this file. The XLSX copy of the same table. Powiat, gmina and school results (CKE,
OKE: robots refuse).
