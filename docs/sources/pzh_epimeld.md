# NIZP PZH-PIB EPIMELD annual bulletins (`pzh_epimeld`)

**Built (Nov 2026, register PLS-019, #443).** NIZP PZH-PIB (Zakład Epidemiologii Chorób Zakaźnych i Nadzoru) and GIS
publish the annual bulletin "Choroby zakaźne i zatrucia w Polsce w <year> roku" as one PDF per year on the fixed
EPIMELD index https://wwwold.pzh.gov.pl/oldpage/epimeld/index_p.html (robots.txt 404; no XLSX, CSV or API). Route:
the index is read once per run and its links `<year>/Ch_<year>.pdf` are taken as published, from 2006 on, one
request every 5 seconds with the platform's user agent (`ingestion/to_landing/pzh_epimeld/`, files through the
`bulk_file` pattern). Entity = one bulletin (`ch_<year>`), version = Last-Modified, a new copy only when the SHA-256
changed; a held bulletin is downloaded again only when its Last-Modified is newer. Restart unit: one bulletin.
The partial updates `Ch_<year>a.pdf` (2006-2013) are not landed: they reprint single changed rows of several tables
without the column headers, so they do not carry the bulletin's table; the next year's bulletin reprints the year's
Poland rows with those corrections, and Silver takes them from there.

Bronze `bronze.pzh_epimeld_rows`: the PDF text layer (pypdf, plain mode), one row per printed row of the per-disease
tables "Liczba zachorowań (w kwartałach i ogółem), zapadalność oraz liczba i procent hospitalizowanych wg
województw": POLSKA for the previous year and the bulletin's year, the voivodeships with a case in the bulletin's year
(numbered 1-16; voivodeships without a case are left out), quarters I-IV, cases, incidence per 100,000, hospitalised
and per cent, as printed. The text layer splits Polish diacritics into a letter and a spacing mark; titles and headers
are matched on a key without spaces, marks and diacritics, labels are stored with the marks recombined (NFC). A
table without its disease title, with other column headers, without the POLSKA row of the bulletin's year, with a
year other than the bulletin's or the one before, or with voivodeship rows out of order rejects the whole bulletin
(`bronze.pzh_epimeld_rejected_files`). The other tables of the bulletin (age, sex, place, season) are not read.

Silver `silver.pzh_epimeld_cases`: cases and incidence per year × disease (the ICD-10 codes of the table title, as
published) × area (`PL`, TERYT voivodeship code = bulletin number × 2). A value comes from the newest bulletin that
prints it (rank over the bulletin year): Poland's year Y from bulletin Y+1 when it exists, voivodeships from bulletin
Y. Cases printed "-" (no case) are not loaded, never as 0; "x" is not a value. The test `pzh_epimeld_cases_complete`
checks that every year has Poland rows and that the voivodeships of each bulletin add up to its POLSKA row. Values
of a bulletin whose text says "wstępne" are flagged `preliminary`.

Gold block `annual` (`build_pzh_epimeld`): per disease the measures `pzh_epimeld:annual:cases:<codes>` (unit
`disease_cases`) and `pzh_epimeld:annual:per_100k:<codes>` (`per_100k_population`), Poland and voivodeships,
frequency A. A disease whose codes change between bulletins is a new measure. Left out: quarters, hospitalisation,
the age/sex/place tables, the bulletins 1999-2005 and the fortnightly notices (INF_*, cumulative year to date; a
separate frequency later). Scheduled on the 13th of each month (UTC 08:50). Licence: unresolved (no reuse terms on the
index or in the bulletins; internal only). GUS republishes the annual counts (dane.gov.pl dataset 1776); that is not
this route.
