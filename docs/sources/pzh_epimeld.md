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
województw" and their variants (reported or diagnosed cases, "skierowanych do szpitala", incidence per 100,000 live
births for the congenital diseases, the six-column tables without hospitalisation of laboratory-confirmed influenza
and AIDS): POLSKA for the previous year and the bulletin's year, the voivodeships with a case in the bulletin's year
(numbered 1-16; voivodeships without a case are left out), quarters I-IV, cases, incidence, hospitalised and per
cent, as printed. The text layer splits Polish diacritics into a letter and a spacing mark; titles and headers are
matched on a key without spaces, marks and diacritics, labels are stored with the marks recombined (NFC). Column
headers that differ, a POLSKA block without a disease line, a table without a year row or voivodeship numbers out of
order reject the whole bulletin (`bronze.pzh_epimeld_rejected_files`). A printed row that cannot be split into its
values (glyphs printed over each other, a spreadsheet's "1E+05") or that names a year other than the bulletin's or the
one before is kept with its text and a `parse_note`, without values. The other tables of the bulletin (age, sex,
place, season, vaccination, HIV tests) are not read.

Silver `silver.pzh_epimeld_cases`: cases and incidence per year × disease × area (`PL`, TERYT voivodeship code =
bulletin number × 2). The disease key is the disease line as published without spaces, accents and punctuation (the
ICD codes alone are shared by several tables: children under 2, CJD and vCJD, the influenza tables), plus
`:reported`/`:diagnosed` for the two AIDS tables; a renamed disease is a new series. A value comes from the newest
bulletin that prints it (rank over the bulletin year): Poland's year Y from bulletin Y+1 when it exists, voivodeships
from bulletin Y. Cases printed "-" (no case), "x" or "." are not loaded, never as 0; a row Bronze could not read is not
replaced by an older bulletin's value. `quarters_match` marks a row whose printed quarters do not add up to its
printed year (a misprint, e.g. Wielkopolskie in the A41.3 tables 2020-2025; kept as published, `flagged` in Gold).
The incidence denominator stays with the measure: per 100,000 population, live births (congenital diseases), children
under 2 or aged 0-14 (the "U DZIECI" tables, whose header says only "na 100 tys."; cases / incidence gives the
children of that age). A block whose POLSKA rows give cases / incidence × 100,000 outside the band of its denominator
has its incidence dropped (`incidence_dropped`: A04 children under 2 in 2013-2014, congenital syphilis, puerperal
fever, listeriosis 2006); the cases stay. The
test `pzh_epimeld_cases_complete` checks that every year has Poland rows and that the voivodeships of each bulletin
add up to its POLSKA row (blocks with an unread or misprinted row excepted). No bulletin 2006-2025 says "wstępne";
the `preliminary` flag would mark one.

Gold block `annual` (`build_pzh_epimeld`): per disease the measures `pzh_epimeld:annual:cases:<disease key>` (unit
`disease_cases`) and `pzh_epimeld:annual:per_100k:<disease key>` (`per_100k_population`; `per_100k_live_births` for
the congenital diseases), Poland and voivodeships, frequency A. Draft KPIs `pertussis_incidence`,
`measles_incidence`, `chickenpox_incidence`. Left out: quarters, hospitalisation, the bulletin's other tables, the
bulletins 1999-2005 and the fortnightly notices (INF_*, cumulative year to date; a separate frequency later).
Scheduled on the 13th of each month (UTC 08:50). Licence: unresolved (no reuse terms on the index or in the
bulletins; internal only). GUS republishes the annual counts (dane.gov.pl dataset 1776); that is not this route.
