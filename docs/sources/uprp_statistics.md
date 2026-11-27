# UPRP industrial-property statistics (`uprp_statistics`)

**Built (Nov 2026, register EX-001, #3265).** The Patent Office of the Republic of Poland (UPRP) publishes its
statistics as annual reports: https://uprp.gov.pl/pl/publikacje/raport-roczny-uprp links 30 PDF files for 2003 to
2025 (from 2019 a Polish and an English edition, link texts "2025 (wersja polska)" / "2025 (wersja angielska)"),
at addresses with no pattern, so the page is the entry. The statistics API and the chart iframes on
statystyka.pue.uprp.gov.pl are not read: that host's robots.txt is `Disallow: /`, and nothing is requested there
(owner question in the register: an e-mail to UPRP asking to allow the API and to state reuse terms).

**Landing** (`ingestion/to_landing/uprp_statistics/`): the Polish reports from 2019 only (seven files, about 110 MB);
entity = one report (`raport_<year>`), version = Last-Modified. A held URL whose Last-Modified has not changed is not
downloaded again; any other listed file is downloaded and lands only when its SHA-256 differs. Two Polish files for
one year, no report, or a gap between 2019 and the newest year fails the run. One request at a time, 3 s apart.
Restart unit: one file.

**Bronze** `bronze.uprp_statistics_cells` (`ingestion/to_bronze/uprp_statistics/`): the text layer read with pypdf
(already in `requirements.txt`; it applies the fonts' character maps, so "patenty na wynalazki" keeps its letters).
Nine tables of the statistics section are found by title (spaces removed, as the 2023 text layer splits words, "zg
łoszenia"): 1.1 and 1.7 inventions (applications by procedure; patents granted by origin), 2.1 and 2.7 utility
models, 3.1 European patents validated in Poland (the total over the countries of origin only), 4.1 and 4.5
trademarks, 5.1 and 5.4 industrial designs. Each table prints the five years up to the report's year. Guards, no
value by position alone: the header must name consecutive years; a row's digits must split into exactly one value
per year in exactly one way (columns and thousands are both separated by single spaces, "13 284 12 841"); every cell
is checked against its table's sums year by year (total = national + international procedure, each procedure =
domestic + foreign applicants; the validations total = the sum of the country rows, or, where a country row cannot
be read, for the report's own year only, the summary page's labelled figure "walidacje patentów europejskich
<n>"). Each expected cell is a Bronze row with its value as printed (or none) and its guard: `ok`, or the reason. A
table not found is a row with its reason; a file with none of the tables is rejected into
`bronze.uprp_statistics_rejected_files`. A page beyond pypdf's safety limits (the picture covers of the 2019
report) is read as empty, so a table on it would be "table title not found". Ranking tables (which name persons and
companies) are not read.

What the guards found in the reports 2019 to 2025 (Dev probes 45811725126 and 45813573663): rows with empty cells
for zeros ("krajowe 2 1" in 2019, many country rows of the 2019 and 2020 validations tables, "Tajlandia" in 2025),
glued digits in the 2023 text layer ("1 04274", "00000"), and producer inconsistencies (2024 report: validations
2023 total 8,105 while its countries sum to 8,106; trademark rights 2020 total 9,406 while national 7,150 +
international 2,229 = 9,379, repeated in the reports 2021 to 2024; 2023 report: foreign design registrations 2019 to
2021 that do not add up). Such cells are not loaded; the year then comes from the next newest report whose cell
passes (trademark rights 2020: 9,379 from the 2020 report).

**Silver** `silver.uprp_statistics_counts`: the `ok` cells, newest report per table × row × year (grain
`table_code x row_code x year`), value as a number and as printed, right, event, procedure and origin codes. History
2015 to 2025 for the eight application and grant tables (2015 for international utility-model applications by origin
is missing: the 2019 row has empty cells), validations 2017 to 2025. Test `uprp_statistics_counts_complete`: every
table's total for every year from its first year to the newest one.

**Gold** block `annual_reports` (`poc_build_uprp_statistics`, registered in
`warehouse/macros/sources/uprp_statistics.sql`): one measure per table and row, annual, Poland (`polska` → `PL`),
units as published in single units (`applications`, `rights` → `ip_rights`). Draft KPIs in education.science:
`patent_applications`, `patents_granted`, `trademark_applications`, `industrial_design_applications`; bounds in
`warehouse/macros/value_range/uprp_statistics.sql`. Patents granted by UPRP are also in GUS BDL P3005 (2,242 for 2025
there, 2,283 here: other scopes); the two stay separate series (O31).

Schedule: one run a month on the 17th, 08:35 UTC (one report a year, published in late summer; the run ends with
Bronze, and Silver and Gold are built by the platform's weekly Sunday run, O46). Licence: `unresolved`: UPRP's terms
of use (https://uprp.gov.pl/pl/warunki-korzystania-z-portalu-informacyjnego-urzedu-patentowego-rp-i-platformy-uslug-elektronicznych-urzedu-patentowego-rp)
do not address reuse, so the source stays internal (licence gate). Left out: the 2003 to 2018 reports (other
layouts: bilingual tables split by domestic and foreign applicants, charts; their years 2015 to 2018 are restated by
the 2019 to 2022 reports, the years before 2015 are not loaded), the English editions (the same figures), the tables
by voivodeship, applicant type, technology field, class and country, rights in force, and the API aggregates.
