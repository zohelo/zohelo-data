# RSPO register of schools and educational institutions (`rspo`)

**Built (Nov 2026, register PLS-021, #3031).** Route: the Ministry of National Education's yearly lists of the
register's state on 30 September on dane.gov.pl, dataset 839 "Wykaz szkół i placówek oświatowych"
(https://dane.gov.pl/pl/dataset/839), one XLSX resource per year titled "Wykaz szkół i placówek oświatowych wg stanu
bazy SIO na 30.09.<year>" (2020-2025: one sheet `Arkusz1`, the same 58 columns, about 55,600 rows and 17.5 MB each).
The register's REST API (api.rspo.gov.pl) needs credentials CIE grants on request and is not used; the public search
rspo.gov.pl offers no bulk file (Dev probes 43808336871, 43838490749, 45746487144). api.dane.gov.pl answered runners
on 26 Nov 2026, after timing out in earlier probes (45746487144, 45746804861, 45747111363, 45748207131).

`ingestion/to_landing/rspo/` reads the dataset's resource list once per run from the portal's public API
(`/1.4/datasets/839/resources`) and lands every yearly list whose file URL is not landed yet through the shared
bulk_file pattern, 3 s apart (a corrected upload gets a new dated URL). Entity = one reference year
(`school_list_<year>`); version = Last-Modified; restart unit: one list. Not landed: 2017 and 2018 (other layouts),
2013-2015 (one XLS per voivodeship).

Bronze `bronze.rspo_institutions`: one row per sheet row, read with DuckDB's `read_xlsx` (all text), allowlisted
columns only; a workbook with another sheet, header or school year, or fewer rows than its sheet XML, is rejected
(`bronze.rspo_rejected_files`). Not read (privacy, register research "contact and person-related fields are
minimised"): name, street, house and flat number, postcode, post office, locality, web address, REGON, the governing
body's id and name (7,742 of 55,741 entries of 2025 are run by a natural person) and the registering body's name.

Silver `silver.rspo_institutions`: one row per RSPO number and school year (2020/2021 to 2025/2026), codes and labels as
published (institution type, unit kind, public status, pupil category, specificity, organisational link,
governing-body type and class, registering-body type), TERYT codes of the gmina, powiat and voivodeship and of the
registering body, pupils, classes, teachers and full-time equivalents as published (missing = null, never 0).

Gold: entity table `gold.rspo_institution` (one row per RSPO number listed in any year, attributes of its newest list,
the years it is listed, whether the newest list holds it; the lists publish no start or liquidation dates) and block
`institutions` of `poc_build_rspo` (register entries, total and by institution type, public status and governing-body
class, one breakdown at a time; Poland, voivodeships, powiats and gminas; annual, date_key = 1 January of the
reference year, period_label 30.09.<year>; areas mapped statically by TERYT code in `macros/sources/rspo.sql`).
Pupils, classes and teachers stay in Silver for now (1,272 of 35,898 complex entries of the six lists report pupils, as their member
schools do). Dev trial 45754116316: 6 lists, 105.5 MB; 333,695 Bronze and Silver rows, 0 rejected, 60,859 RSPO numbers;
216,021 build rows, 0 duplicates by series_key x date_key; 72 gmina codes (all type 2, rural; 206 code-years, 6 in
the 2025 list) are not in today's TERYT and map to `_X` (1,712 rows).

Schedule: weekly, Wednesday 08:55 UTC (O46; one API request and no download when nothing is new). Refresh full (D32).
Terms: dane.gov.pl dataset 839 states CC BY 4.0 (`license_name`, default condition `license_condition_default_cc40`),
attribution "Źródło: Ministerstwo Edukacji Narodowej, Wykaz szkół i placówek oświatowych (dane.gov.pl)"; licence key
`cc_by_4_0_men_schools`, status open-conditional.
