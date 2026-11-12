# NFZ statistics API, services (`nfz_services`)

**Built (register PLS-012, #2929).** NFZ publishes no bulk file of its hospital statistics (JGP); the API
`https://api.nfz.gov.pl/app-stat-api-jgp/` (version 1.1, Swagger at `/swagger/v1.1/swagger.json`) answers one
table per request, at most 25 rows a page. robots.txt answers 404; the terms allow 10 requests per second per IP,
the extractor makes one request at a time with a 0.7 s pause, bounded retries with backoff on 429, 5xx (also the
API's `"error-result": "Maintenance"` 500) and timeouts, and stops a run after three benefits in a row failed.

**Entity:** one benefit = catalog + dictionary entry (NFZ product code). Catalogs 1a homogeneous patient groups
(JGP, 883 on 12 Nov 2026), 1b separate benefits (134), 1c benefits added up (273), 1d radiotherapy (43),
1w highly specialised benefits (14). `ingestion/to_landing/nfz_services/` lands per benefit one ZIP of the exact
answers: the `index-of-tables` answer (asked by the dictionary *name*: the code answers `data: null`) and, for every
basic-data table it lists (type `general-data`, one per year 2009-2024, plus part-year tables where a benefit changed
within a year), every page for Poland (`branch=false`) and by NFZ regional branch (`branch=true`); and per catalog
one ZIP of the dictionary pages (`benefits_<catalog>`). Two 1b entries share a name (5.52.01.0001492 and
5.52.01.0001497, "POBRANIE NERKI OD DAWCY ŻYWEGO"): the index, asked by name, serves one product, so the other is
landed as its index only and recorded in `bronze.nfz_services_rejected_files` with that reason.

**Not loaded** (this is not full coverage of the API): the hospital-type splits (`hospitalType=true`) and the other
table types: sex, age, admission type (also NFZ-categorised), discharge type, healthcare services, product
categories, length-of-stay histograms, ICD-9 procedures, ICD-10 diagnoses. Age and sex nationally would add two
requests per benefit and year (8 and 4 rows), doubling the national requests.

**Version:** the API gives no update date (`meta.date-modified` is the time of the request, so the bytes of an
unchanged answer differ every time). A copy is written only when the digest of its answers' `data` members (in the
receipt with every member's URL, SHA-256 and digest) differs from the newest copy; version = download time. Each
weekly run asks the dictionaries (56 pages), six canary benefits (the KPI groups and one 1d and one 1w benefit) and
a quarter of the other benefits' indexes; an unchanged index costs that one request. A changed canary index means a
new year: every benefit is asked in that run. A fifty-second of the benefits is read whole each week, so a revision
under unchanged table ids is landed within a year. Restart unit: one benefit (complete or not there).

**Initial load:** about 1,347 index requests + 2 × ~11,000 benefit-years ≈ 23,500 requests at about 1.2 s ≈
8 hours, so two to three weekly runs (330-minute budget each); missing benefits first, the canaries first among them.

**Bronze:** `bronze.nfz_services_benefits` (one row per dictionary entry) and `bronze.nfz_services_basic_data` (one
row per record of a basic-data answer, values as the exact JSON number text; one `empty_table` row per listed table
without records and one `no_table` row per benefit without basic-data tables). **Silver:**
`silver.nfz_services_benefits` (catalog × code) and `silver.nfz_services_basic_data` (catalog × code × period ×
area: `PL` or the NFZ branch code 01-16), typed as published: counts, per cent (0-100), hospitalisations per patient
(`ratio-of-rehospitalizations`, e.g. 1.21), days, PLN and NFZ points. Small counts are published as they are (1, 2);
a branch without hospitalisations has no row; a value the API does not publish is null (average values since 2012,
`percentage-of-sections` of 1w) and never becomes 0.

**Gold:** `gold.build_nfz_services`, block = catalog, frequency A (part-year tables stay in Silver). Each benefit is
its own measure, `nfz_services:<catalog>:<unit>:<product code>:<measure>` (no total over benefits exists). Built:
hospitalisations and patients of every benefit for Poland, and every published measure for Poland and the 16
branches (branch code n → voivodeship TERC 2n, `poc_geography__nfz_services()`) of the JGP groups N01 childbirth,
H13 primary total hip replacement, H15 primary total knee replacement and A48 stroke unit > 7 days. The other
series stay in Silver: building every benefit by branch would add about 2 million rows to `gold.fact_annual`, whose
full rebuild is capped at 1.5 million (D28 exception #2294); they wait for the per-block Gold tables of D35. KPIs
(draft): `childbirth_hospitalisations`, `hip_replacements`, `knee_replacements`, `stroke_unit_hospitalisations`.

**Terms:** Regulamin API Statystyki NFZ – Świadczenia v1.1 (https://api.nfz.gov.pl/app-stat-api-jgp/terms, read in
the probe of 12 Nov 2026): the source must be stated by giving https://api.nfz.gov.pl/; the user may not modify the
data; no overloading; no licence is named and commercial reuse is not addressed. Licence row
`nfz_api_terms_services` (open-conditional): values are kept and shown as published, never rescaled or rounded.
