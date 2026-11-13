# RAD-on / POL-on: institutions and study programmes (`radon`)

**Built (register PLS-023, #2990).** Route: the public RAD-on open-data API of OPI PIB, no key,
`https://radon.nauka.gov.pl/opendata/polon/<resource>?resultNumbers=<n>&token=<cursor>`; an answer holds `results`,
`pagination` (`maxCount`, the cursor `token` of the next page) and the API `version`. RAD-on has no bulk file and
publishes no quota; the swagger page and the `/api` catalogue are single-page apps (Dev probe 43328619935). A run
reads every page of each resource, one request at a time, 2 s apart, with bounded retries.

Selection (the smallest valuable one of the register entry): `institutions` (institutions of the higher education
and science system, every kind and status, 840 records) and `courses` (study programmes with their launches,
11,696 records). Left out for now: branches, doctoral schools, degree-granting bodies, evaluation of scientific
activity, projects, investments, patents. Out of scope: persons (staff, doctoral students, project managers,
promotion procedures, publication authors).

Personal data: an institution record names its head and carries contact details; every record is reduced to an
allowlist of non-person fields in memory before it lands (`ingestion/to_landing/radon/extract.py`), so Landing is
not the API's exact bytes but the allowlisted records as JSON lines sorted by UUID (as the Open KRS API source does).

Entity: one resource. Version: download time (the API gives none); an unchanged resource (same SHA-256) is not
landed again. A resource lands only when its distinct records equal the API's `maxCount`; records seen twice while
paging (the cursor follows the refresh time) keep the newest `lastRefresh`. Restart unit: one resource.

Bronze `bronze.radon_institutions`, `bronze.radon_courses` (one row per record, text, API field names). Silver
`silver.radon_institutions` (grain `institution_uuid`), `silver.radon_courses` (grain `course_uuid`). Gold entity
tables `gold.radon_institution`, `gold.radon_study_programme` keyed by POL-on's ids (REGON, NIP and KRS kept for
joins; voivodeship by name to `dim_geography`), and the build stage `gold.build_radon`: operating institutions by
kind and study programmes in operation by level, Poland and voivodeships, on the download day (frequency D, draft
KPIs `higher_education_institutions`, `study_programmes`).

Terms: none found on https://polon.nauka.gov.pl/en/public-data/ ("We make part of the data from the POL-on system
publicly available in accordance with the law. Most of it is provided through the RAD-on system") or in the RAD-on
help centre (searched for licencja, regulamin, limit); licence `unresolved`, internal only. Refresh full (D32),
weekly (Monday 03:35 UTC).
