# TED notices of Polish buyers (`ted_notices`)

**Built (16 Nov 2026, register PLG-023, #2976).** TED (Tenders Electronic Daily, the Publications Office of the EU)
answers an expert query anonymously through the Search API v3 (`POST https://api.ted.europa.eu/v3/notices/search`,
"does not require a key"); the daily bulk packages hold every country's eForms XML, so the Search API is the route for
a Poland selection. Query `buyer-country=POL AND publication-date=<day>`, only the fields of `extract.FIELDS`
(publication number, dates, notice and form type, procedure type, contract nature, CPV, buyer country, buyer NUTS,
buyer legal type, estimated procedure value and currency): no buyer names, contact points, persons, addresses or
e-mails. Buyer names are left out until the register's person-field review: a subsidised buyer (legal types `org-sub*`,
art. 6 Pzp) can be a natural person running a business. At most 250 notices a page, `page × limit` at most 15,000 and
`fields × limit` at most 10,000 (measured in Dev trial 43344674034); the answer order is stable. `ingestion/to_landing/ted_notices/` lands one file
per page of one publication day (entity `day_<yyyymmdd>_p<n>`, version = the publication day); a published notice
never changes (a change is a new notice), so a day is fetched once and the newest three days are asked again
(unchanged bytes skipped). Page 1 of a day is written last and a day counts as landed only when page 1 has a receipt,
so a day whose writing stopped part-way is fetched again; `tests/ted_notices_pages_complete.sql` also compares a
day's notices in Silver with the total its answer states. A red Landing stops the run's later jobs (Pipeline
`!failure()`), so Gold is not rebuilt from a day that failed. Window from 2025-01-01; earlier notices and other buyer countries are left out. Bronze
`bronze.ted_notices` (one row per notice, values as text; unknown layouts to `bronze.ted_notices_rejected_files`).
Silver `silver.ted_notices` (grain: publication number; about 115,000 notices a year). Gold: entity table
`gold.ted_notice` (one row per notice, buyer voivodeship keys) and block `gold.build_ted_notices`: notices per
publication month for Poland and voivodeships, by form type and by notice type (KPI `procurement_notices`, draft); a
notice of several buyers counts once in each of their voivodeships; the open month is left out. Refresh full (D32).
Schedule weekdays 11:35 UTC. Restart unit: one publication day. Licence `unresolved` (the legal notice page could not
be read from the runner): internal.
