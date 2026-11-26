# dane.gov.pl open-data catalogue (`dane_gov_pl`)

**Built (register PLG-030, #3263).** The national open-data catalogue dane.gov.pl (Chancellery of the Prime
Minister) does not accept connections from GitHub runners, so its catalogue is read from the official EU republisher:
data.europa.eu harvests it as the catalogue `dane-gov-pl` and serves it through its public search API (no key, no
published limit). Route (`ingestion/to_landing/dane_gov_pl/`): the API's scroll
(`/api/hub/search/search?filter=dataset&facets={"catalog":["dane-gov-pl"]}&scroll=true`, then
`/api/hub/search/scroll?scrollId=…`), pages of 1,000, one request at a time, a second apart; `includes` asks only
for the fields kept. Plain paging is not used: it stops at 10,000 records and its order is not stable (probe
45813541234 read 7,751 distinct records in 10,000 rows). Each record is reduced before Landing to an allowlist of
catalogue fields; descriptions in 24 languages, keywords and the publishers' e-mail addresses never land. The
catalogue lands only when the distinct dataset ids equal the `count` of the first answer. Entity = the catalogue
(`catalogue`, one JSON-lines file sorted by id), version = download time (the API gives no version), written only when
the SHA-256 changed; restart unit: the whole catalogue (12 requests). Size and history:
`ingestion/sources/dane_gov_pl.yaml`.

Bronze (`ingestion/to_bronze/dane_gov_pl/`; another key layout, a missing or repeated id rejects the file into
`bronze.dane_gov_pl_rejected_files`): `dane_gov_pl_datasets` (one row per harvested dataset record) and
`dane_gov_pl_distributions` (one row per distribution: format, media type, size, times, the dane.gov.pl resource link).
Silver: `dane_gov_pl_datasets` (grain: the dane.gov.pl dataset number; when a dataset's slug changes, data.europa.eu
adds a record and keeps the old one, and the newest version by `modified`, then by the harvest record's `modified`, is
kept, `harvest_records` counting them), `dane_gov_pl_dataset_categories` (dataset × EU data theme) and
`dane_gov_pl_distribution_months` (dataset × month of the distribution's last modification × format, with counts and
bytes; the per-distribution rows stay in Bronze, see the declaration). Gold: entity table `gold.open_data_datasets_pl`
(one row per dataset: publisher's dane.gov.pl institution number and name, periodicity, numbers of themes and
distributions, harvest lag); build stage `build_dane_gov_pl`, block `catalogue`: datasets and distributions per month
of their last modification, in total, by publisher and by EU data theme (Poland; the state of the catalogue on the
download date, so earlier months change as datasets are modified again; the download month is provisional). Draft
KPIs `open_data_datasets_modified` and `open_data_distributions_modified` (no v2 map row). Weekly, Wednesday 15:25 UTC
(O46).

Harvest lag (Dev trial 45827739830, 26 Nov 2026, 13:19 UTC): the newest dataset modification in the harvest was 18.8 h
old and the newest harvest record 13.5 h old (data.europa.eu harvests daily, its records changed between 17:00 and
24:00 UTC); for datasets modified in the last 30 days, the harvest record followed the modification by 63 h at the
median and 375 h at the 90th percentile. So the weekly run reads a catalogue at most about a day behind dane.gov.pl
for most records, and up to about two weeks behind for some. The entity table carries `harvest_lag_hours` per dataset.

Licence: CC0 1.0 for the metadata (`cc0_data_europa_eu`), from the copyright notice of data.europa.eu
(https://data.europa.eu/en/copyright-notice): "To the extent possible under law, the European Union has waived all
copyright and related or neighbouring rights to metadata of the open data portal". The resources themselves keep the
licence their owners chose; none is read here. Attribution used anyway: "Metadata: data.europa.eu (catalogue
dane.gov.pl)". The terms of dane.gov.pl itself were not read (the host does not answer runners).

Left out: the distributions' files (api.dane.gov.pl does not answer runners; each publisher's data comes from its own
register entry), descriptions, keywords, contact points, machine translations; counts by `issued` (first publication)
and the lookup from register entries to their dane.gov.pl publishers (the entity table holds the institution numbers it
needs).
