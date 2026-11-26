# OpenData.org (BrightQuery) (`opendata_org`)

**Extraction (owner decision 25 Oct 2026, #906: keep and run through the whole process).**
BrightQuery publishes OpenData.org snapshots in the public Google Cloud Storage bucket
`bq-public-datastore`: `opendata/ODO-SENZING-<date>/full/ODO_SENZING_<date>.zip` (newest
5 Mar 2026, 21.4 GB) plus per-type ZIPs, listed by the GCS JSON API with size, MD5 and update
time (no account). The ZIP holds 2,496 newline-delimited Senzing JSON files: Organization/
(501 files, 44 GB), PeopleBusiness/ (494, 46 GB) and Locations/ (1,501, 68 GB); each record is
`{DATA_SOURCE, RECORD_ID, bq_dataset, FEATURES: [ {...}, ... ]}`. Coverage: United States
(86 M organizations, 101 M business contacts, 142 M locations, per the publisher). The publisher
states no licence; the data includes names of people (business contacts, LinkedIn URLs).
Entity = one member file; Bronze keeps the records as published, Silver
(`warehouse/entity/opendata_*.sql`) takes the main Senzing features per record type. A monthly
run (owner, 25 Oct 2026) checks the bucket; a new snapshot (~21 GB) replaces all files.
