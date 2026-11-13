# RPWDL register of healthcare providers and practices (`rpwdl`)

**Built (13 Nov 2026, register PLS-020, #2958).** The "Pliki do pobrania" section of rpwdl2.ezdrowie.gov.pl
(Minister Zdrowia / Centrum e-Zdrowia) offers one ZIP of CSV files per register type and ledger state; its buttons call
the public endpoint `/api/rpwdl-public-resource/ledgers-zip/initial?fileTypeInsideZip=CSV&activityType=<type>&activeLedgers=<true|false>`
(no login, no session). Route: 18 fixed URLs (healthcare providers and 8 practice registers, active and inactive),
`bulk_file` pattern with a 5 s pause between downloads (`ingestion/to_landing/rpwdl/`). Entity = one ZIP
(`<register>_<active|inactive>`), version = download time (no Last-Modified), written only when the SHA-256 changed;
restart unit: one ZIP. About 104 MB a run (November 2026). Weekly schedule (Tuesday 04:25 UTC).

Bronze (`ingestion/to_bronze/rpwdl/load.py`) reads only allowlisted columns: `rpwdl_providers` (podmioty.csv),
`rpwdl_establishments` (zaklady.csv), `rpwdl_units` (jednostki.csv), `rpwdl_cells` (komorki.csv, with beds) and
`rpwdl_practices` (praktyki.csv of the 8 practice registers). Never read: names, NIP, decision and business-register
numbers, street addresses, postcodes, phone, e-mail, web addresses, and the members `wspolnicy.csv` (practitioners'
names and licence numbers) and `adresy_swiadczen.csv` (places of practice). A changed header rejects the ZIP. Landing
and the Archive keep the ZIPs as received (newest plus 30 days).

Silver (`warehouse/models/silver/rpwdl/`): one row per provider ledger, establishment, unit, cell and practice entry
of the newest files; the text `NULL` and `brak wpisu` are missing values; dates and counts typed (other text fails the
build); voivodeship = first two digits of the TERYT code; `is_active` = active ledger file, not removed, not ended on
the snapshot (download) day. Gold: block `register` (`poc_build_rpwdl`, annual, date = 1 January of the snapshot
year, latest-only): active providers, establishments, organisational units, beds in organisational cells, practices
(also by register), Poland and 16 voivodeships; entity tables `healthcare_provider_rpwdl` and
`healthcare_establishment_rpwdl` (keys book_id, establishment_id, REGON, TERC keys). Draft KPIs
`healthcare_providers`, `medical_practices`.

Left out: services per cell (swiadczenia.csv, 1.1 M rows), inspections (kontrole), accreditations (akredyt_*),
places of practice, the practitioners' professions and specialisations (in `wspolnicy.csv` next to their names),
names of providers and units (natural persons cannot be told apart reliably), snapshot history (one year per series).
Licence: unresolved, no reuse terms stated (register PLS-020 quotes the RPWDL 2.0 user guide); internal only.
