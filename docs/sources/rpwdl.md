# RPWDL register of healthcare providers and practices (`rpwdl`)

**Built (13 Nov 2026, register PLS-020, #2958).** The "Pliki do pobrania" section of rpwdl2.ezdrowie.gov.pl
(Minister Zdrowia / Centrum e-Zdrowia) offers one ZIP of CSV files per register type and ledger state; its buttons call
the public endpoint `/api/rpwdl-public-resource/ledgers-zip/initial?fileTypeInsideZip=CSV&activityType=<type>&activeLedgers=<true|false>`
(no login, no session). Route: 18 fixed URLs (healthcare providers and 8 practice registers, active and inactive),
`bulk_file` pattern with a 5 s pause between downloads (`ingestion/to_landing/rpwdl/`). Entity = one ZIP
(`<register>_<active|inactive>_<year>`), version = download time (no Last-Modified), written only when the SHA-256
changed; restart unit: one ZIP. About 104 MB a run (November 2026). Weekly schedule (Tuesday 04:25 UTC).

Years (#3139). The register has no history, so each year keeps its own state: every ZIP is declared with
`years: {from: 2026, open: 1}`, a run downloads only the current year's 18 ZIPs (Europe/Warsaw year; required, a
refused download fails the run) and a past year is closed: never downloaded again, its newest copy (the last weekly
load of that year) is the year-end state. The first run of a year always lands its 18 ZIPs, even when the bytes are
those of December: the new year's entity holds no copy yet, and Bronze treats a file as loaded per entity
(`bulk_bronze` `per_entity`), so the same bytes are loaded again for the new year. A past year without a copy stays
empty (today's register is not that year's state). Snapshot day: the download day of the copy, i.e. the day these
bytes were first read; a later run that finds the same bytes writes nothing (idempotent Landing) and the day stays,
because the endpoint caches generated ZIPs and the same bytes show the same register state, not a newer one. Copies
landed before #3139 under entities without a year are left out by Silver.

Privacy (register PLS-020: aggregate counts only). A healthcare provider can be one natural person (sole trader) and
an individual practice is one practitioner, so no table keeps an identifier of a provider or a practice: no name,
NIP, REGON, book number, ledger id or practice register id (pseudonymous identifiers of a natural person are still
personal data). Bronze (`ingestion/to_bronze/rpwdl/load.py`) reads only allowlisted columns:
`rpwdl_providers` (podmioty.csv), `rpwdl_establishments` (zaklady.csv), `rpwdl_units` (jednostki.csv), `rpwdl_cells`
(komorki.csv, with beds) and `rpwdl_practices` (praktyki.csv of the 8 practice registers). The seat TERYT of a
provider and the correspondence TERYT of a practice reach Bronze cut to the voivodeship (two digits); establishments,
units and cells (places of care) keep their own ids and TERYT code. Never read: names, NIP, REGON, decision and
business-register numbers, street addresses, postcodes, phone, e-mail, web addresses, and the members `wspolnicy.csv`
(practitioners' names and licence numbers) and `adresy_swiadczen.csv` (places of practice). A changed header rejects
the ZIP. Landing and the Archive keep the ZIPs as received (newest plus 30 days), `wspolnicy.csv` included: an open
owner question (#3117), with the licence. Person-level records (sole traders) are not loaded without an owner decision.

Silver (`warehouse/models/silver/rpwdl/`): one row per provider ledger, establishment, unit, cell and practice entry of
the newest files of each snapshot year (grain: the file row for providers and practices, snapshot year x id for the
places of care); the text `NULL` and `brak wpisu` are missing values; dates and counts typed (other text fails the
build); voivodeship = first two digits of the TERYT code when `silver.teryt_terc` has that voivodeship; `is_active` =
active ledger file, not removed, not ended on the snapshot (download) day. Gold: block `register` (`poc_build_rpwdl`,
annual, date = 1 January of the snapshot year; the current year moves with each weekly load, a past year keeps its
year-end state): active providers, establishments, organisational units, beds in organisational cells, practices (also
by register), Poland and 16 voivodeships; counts only, no entity tables. Draft KPIs `healthcare_providers`,
`medical_practices`.

Left out: services per cell (swiadczenia.csv, 1.1 M rows), inspections (kontrole), accreditations (akredyt_*), places of
practice, the practitioners' professions and specialisations (in `wspolnicy.csv` next to their names), names of
providers and units (natural persons cannot be told apart reliably), history before November 2026 (the register
publishes none) and states within a year (one per year).
Licence: unresolved, no reuse terms stated (register PLS-020 quotes the RPWDL 2.0 user guide); internal only.
