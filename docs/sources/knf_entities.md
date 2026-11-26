# KNF supervised entities, route 1: ESMA registers (`knf_entities`)

**Built (register PLG-016, #2994).** The capital-market entities KNF supervises are republished by ESMA in its
registers; the documented application-to-application interface (https://registers.esma.europa.eu/publication/helpApp)
is a Solr core, `esma_registers_upreg`, answered as JSON. Route: the shared `bulk_file` pattern, one request for every
parent entry whose home member state is Poland (`fq=ae_homeMemberState:"POLAND"`, `fq=type_s:parent`), sorted by id,
without the response header and with a field list that leaves out addresses, websites, comments and the index's own
timestamps; up to 1000 rows (623 entries on 26 Nov 2026), and Bronze rejects an answer whose `numFound` exceeds the
documents returned. Entity = the Polish slice (`esma_upreg_pl`, one file); version = download time (no Last-Modified or
ETag), written only when the SHA-256 changed; restart unit: the file. Weekly, Thursday 16:15 UTC (O46).

Bronze `bronze.knf_entities_esma_entries` (`ingestion/to_bronze/knf_entities/`; the layout is checked first, DuckDB
reads the documents with declared types; another layout rejects the file into `bronze.knf_entities_rejected_files`):
one row per entry, text as published. Silver `silver.knf_entities_esma_entries` (grain: ESMA id `entry_id`): entity
type, office type (head office or branch), status, authorisation or notification date, withdrawal date, LEI and head
office LEI where filled (71 entries carry an LEI; a branch may carry its firm's LEI), competent authority, host member
states of branches, number of funds of an AIFM. Gold: entity table `gold.knf_supervised_entity` keyed by the ESMA id
(joins GLEIF by LEI only; entries without an LEI are kept, never matched by name); build stage `build_knf_entities`:
entries by status with entity type and office type as breakdowns at the download day (block `register_entries`, D,
latest-only), and entries by year of authorisation (`authorisations`, A) and of withdrawal (`withdrawals`, A), Poland
only (the register has no coded location). No KPI (register `kpi: false`).

Licence: open-conditional (`esma_registers_notice`): the registers site's own legal notice
(https://registers.esma.europa.eu/publication/legalNoticePage) authorises reproduction with the source acknowledged,
buyers of sold documents told the data is free from the registers, and a transformation stated.

Left out: KNF's own HTML entity lists below https://www.knf.gov.pl/podmioty (route 2: banks, credit unions, insurers,
pension societies; waits for the owner question in the register entry); the KNF register applications; the ESMA child
documents (services and activities per entry, funds per AIFM) and the other ESMA cores; addresses of every entry.
