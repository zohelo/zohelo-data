# ELI Poland: Dziennik Ustaw and Monitor Polski (`eli_pl`)

**Built (register PLG-026, #483).** The Sejm's public ELI API without key (https://api.sejm.gov.pl/eli.html); no bulk
file. The year listing `/eli/acts/<DU|MP>/<year>` returns every act of the journal year in one answer (count =
totalCount = items), so one request per journal year is a full load. robots.txt (`Allow: /`) is read first in every
run; one request at a time, 2 s apart, bounded retries (`ingestion/to_landing/eli_pl/`, landed by the shared bulk_file
pattern). Entities (version = download time, written only when the SHA-256 changed): `publishers` (`/eli/acts`, every
run) and `acts_<DU|MP>_<year>` for every year that answer lists. The current and the previous year are read every
run; an older year once and again every fourth week (year % 4 = ISO week % 4), so status changes of old acts arrive
within four weeks. Restart unit: one journal year. Size and history: `ingestion/sources/eli_pl.yaml`.

Bronze (`ingestion/to_bronze/eli_pl/`; an incomplete year answer, an unknown key or an item of another journal or
year rejects the file into `bronze.eli_pl_rejected_files`): `eli_pl_publishers`, `eli_pl_acts`. Silver: `eli_pl_acts`
(one row per act, grain ELI id). Gold: entity table `legal_acts`; build stage `build_eli_pl` (block `acts`: acts per
month by promulgation date, breakdowns journal × type, `_T` totals; only months with an act); draft KPI
`legal_acts_published` (both journals, all types; no v2 map row). Weekly, Sunday 04:20 UTC. Licence: `unresolved`
(the API page states no terms; legal acts are not protected by copyright, Polish Copyright Act art. 4, but the
database terms are not stated), internal only.

Left out: act texts (PDF, HTML), the per-act details (entry into force, in force, references, keywords, authorised
body; one request per act, about 165,000), the changed-since list `/eli/changes/acts`, other ELI publishers.
