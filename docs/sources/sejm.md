# Sejm API (`sejm`)

**Built (register PLG-024, #481).** Public JSON API of the Sejm without key (https://api.sejm.gov.pl/sejm.html); no
bulk file. robots.txt (`Allow: /`) is read first in every run; one request at a time, 2 s apart, bounded retries
(`ingestion/to_landing/sejm/`, landed by the shared bulk_file pattern). Entities (one answer each, version = download
time, written only when the SHA-256 changed): `terms`; per term 9 and 10 `term<t>_proceedings`, `term<t>_voting_days`,
`term<t>_prints`; one `term<t>_votings_<sitting>` per sitting with votes; `term10_mps`, reduced before Landing to id,
active, club, districtNum, districtName and voivodeship (no names, birth data, e-mails, professions or photos land).
Term 9 and the votings of finished sittings are read once; the lists of term 10 and the votings of its two newest
sittings every run. Restart unit: one answer. Size and history: `ingestion/sources/sejm.yaml`.

Bronze (`ingestion/to_bronze/sejm/`, an unknown item key rejects the file into `bronze.sejm_rejected_files`):
`sejm_terms`, `sejm_proceedings`, `sejm_voting_days`, `sejm_votings`, `sejm_prints`, `sejm_mps`. Silver:
`sejm_sitting_days` (numbered sittings only; planned entries numbered 0 are left out), `sejm_votings` (totals as
published; yes, no and abstain empty for list and traditional votes), `sejm_prints`, `sejm_mps`. Gold: entity tables
`parliament_sejm_sitting_days`, `parliament_sejm_votings`, `parliament_sejm_prints`, `parliament_sejm_mps`; build
stage `build_sejm` (blocks `sitting_days`, `votings`, `prints`: counts per month, Poland, only months with an event);
draft KPIs `sejm_sitting_days`, `sejm_votings`, `sejm_prints` (no v2 map row). Weekly, Saturday 04:20 UTC. Licence:
`unresolved` (the API page states no terms), internal only.

Left out: individual MPs' votes (one request per voting, about 9,000 per term), per-option results of list votes,
MP names and other personal data, clubs (their contact data), committees, interpellations and questions, the
legislative processes, the agenda HTML, print attachments and texts, terms 1 to 8; prints by applicant (the API has
no applicant field, only the title).
