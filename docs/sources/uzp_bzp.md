# e-Zamówienia BZP: contract and result notices (`uzp_bzp`)

**Built (16 Nov 2026, register PLG-022, #2981).** Route: the BZP read API of the e-Zamówienia platform
(`GET https://ezamowienia.gov.pl/mo-board/api/v1/notice`, free, no access application; instruction "Załącznik 3"
on https://ezamowienia.gov.pl/pl/integracja/), one request at a time, 2.5 s apart (stated limit 30 requests a
minute). There is no bulk download. One notice type per request, at most 500 notices a page, `SearchAfter` = the
last `objectId` for the next page; `notice/stats` gives the count per type and day. Measured (Dev probes
43797263601, 43797627130, 43797993730): `PublicationDateTo` reaches 24 hours past the time given, so a day D is
asked with From = To = D 00:00:00 and every answer is filtered to the UTC day D, then checked against
`notice/stats` (a mismatch keeps the day out of Landing and fails the run).

Selection: contract notices (`ContractNotice`) and result notices (`TenderResultNotice`) published from
2026-01-01 (about 600 and 700 a working day; 2025: 142,424 and 148,561). Left out: the 13 other notice types
(contract performance, notice and contract changes, intentions, competitions, concessions, notices outside the
Act), notices of 2021-2025 (the platform has them from 2021), and TED notices (PLG-023; overlap is by procedure
id, never summed). Entity: one notice type on one UTC publication day (`contract_<YYYYMMDD>`,
`result_<YYYYMMDD>`); version: the day; the newest three days are asked again, unchanged bytes are skipped.

**Reduced before Landing** (as news feeds, O33): the notice form `htmlBody` (about 95% of the bytes) holds contact
persons, e-mails, telephones and the winning contractors' names and addresses; it is replaced by the template-
labelled value lines (estimated value, value per part, outcome per part, offers received, price of the chosen offer,
contract date, contract value). Contractors keep only province and country; a subsidised contracting authority
(client type 3, possibly a natural person) keeps no name, city or ids; an e-mail address written into a kept
free-text field (subject, authority name or city) becomes `[e-mail]`. The receipt keeps the SHA-256 and size of
every original answer page. Unknown fields fail the day.

Bronze `bronze.uzp_bzp_notices` (one row per notice, text). Silver `silver.uzp_bzp_notices` (grain: BZP notice
number; amounts parsed from "<amount> <currency>" with a decimal comma, unparsed or ambiguous ones stay null with
their line counts; `tests/uzp_bzp_notices_days_complete.sql` checks each type-day against `notice/stats`). Gold:
`gold.build_uzp_bzp` (block `notices`: `uzp_bzp:notices:notices` by `notice_type`, and
`uzp_bzp:notices:PLN:contract_value`, the sum of the 8.2 contract values of the month's result notices; complete
months; Poland and the contracting authority's voivodeship, `PL02`…`PL32` = `TERC:02`…`TERC:32`) and
`gold.uzp_bzp_notice` (entity table keyed by the notice number). KPIs `bzp_notices` and `bzp_contract_value`
(draft). Terms: no reuse or attribution clause found (API regulations of 24 Nov 2023); licence `unresolved`,
internal. Refresh full (D32). Schedule weekly (O46): Saturday 02:47 UTC, Landing and Bronze (the missing days up
to Friday, about 14 files; the API keeps every past day, so nothing is lost between runs); Silver and Gold come
from the platform's Sunday build. Restart unit: one notice type on one day.
