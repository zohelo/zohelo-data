# Open KRS API, Ministry of Justice (`ms_krs`)

**Built (12 Nov 2026, register PLG-012, #1362).** Route: the Open API of the National Court Register
(`https://prs.ms.gov.pl/krs/openApi`). It answers one KRS number per request,
`https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/<krs>?rejestr=<P|S>&format=json` (P = entrepreneurs, S =
associations and other organisations; 200 with JSON, 204 when that register holds no current extract), and
`https://api-krs.ms.gov.pl/api/Krs/Biuletyn/<YYYY-MM-DD>` lists the KRS numbers with an entry on a day. No bulk
file, no list of numbers, no published limit, no robots.txt on the API host (Dev probe 43320344152: 54 extract
requests, median 0.66 s, 5–63 KB each; bulletins answer back to June 2022).

**Selection.** The KRS numbers of `silver.gleif_legal_entities` (registration authority `RA000484`, an id of
exactly ten digits): 31,892 numbers on 12 Nov 2026. Left out: every other KRS entity (several hundred thousand;
no authorised list of numbers is loaded yet), the 6 GLEIF ids that are not ten digits, full extracts
(`OdpisPelny`) and history. Numbers that leave GLEIF stay loaded. The counts in Gold are counts of what is
loaded, never of Polish companies.

**Personal data: reduced before Landing.** A full answer names board members, supervisors, partners, proxies,
liquidators and trustees, with PESEL. `ingestion/to_landing/ms_krs/extract.py` copies only this allowlist into a
new record and drops the rest in memory; nothing else reaches Landing, the Archive, Bronze or a log (owner rule
O33 is the precedent: Landing holds the reduced record, and its SHA-256 is that of the reduced bytes).

| Kept | From the answer (`odpis.…`) |
|---|---|
| `krs_number`, `register`, `extract_kind` | `naglowekA.numerKRS`, `naglowekA.rejestr`, `rodzaj` |
| `registered_on`, `last_entry_on`, `last_entry_number`, `state_as_of`, `position_state` | `naglowekA.dataRejestracjiWKRS`, `dataOstatniegoWpisu`, `numerOstatniegoWpisu`, `stanZDnia`, `stanPozycji` |
| `legal_form`, `name`, `nip`, `regon`, `has_opp_status` | `dane.dzial1.danePodmiotu`: `formaPrawna`, `nazwa`, `identyfikatory`, `czyPosiadaStatusOPP` |
| `seat_country`, `seat_voivodeship`, `seat_powiat`, `seat_gmina`, `seat_town` | `dane.dzial1.siedzibaIAdres.siedziba` (not `adres`: no street, building, postal code, e-mail or website) |
| `share_capital`, `share_capital_currency` | `dane.dzial1.kapital.wysokoscKapitaluZakladowego` |
| `main_pkd` (codes only) | `dane.dzial3.przedmiotDzialalnosci.przedmiotPrzewazajacejDzialalnosci[]`: `kodDzial.kodKlasa.kodPodklasa` |
| `liquidation_entries`, `liquidation_ended`, `bankruptcy_entries`, `bankruptcy_ended`, `restructuring_entries`, `dissolution_entry` | how many entries `dane.dzial6` holds per section and how many carry an end; never the entries (they name persons) |
| `requested_on`, `http_status` | ours: Warsaw date of the request; 200, or 204 from both registers |

Dropped whole: `dzial2` (representation, supervisory body, proxies), partners and shareholders, branches,
`dzial4`, `dzial5`, the contents of `dzial6`, court file references, the time the extract was generated. The
entity's registered `name` is kept as the register prints it; for a partnership it can contain a partner's
surname, which is part of the legal entity's name.

**Landing.** 16 fixed entities `extracts_00` … `extracts_15` (KRS number modulo 16). A file is the complete
current state of its part, one JSON line per number, sorted, so unchanged data gives the same bytes; a part is
written only when its bytes changed. One request at a time, 1 s apart, user agent of the project, three attempts
with back-off on 408/429/5xx and network errors, any other status stops the run (never worked around); at most
120 minutes a run (about 4,300 requests), so the first load takes about ten daily runs, each continuing from
Landing (numbers not held yet, ascending). Restart unit: one KRS number; a part is saved every 500 new records.
Incremental: each run first reads the bulletins after a part's `bulletin_through` (receipt detail) up to
yesterday (Warsaw; at most 45 days, older unread days are reported as a warning) and asks again for every held
number listed on a day not before its last request.

**Bronze** `bronze.ms_krs_extracts`: one row per KRS number of the newest copy of each part, values as text.
**Silver** `silver.ms_krs_entities`: one row per KRS number asked for, typed; `has_current_extract` is false
for a number neither register holds a current extract of. **Gold** `gold.krs_entity`: one row per KRS number
with a current extract (register, legal form, name, NIP, REGON, seat with the voivodeship key of
`dim_geography`, dates, share capital, main PKD code, liquidation and bankruptcy flags); it joins GLEIF by the
KRS number (`registration_authority_id = 'RA000484'`, `registration_authority_entity_id`), other sources by
NIP or REGON, never by name. `gold.build_ms_krs`: measure `ms_krs:lei_selection:entities`, the entities loaded
by legal form and seat voivodeship on the day of the newest request. No KPI. Registrations per year are not
built: over a selection of LEI holders they would mislead.

**Terms.** Not established: the Open API page is a script-rendered application with no terms text in its HTML,
the API host has no robots.txt, and `api.dane.gov.pl` (dataset 27606, "API Krajowego Rejestru Sądowego") did
not answer the runner. Licence row `unresolved`: internal only (licence gate). Refresh full (D32), daily.
