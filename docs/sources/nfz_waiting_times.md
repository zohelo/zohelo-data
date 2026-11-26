# NFZ waiting times, API Terminy Leczenia (`nfz_waiting_times`)

**Built (register PLS-015, #2956).** NFZ publishes the waiting lists of contracted providers only through the API
Terminy Leczenia (v1.4 at `https://apinfz.nfz.gov.pl/app-itl-api-pcus/`; v1.3 at `api.nfz.gov.pl/app-itl-api` is
marked obsolete); there is no bulk file and the API keeps no history and gives no update time. Route:
`ingestion/to_landing/nfz_waiting_times/`, `/queues` for one case (1 stable, 2 urgent) and one NFZ voivodeship
branch (01-16) at a time, 25 records a page, one request at a time with a 0.3 s pause (the terms allow 10 a second
per IP), bounded retries. The site runs bot protection; a challenge or HTTP 403 fails the run and is never worked
around. Entity = one case × branch in one snapshot month (`queues_<YYYYMM>_case<c>_<pp>`, 32 a month), all pages
landed as one ZIP of the exact answers; version = time the read started; restart unit: one entity. A month is read
whole on the 20th (about 2.5 hours, about 6,000 pages); a second slot on the 27th (a week later: at most one scheduled run per week, owner decision of 25 Nov) reads only the entities of the
month still missing.

**Layers.** Bronze `bronze.nfz_waiting_times_queues`: one row per queue record, values as text. Silver
`silver.nfz_waiting_times_queues` (grain queue_id, the newest complete month: all 32 entities present, so a month cut
short is never shown half), `silver.nfz_waiting_times_providers` (entity table, grain branch × provider code) and
`silver.nfz_waiting_times_monthly` (counts per branch and Poland, case and month, accumulating one month per run).
Missing counts stay null and are not summed. Gold block `waiting_lists` (`gold.build_nfz_waiting_times`, registered
in `warehouse/macros/sources/nfz_waiting_times.sql`), monthly, by NFZ branch (= voivodeship, NFZ code n = TERYT 2n)
and Poland, per case: waiting lists listed in the snapshot month, persons awaiting at the end of the statistics month
summed over the lists, and the number of lists summed. **These are zohelo calculations.** A list shown at several
places (`many_places` Y) enters once when all its places give the same count and not at all when they differ. A
branch may list a place across the voivodeship border (8 of 148,144 queues in November 2026); the counts follow the
branch. Draft KPIs `waiting_list_persons`, `waiting_lists` (stable case).

Left out: the forecast time to the benefit (published text, not aggregated), the accessibility attributes in Gold
(kept in Silver), the history before the first scheduled run (the API offers none).

**Licence.** "Regulamin API Terminy Leczenia" on the statute page of the v1.4 API
(`swagger-ui/statute.html`, the same clauses as the v1.3 terms): the source must be given with the address
https://api.nfz.gov.pl/, "Niedozwolone jest modyfikowanie danych przez Usługobiorcę", 10 requests per second per IP;
no licence name and no clause on commercial reuse. Licence row `nfz_itl_api_terms`.
