# Senate of the Republic of Poland (`senat`)

**Reviewing (register PLG-025, #482; route decision D43): code built, not scheduled.** www.senat.gov.pl is behind
Cloudflare, which answered two of four Dev runs from GitHub runners with its bot check (HTTP 403 "Just a moment..." on
`/robots.txt`); the extractor stops there and never works around it, so the weekly slot (Wednesday 19:20 UTC) waits for
the owner's answer (register entry, first blocker). No API or feed. The one export, the vote results per term as
XML (dane.gov.pl dataset 4648 "Głosowania Senatu", CC0 1.0, file `senat_kadencja_11.xml`), sits under `/gfx/`, which
robots.txt disallows for every agent, so it is not read until the owner decides (#482). Term XI is read from server-rendered
public pages of www.senat.gov.pl that robots.txt allows, with plain HTTP GET (no browser session), one connection, one
request at a time at least 1.1 s apart, a clear user agent, robots.txt read first in every run; a 401/403/429 or a
bot-protection page stops the run (`ingestion/to_landing/senat/`). Pages (entity = one page, version = download time,
written only when the SHA-256 changed): the sittings list (`sittings_page_<nn>`, every run), the votes page of each
sitting day (`day_<id>_<day>`: once, again while the sitting runs and for the two newest finished sittings), the print
page of each vote with its totals (`vote_<id>`: once; the day page has no totals) and the senators list (`senators`,
every run). Restart unit: one page; a run stops at 2,000 requests or 75 minutes and the next run continues (first
load about 3,500 pages, two or three weekly runs). Never read: `/gfx/`, `/pdf/`, stenograms, speeches, per-club vote
pages, senator profiles and photos, bip.senat.gov.pl.

Bronze (`ingestion/to_bronze/senat/`, strict parsers in `ingestion/to_landing/senat/pages.py`; an unknown layout is
rejected into `bronze.senat_rejected_files`): `senat_sittings`, `senat_day_votes`, `senat_vote_results` (totals and
the number of senators per printed mark; names not loaded), `senat_senators`. Silver: `senat_sitting_days` (date of
each day read from the printed dates), `senat_votes` (totals empty until the vote's page is read), `senat_senators`
(name and end of mandate only). Gold: entity tables `senate_sitting_days`, `senate_votes`; build stage `build_senat`
(blocks `sitting_days`, `votes`: counts of finished sittings per month and per year, Poland, only periods with a
sitting); draft KPIs `senate_sitting_days`, `senate_votes` (no v2 map row). Once scheduled: weekly, Wednesday 19:20 UTC (O46: the source run ends
with Bronze, the Sunday platform run builds Silver and Gold; until the first Bronze load the build declares the blocks
waiting). Licence:
re-use terms of the site (free, source and processing named; commercial reuse not addressed): `senat_psi_reuse`.
Left out: individual senators' votes and attendance, per-club results, bills and their texts, committees, terms
before XI.
