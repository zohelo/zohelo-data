# UOKiK decisions (`uokik_decisions`)

**Code built, not scheduled; register PLG-020 (decisions) and PLG-021 (concentration decisions) `reviewing` (#3318,
#3319).** The host refuses TCP connections (ports 443 and 80) from GitHub's runners since 26 Nov 2026 (Dev runs
45814788158 to 45838779384, 45922861093, 45924755912), after it answered on 12 Nov (Dev run 43288930412); no other
public copy of the index was found (dane.gov.pl lists no UOKiK decision dataset). The owner question is the first
blocker of PLG-020. Nothing below has read real data yet: the cell labels, the row counts and the ceilings are
unverified.

Route: the database of decisions of the President of UOKiK, https://decyzje.uokik.gov.pl/bp/dec_prez.nsf, a Lotus
Domino database; its view `WWW-wszystkie` as JSON (`?ReadViewEntries&OutputFormat=JSON&start=<position>&count=<n>`),
categorised by subject area, then year, then one entry per decision whose cell is an HTML fragment starting "Numer
decyzji:". Only this host is read (robots.txt HTTP 404); uokik.gov.pl, whose robots.txt names ClaudeBot and other AI
crawlers, is never requested, and no decision PDF is downloaded. One request at a time, 1 s apart, timeouts 45 s /
180 s, at most 3 tries; a refusal, timeout or unknown layout stops the run and is reported. Code:
`ingestion/to_landing/uokik_decisions/extract.py`.

Entity = one subject-area category (`category_<slug>`), read from its own view position in pages of 500 until the next
category; the entries read must equal the category's `@descendants`. Reduced before Landing: of each decision only
the view's `@unid` and `@noteid`, the category, the year category and the cell's decision number and date are kept,
each checked against a closed shape (the number: a signature in capitals, a dash or space, digit groups, optional
initials; no free words), and an unlabelled text node after a kept value stops the run as an unknown layout; the
parties and the free-text department label are never stored (a party can be a natural person, a sole trader). Version = download time; a category's document is written only when its SHA-256
changed. Restart unit: the category (a run left short by its time budget reads the categories without a copy, then the
oldest copies, first).

Bronze `bronze.uokik_decisions_index` (`ingestion/to_bronze/uokik_decisions/`, shared loader; another document shape
is rejected into `bronze.uokik_decisions_rejected_files`): one row per decision, text as listed. Silver
`silver.uokik_decisions` (grain `@unid` x category: a categorised view lists a multi-category decision once per
category): number, department signature (the letters that start the number), date,
year, category, link; `is_concentration` = the signature DKK (Departament Kontroli Koncentracji). Gold: entity table
`gold.uokik_decision` (keyed by `@unid`, its categories joined); build stage `build_uokik_decisions`, block `decisions` (decisions per year,
in total and by category) and block `concentration` (concentration decisions per year), Poland, frequency A; years
without a decision have no row, the current year is provisional. kpi false. Schedule once reachable: weekly,
Wednesday 16:40 UTC (O46; the slot is written in the source YAML). Licence: `unresolved` (UOKiK's terms would be on
uokik.gov.pl, which is not read): internal only.

Left out: party names, the decision PDFs and anything in them (fines, legal bases), notifications that did not end in
a published decision, and the pages of uokik.gov.pl (including https://uokik.gov.pl/koncentracje).
