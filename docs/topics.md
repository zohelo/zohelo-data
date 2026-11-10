# Topics: what is being talked about, next to what the data says

Owner decision O32 (10 November 2026). The platform tracks what the news and the public are talking about and
ranks topics as hot for the day, week, month, quarter and year. Each topic is linked to the KPIs that measure it,
so the portals can show "what is being said, and what the data says" and put hot topics first. The data layer
(Bronze → Silver → Gold → published) is unchanged; topics are a second kind of input, attention signals, processed
the same way.

## Principles

- **Rules, not a model, decide the ranking.** Matching headlines to topics uses keyword dictionaries; the heat
  score is a documented formula. A language model may later suggest dictionary entries or classify headlines the
  rules miss, but it never writes text that is published and never changes a score directly.
- **Links, not content.** From news sites we keep the headline, link, outlet, language and publication time. Never
  the article text, never an image. Headlines are shown with the outlet's name and link to the article (the press
  publishers' right of EU directive 2019/790 art. 15, in Polish law since the 2024 copyright amendment, covers
  more than very short extracts; a headline with a link stays within it). Only feeds the outlet publishes for syndication
  (RSS/Atom) are read; robots.txt and each feed's terms are recorded in the register entry.
- **A topic does not need data.** "Cła USA" can be hot before any KPI measures it; the portal then shows the
  headlines and says that no indicator is linked yet.
- **Internal first.** Rankings run for at least two weeks without being shown; the owner reviews them, then the
  portals show them (zohelo.com publicly, bi.zohelo.com behind sign-in).

## Sources (register family J, all `current: true`)

| Signal | Source | What we keep | Refresh |
|---|---|---|---|
| Official releases | Release calendars of GUS, Eurostat (euro indicators), NBP (MPC meetings, publications), Ministry of Finance, ECB; the existing seed `release_calendar.csv` stays as the manual fallback | date, source, title, link, linked KPIs | daily |
| News headlines | RSS/Atom feeds of Polish business and general news: at the start Bankier.pl, Money.pl, Business Insider Polska, Rzeczpospolita (ekonomia), Puls Biznesu, Forsal, PAP (free feed), Notes from Poland (EN); more by register entry | title, link, outlet, language, published_at, feed | hourly |
| News volume | GDELT 2.0 DOC API (CD-030): per topic query, daily article volume and tone in Polish-language and Poland-located coverage | topic, day, volume, normalised volume, tone | daily |
| Public attention | Wikimedia pageviews REST API (CD-025): daily views of the Polish (and English) Wikipedia articles listed per topic | article, day, views | daily |

As built: source system `news_feeds` (TP-001…TP-008, one register entry per outlet; PAP parked, its feed is
behind bot protection), feeds every 3 hours, loaded and built in the evening full dataflow (21:40 UTC); routes
and terms in [sources.md](sources.md).

**Storage.** Landing keeps exact source bytes (docs/landing.md), so a feed copy still holds the outlet's
description snippets; Bronze and every later layer keep only title, link, outlet, language and time. The raw
copies are moved to the Archive and, because Bronze holds them, archive retention keeps them (about 1 GB a year
compressed). Deleting raw copies once Bronze holds them is a retention change beyond the current policy and
waits for the owner.

Not now: Google Trends (no general public API), X/Reddit/TikTok APIs (paid or restricted; CD-017…024 stay
registered), PAP Biznes paid services (CD-008).

## Topics and their links (seeds)

| Seed | Columns |
|---|---|
| `topic.csv` | topic_key, name_pl, name_en, description_pl, description_en, parent_key, sort, status (draft/active) |
| `topic_kpi.csv` | topic_key, kpi_key (v2 key), role (`main` = the KPI that answers the topic, `related`) |
| `topic_keyword.csv` | topic_key, lang, pattern (case-insensitive, matched on word stems: `inflacj`, `drożyzn`, `stóp procentow`), weight, exclude (patterns that cancel a match) |
| `topic_signal.csv` | topic_key, signal (`gdelt_query`, `wiki_article`), value (the GDELT query; the Wikipedia article title with language) |

The first set covers the zohelo-bi slice and the themes Polish economic news returns to: inflation and prices,
interest rates and the NBP, wages, labour market, unemployment, the złoty, GDP and growth, public finances and
debt, housing and mortgages, energy prices, demography and migration, EU funds (KPO), trade and tariffs, industry,
retail sales, consumer and business sentiment, regional economy. About 30 topics, each with at least one main KPI
where the platform has one.

## Matching

A headline matches a topic when one of the topic's patterns occurs and none of its exclusions does. A headline can
match several topics. Each match keeps the pattern that hit, so a wrong match can be traced to a dictionary line.
Unmatched headlines are kept (for dictionary work) and counted in `gold.topic_quality`.

## Heat score

For each topic and day *d*, three signals are turned into robust z-scores against the topic's own trailing 90 days
(median and median absolute deviation, MAD floor 1):

- `news` = headlines matched on *d* (weighted by pattern weight),
- `volume` = GDELT normalised volume on *d*,
- `attention` = Wikipedia views on *d* (sum over the topic's articles).

`heat(d) = 0.5·z_news + 0.3·z_volume + 0.2·z_attention + release(d)`, where a missing signal drops out and the
weights of the others are rescaled; `release(d)` = 1 on the day of an official release of one of the topic's main
KPIs and 0.5 on the day before. Weights live in `warehouse.yaml` (`topics:` block), not in code.

Ranking per period, on the day of the run:

| Period | Window | Score |
|---|---|---|
| day | today | heat(today) |
| week | last 7 days | mean heat |
| month | last 30 days | mean heat |
| quarter | last 91 days | mean heat |
| year | last 365 days | mean heat × share of days with heat > 1 (rewards topics that stayed hot) |

A topic enters a period's list only with at least 3 matched headlines in the window (day: 2). Ties are broken by
headline count. The trend arrow compares the window with the window before it.

## Models

Bronze `bronze.news_items`, `bronze.gdelt_timeline`, `bronze.wiki_pageviews`, `bronze.release_calendar_items` →
Silver `silver.news_headlines` (deduplicated by canonical URL, then by outlet + normalised title),
`silver.topic_volume`, `silver.topic_attention`, `silver.releases` → Gold `gold.topic_matches`,
`gold.topic_daily` (topic × day: signals, z-scores, heat), `gold.topic_rank` (period × rank), `gold.topic_quality`
(feed freshness, matched share, topics without signals). Headlines are kept for 400 days, then dropped (enough for
the year ranking); scores are kept.

## Published

`06_published/v2/topics.json`, contract in [published.md](published.md) ("topics.json").

## Quality checks (the run fails, or warns, as stated)

- every active feed delivered items in the last 48 hours (warn), none for 7 days (fail and alert),
- matched share of headlines between 10% and 90% (warn outside),
- every active topic has at least one signal configured (fail),
- `topics.json` within its size limit and schema (fail).
