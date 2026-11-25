# News headline feeds (`news_feeds`, topics)

**Implemented (10 Nov 2026, register TP-001…TP-008, family J; topics layer O32, [topics.md](../topics.md)).**
One register entry per outlet; the feeds are complete small files at fixed URLs, landed whole through the
shared `bulk_file` pattern every 3 hours (one request per feed and run; a copy is written only when the
SHA-256 changed). Feeds (Dev probe 42784534992): Bankier.pl `rss/wiadomosci.xml` (RSS, 50 items),
Money.pl `rss/rss.xml` (RSS, 15 items, ~20 h), Business Insider Polska `gospodarka.feed` (RSS, 20 items,
~3 days; the main `/.feed` rolls over in ~3 h), Rzeczpospolita `rss/451-ekonomia` (RSS, 100 items, ~6
days), Puls Biznesu `rss/najnowsze.xml` (RSS, 60 items, local time without offset), Forsal `/.feed`
(Atom, 50 entries), Notes from Poland `/feed/` (WordPress RSS, English). robots.txt allows every feed;
no outlet publishes feed-specific terms, their site terms reserve rights to the content, so only the
headline, link and outlet name are used. PAP (TP-007) is parked: `www.pap.pl/rss.xml` answers automated
clients with a bot-protection page. Bronze `bronze.news_items` is accumulating (every landed copy, not
latest-only; `bulk_bronze.load(all_versions=True)`): title, link, guid, outlet, feed language and
publication time (text and UTC) only, never descriptions, article text or images. Silver
`silver.news_headlines`: deduplicated by canonical URL, then outlet + normalised title on the same day;
400 days. Outlets: seed `news_outlet.csv`. The raw feed copies keep the outlets' descriptions; they stay
in Landing/Archive as provenance (retention keeps what Bronze holds), see topics.md, "Storage".
