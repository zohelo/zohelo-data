# Wikimedia pageviews (`wikimedia_pageviews`, topics)

**Implemented (10 Nov 2026, register CD-025; topics layer O32).** Per `wiki_article` row of `topic_signal.csv`
(`<lang>:<Title>`), daily user views from the pageviews REST API
(`https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/<lang>.wikipedia.org/all-access/user/<Title>/daily/<start>/<end>`),
455 days on the first landing, 14 days afterwards, 0.2 s apart, with the platform's user agent (Wikimedia's
User-Agent policy). A 404 (renamed article) is a warning. Bronze `bronze.wiki_pageviews` (accumulating), Silver
`silver.topic_attention` (views summed over a topic's articles per day). Pageview data: CC0 1.0.
