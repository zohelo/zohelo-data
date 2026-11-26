# GDELT DOC 2.0 API (`gdelt`, topics)

**Implemented (10 Nov 2026, register CD-030; topics layer O32).** Per `gdelt_query` row of the seed
`topic_signal.csv` (an English query with `sourcecountry:PL`; GDELT searches machine-translated coverage), two
small JSON timelines a day from `https://api.gdeltproject.org/api/v2/doc/doc`: `mode=timelinevolraw` (matching
articles per day and all monitored articles, `norm`) and `mode=timelinetone`; 3 months (the API's window) on the
first landing, 2 weeks afterwards. Files are built by `ingestion/to_landing/gdelt/` from the seed and landed
through the shared `bulk_file` pattern. GDELT allows one request every 5 seconds and refuses shared GitHub runner
addresses for long stretches (HTTP 429, Dev probes 42784542844, 42785760450: one of six queries answered after
back-off), so the daily run is best effort: 6 s between requests, a different first topic each day, stop after 3
refused files in a row or 40 minutes; stale topics show in `gold.topic_quality`. Bronze `bronze.gdelt_timeline`
(accumulating), Silver `silver.topic_volume` (newest value per topic and day; volume_share = volume / norm).
Terms: unrestricted use with a citation of the GDELT Project and a link (gdeltproject.org/about.html).
