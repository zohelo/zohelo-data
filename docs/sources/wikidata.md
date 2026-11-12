# Wikidata: Polish territorial units by TERC (`wikidata`)

**Built (12 Nov 2026, register CD-026, #2843).** Route: the Wikidata Query Service
(`https://query.wikidata.org/sparql`), six queries in `ingestion/to_landing/wikidata/queries.rq`, one at a time,
5 s apart, user agent naming the project and https://zohelo.com (Wikimedia User-Agent policy); the entity dumps
would be over 100 GB for some 3,500 items. Entity: one selection (`terc_units`, `population`, `area`,
`coordinates`, `website`, `names`), each holding every statement (any rank) of every item with a TERC code (P1653).
Version: download time (no Last-Modified); an unchanged answer is skipped by SHA-256. Bronze
`bronze.wikidata_bindings` (one row per SPARQL solution, text). Silver `silver.wikidata_statements` (one row per
statement, qualifiers folded) and `silver.wikidata_terc_attributes` (one row per current TERYT code × property; one
statement chosen by preferred rank, latest point in time, statement id as the last tie-break, the rest counted in
`candidates` and `tied`; a code held by several items is kept only when exactly one is preferred, and
`tests/wikidata_terc_several_items.sql` warns on each such code). Landing parses each JSON answer before it lands
(`format: json`); queries are ordered by every variable so unchanged data gives the same bytes. Gold `gold.geo_unit_wikidata` (one row per `TERC:<code>`: QID, labels
pl/en, population and its date, area as published, coordinates, website, Wikipedia articles pl/en): attributes,
not a KPI block; GUS and PRG stay the statistical sources. Dev trial 43288241509: 6 files (11.8 MB), 21,028
bindings, 3,508 items; 3,149 current TERYT codes matched (11 of 16 voivodeships, 366 of 380 powiats, 2,772 of
3,964 gmina-level codes; 445 Wikidata codes of other shapes and 16 gmina codes not in TERYT are left out, and the
powiat code 1463 held by two items without a preferred one); population for 1,580 units. Terms: CC0 1.0 for structured data
(https://www.wikidata.org/wiki/Wikidata:Copyright); licence row `cc0_wikidata`. Refresh full (D32), weekly.
Restart unit: one selection.
