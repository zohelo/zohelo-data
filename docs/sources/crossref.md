# Crossref: works with a Polish author affiliation (`crossref`)

**Built (13 Nov 2026, register CD-029, #553).** Route: the Crossref REST API (`https://api.crossref.org`), aggregate
requests only, one at a time, 2 s apart (the API sends `x-rate-limit-limit: 1` per `1s` for `/works` list queries,
5 per 1 s for `/funders`; Dev probe 43326689827), user agent naming the project; no contact address is sent (the
polite pool is optional). The annual public data file is not used: it holds every work's full metadata, abstracts
included, for a selection this route answers in about 30 small requests.

Selection: the works whose deposited author affiliation text names Poland or Polska (`query.affiliation=Poland
Polska`, `rows=0`: only `total-results` and facet counts, never work records, so no titles, abstracts, author names
or ORCIDs reach Landing). Entities: `works_pl_by_year` (total and works per publication year, facet `published`),
`works_pl_<year>` from 2000 to the current year (facets `type-name`, `publisher-name`, `funder-doi`) and
`funders_pl` (Funder Registry entries with `location` Poland). Version: download time (no Last-Modified); an
unchanged answer is skipped by SHA-256. Restart unit: one request.

Bronze `bronze.crossref_facet_counts` (one row per facet value plus a `_total` row per request, counts as text;
a works answer carrying work items is rejected) and `bronze.crossref_funders` (one row per funder; an answer with
more funders than one page holds is rejected). Silver `silver.crossref_works_pl_counts` (entity × facet × value;
`is_truncated` where Crossref cut a facet at 1,000 values) and `silver.crossref_funders` (one row per registry id).
Gold `gold.build_crossref`, block `works_pl`, unit `works` as published: `crossref:works_pl:works` (every
publication year 1900 to the year of the copy) and per year from 2000 the breakdowns `:type`, `:publisher`
(Crossref's publisher name) and `:funder` (registry id; the name as label for Polish funders). The year of the copy
is `provisional`; later years (scheduled issues) and years before 1900 are not loaded. Draft KPI
`scholarly_works_pl_affiliation`.

Limits: only works whose publisher deposits affiliations are counted, so the totals are a lower bound of Polish
output; a field query also matches an affiliation abroad that names Poland; breakdowns are not additive (several
funders per work, none for most) and facets stop at 1,000 values. Left out: work-level records, licences, ROR
affiliation ids and Polish Crossref members (the members route has no country filter).

Terms (https://www.crossref.org/documentation/retrieve-metadata/rest-api/, register CD-029): "almost none of the
metadata is subject to copyright, and you may use it for any purpose"; abstracts may be copyrighted and none is
requested. Licence row `crossref_metadata`, attribution "Source: Crossref metadata, crossref.org". Refresh full
(D32), weekly (Monday 04:50 UTC).
