# OpenAlex (`openalex`)

**Built (register CD-028, #2996).** OpenAlex (OurResearch, https://openalex.org/) is an open index of scholarly
works, authors, institutions, sources, publishers and funders. Its bulk feature, the snapshot
(`s3://openalex`, about 750 GB per format), is far beyond this selection and the free storage tier and is not used;
the documented REST API `https://api.openalex.org` is read with aggregate requests only.

Route (`ingestion/to_landing/openalex/`), two entities, each one ZIP of the exact answers (one member per page):

- `works_pl`: `/works?filter=institutions.country_code:PL&group_by=publication_year` (works with at least one
  authorship affiliated with an institution in Poland, each work once), the same with `open_access.is_oa:true`, the
  field list (`/fields`) and per field `...,primary_topic.field.id:<id>&group_by=publication_year`. Counts only: no
  work, title or author name is requested.
- `institutions_pl`: `/institutions?filter=country_code:PL&sort=id:asc` with the root fields selected (id, ROR,
  name, type, geo, works and citation counts, counts by year, lineage, updated date). Organisations only.

No key (Dev probes 45813299261, 45816100460): a list or group_by request costs $0.0001 of a keyless budget of $0.10
a day (1,000 requests) shared per IP address, at most 10 requests a second (help.openalex.org/access/pricing/ and
the `X-RateLimit-*` headers); `mailto` no longer raises anything. Cursor paging past the first page answers HTTP 401
"API key required for cursor paging", so the institutions are read with `page=` (200 a page, sorted by id; 7 pages)
and each group_by request is one page of at most 200 groups (175 years, 26 fields); a full page fails the read
rather than cutting the list. About 37 requests a read, one at a time, 1 s apart, an identifying User-Agent, bounded
retries with backoff. Version = time the read started (aggregates have no update time). The answers carry response
times that differ on every read, so a read is "unchanged" when its content (per request the items sorted, and the
counts of `meta`; `content_sha256` in the receipt) equals the newest copy's; then nothing is written. Restart unit:
the entity's whole read (a read that fails part-way lands nothing). If the shared keyless budget is spent when a run
reads (`daily_budget`), the read fails; the remedy is a free OpenAlex key, which needs an account the owner creates.

Bronze `bronze.openalex_works_counts` (`group` rows: request, field, year, count; `field` rows: the field list) and
`bronze.openalex_institutions` (one row per institution, counts by year and lineage as JSON text), values as text; a
page missing (items that do not add up to `meta`), a key twice, a field without its request or an attribute outside
the selection rejects the file. Silver `silver.openalex_works_pl_counts` (selection x field x publication year),
`silver.openalex_institutions_pl` (entity table keyed by the OpenAlex id `I…`) and
`silver.openalex_institution_works_by_year` (institution x year, works and open-access works from `counts_by_year`:
the recent years OpenAlex lists, 2010 onwards; its citations per year are not kept, because they are 0 for 2010 and
2011 at almost every institution, years OpenAlex does not cover rather than counts). Gold
(`warehouse/macros/sources/openalex.sql`): block `works_pl` (all, open access, by field) and block `institutions_pl`
(works and open-access works per institution), annual, area Poland, values and units as published (D33);
the entity table `gold.institution_openalex`; draft KPI `scholarly_works_pl_institution` (all works, by publication
year).

**The current year** is kept as published at the read with `observation_status_key = 'provisional'`; its date is
the year of `source_version` (the read's time). OpenAlex revises past years too (works added and merged), so every
read replaces all years. Years after the read (works dated ahead) and before 1900 (the calendar's range) stay in
Silver and are not loaded into Gold.

Size (Dev trial 45825975173, 26 Nov 2026): two Landing files of 48 KB and 226 KB (29 and 7 answers). 1,818,049
works in 175 publication years 1812-2027 (`meta.count` 1,819,339: 1,290 works have no publication year and fall in
no group), 1,001,460 open access, 26 fields (works without a primary topic, 244 to 393 a year since 2020, are in
none); 2024: 87,917 works, 72,910 open access, Medicine 15,402 the largest field. 1,352 institutions (company 504,
education 330, facility 205, ...), 1,174 with yearly counts (15,732 institution-years, 2010-2026). Gold 34,233 rows:
works_pl 2,769 (1900-2026; 289 rows before 1900 and 14 of 2027 stay in Silver), institutions_pl 2 x 15,732.

Geography: Poland only; institutions are not assigned to voivodeships (no code, only `geo.region` names).

Left out (next steps, same API): other countries for comparison, works by type, source, publisher or funder,
citations, authors (no person's name or identifier is read), the bulk snapshot, a voivodeship for each institution
(OpenAlex's `geo.region` names the 16 voivodeships in English, so a mapping by name would be possible; not done,
names are not codes). Weekly schedule (Friday 00:50 UTC, O46; early in the UTC day because the keyless budget is
per day and shared per IP address).

Licence: https://help.openalex.org/access/pricing/: "Every record is released under a CC0 public-domain license,
with no “personal use only” carve-out and no permission to ask." (`cc0_openalex`); the snapshot's
https://openalex.s3.amazonaws.com/LICENSE.txt is "Creative Commons Legal Code CC0 1.0 Universal". Attribution is not
required; shown as a courtesy: "Source: OpenAlex (CC0)".
