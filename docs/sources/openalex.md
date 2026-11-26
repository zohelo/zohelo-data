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
- `institutions_pl`: `/institutions?filter=country_code:PL` with the root fields selected (id, ROR, name, type,
  geo, works and citation counts, counts by year, lineage, updated date). Organisations only.

Every request pages by cursor (`per_page=200`). About 45 requests a read, one at a time, 1 s apart, an identifying
User-Agent, bounded retries with backoff; no key. Version = time the read started (aggregates have no update time).
The answers carry response times and cursors that differ on every read, so a read is "unchanged" when its content
(the items sorted, and the counts of `meta`; `content_sha256` in the receipt) equals the newest copy's; then nothing
is written. Restart unit: the entity's whole read (a read that fails part-way lands nothing).

Bronze `bronze.openalex_works_counts` (`group` rows: request, field, year, count; `field` rows: the field list) and
`bronze.openalex_institutions` (one row per institution, counts by year and lineage as JSON text), values as text; a
page missing (items that do not add up to `meta`), a key twice, a field without its request or an attribute outside
the selection rejects the file. Silver `silver.openalex_works_pl_counts` (selection x field x publication year),
`silver.openalex_institutions_pl` (entity table keyed by the OpenAlex id `I…`) and
`silver.openalex_institution_works_by_year` (institution x year, from `counts_by_year`: the recent years OpenAlex
lists). Gold (`warehouse/macros/sources/openalex.sql`): block `works_pl` (all, open access, by field) and block
`institutions_pl` (works and citations per institution), annual, area Poland, values and units as published (D33);
the entity table `gold.institution_openalex`; draft KPI `scholarly_works_pl_institution` (all works, by publication
year).

**The current year** is kept as published at the read with `observation_status_key = 'provisional'`; its date is
the year of `source_version` (the read's time). OpenAlex revises past years too (works added and merged), so every
read replaces all years. Years after the read (works dated ahead) and before 1900 (the calendar's range) stay in
Silver and are not loaded into Gold.

Size: TRIAL.

Geography: Poland only. OpenAlex's institution `geo.region` is free text and mostly empty, so institutions are not
assigned to voivodeships (a mapping through ROR or coordinates is possible later, not verified).

Left out (next steps, same API): other countries for comparison, works by type, source, publisher or funder,
citations of the Polish works, authors (no person's name or identifier is read), the bulk snapshot, citations per
year of the selection. Weekly schedule (Friday 00:50 UTC, O46; early in the UTC day because the keyless budget is
per day and shared per IP address).

Licence: https://help.openalex.org/access/pricing/: "Every record is released under a CC0 public-domain license,
with no “personal use only” carve-out and no permission to ask." (`cc0_openalex`); the snapshot's
https://openalex.s3.amazonaws.com/LICENSE.txt is "Creative Commons Legal Code CC0 1.0 Universal". Attribution is not
required; shown as a courtesy: "Source: OpenAlex (CC0)".
