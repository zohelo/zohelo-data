# Landing and extraction

How source data is extracted and stored in `zohelo-landing-prod/01_landing/`, and
how it moves on to Bronze and Archive.

## Extraction strategy

Decide per source, in this order, and record the decision in
`ingestion/sources/<source_system>.yaml` (`extraction:`):

1. **Bulk feature** — the source offers a dedicated bulk/full download. Always the
   target for the initial load.
2. **Bulk workaround** — no bulk feature: research options, propose them, the owner
   decides.
3. **API** — only if neither works, after testing whether the API can serve a full
   load (data size, paging, limits, quotas).

After the initial load, choose the production load per source:

| Setting | Values |
|---|---|
| `production` | `snapshot` — each file is a complete copy of an entity (dataset, table); `delta` — each file holds only new or changed rows |
| `incremental_policy` | `changed_since_last` — compare the source catalogue with Landing and fetch only missing or changed entities; `append_only` — fetch only records after the last one; `full_refresh` — fetch everything each run |
| `schedule` | `null` (manual) until the owner approves one; daily is the target where the source refreshes that often |

## Layout

```
01_landing/<source_system>/<snapshot|delta>/<data|metadata>/<entity>/<source_version>__<ingested_at>.<ext>
01_landing/<source_system>/<snapshot|delta>/<data|metadata>/<entity>/<source_version>__<ingested_at>.receipt.json
```

- `source_system` — one folder per source system (`eurostat`, `gus_bdl`, `gus_dbw`,
  …), never grouped by provider.
- `snapshot | delta` — what the file contains (see above), not when it was loaded.
- `data | metadata` — observations vs dictionaries, code lists, structures, catalogues.
- `entity` — the source's own unit (dataset code, table, variable group). Putting it
  before time makes "latest version of X" a single listing.
- `source_version` — the source's last update of that version; `ingested_at` — our
  download time. Both UTC, `YYYYMMDDTHHMMSSZ`.
- A file holding only part of an entity (a source that had to be split) adds
  `__part-<id>` before the extension.
- The extension is the native format; where one entity has several metadata files a
  short type goes before it (Eurostat: `.structure.xml`, `.sdmx.zip`, `.codelist.tsv`).
- Example: `01_landing/eurostat/snapshot/data/NAMA_10_GDP/20261009T210000Z__20261011T190512Z.tsv.gz`

The receipt is written after the file and marks it complete: SHA-256, size, source
URL, both timestamps and transfer details. Files are the exact source bytes, never
modified or overwritten.

Migration: all Eurostat files were copied into this layout on 12 October 2026 and the
Eurostat code reads and writes only this layout. The earlier copies under
`01_landing/eurostat/bulk/` are archived (content-addressed, `05_archive/eurostat/raw/<sha256>.bin`)
and then removed from Landing.

## Landing → Bronze → Archive

- Bronze (`ingestion/to_bronze/`) loads the latest version of every entity, keeps the source's
  meaning and grain and adds provenance (Landing SHA-256, source version, ingestion
  time). Bronze does not interpret values: numbers stay text, casting is a Silver rule.
  Checkpoint: the Landing SHA-256 is in the Bronze table. Files the decoder rejects are
  recorded with a reason in a Bronze rejections table (e.g. `bronze.eurostat_rejected_files`),
  never dropped silently.
- State lives in the data itself (receipts in Landing, Iceberg tables and their file
  lists) — there is no separate control folder.
- Landing is an inbox, not storage. Once Bronze holds a file, `landing.archive` moves
  it: content-addressed copy `05_archive/<source>/raw/<sha256>`, read back and checked
  against the original SHA-256, then deleted from Landing. Its receipt stays in Landing
  as the pointer (`ops/restore_from_archive` puts the file back; `bronze.load` reads
  from the Archive when needed). Areas without a Bronze table yet (Eurostat metadata)
  are moved too when the source's `archive_policy.without_bronze` lists them. Never
  delete because ingestion or a Bronze build merely started.
- Archive format: already-compressed formats (gzip, ZIP, 7z, Parquet) are kept as the
  exact bytes (`.bin`); uncompressed formats (XML, CSV, JSON) are stored
  zstd-compressed (`.zst`) and restore to the exact original bytes.
- Retention per source (`archive_policy.retention` in `ingestion/sources/<source>.yaml`):
  snapshot sources keep the newest version of every entity plus everything ingested in
  the last `keep_days` (agreed: 1 version + 90 days); older versions and their
  receipts are deleted by `archive.prune`. Delta / append-only sources set no retention
  — their raw is unique and kept. `prune: report` reports only; `delete` applies.

## Common ingestion framework (design, 14 Oct 2026)

Goal: a new standard source needs only `ingestion/sources/<source>.yaml` (and a
register entry), no new Python. Today every source has its own extractor
(`ingestion/to_landing/<source>/`, 3,200 lines); WDI and PRG are the same bulk-file
code twice, and every extractor re-implements listing, receipts and verification.

### Patterns

The pattern names are the register's `access.kind` values (one vocabulary):

| `kind` | When | Examples |
|---|---|---|
| `bulk_file` | fixed URLs or form postbacks, each returning a complete file (ZIP, CSV, XML) | World Bank WDI, GUGiK PRG, GUS TERYT (ASP.NET form postback, no browser), GLEIF golden copy, MF white list, Eurostat bulk download (plus its source-specific inventory hook) |
| `api` | REST/JSON, XML or SDMX API; the full load is a set of requests split by entity, page or period | NBP, GUS BDL API, GUS DBW; SDMX sources (OECD, ECB, IMF) as an `api` sub-type `sdmx` |
| `web` | no machine route: a polite browser session (one route, no rotation) that asks the site for an export | GUS BDL website export |

### Shared engine (`ingestion/common/landing.py`)

- Polite HTTP: user agent, timeouts, retries with backoff, and the source's quota
  windows (per second and longer windows, e.g. BDL 100/15 min, 1,000/12 h), declared
  in YAML.
- Auth by reference only: the YAML names an environment variable (`auth: {header:
  X-ClientId, env: GUS_BDL_API_KEY}`), never a value.
- Streaming to disk and multipart upload for large files (WDI ~270 MB, PRG ~377 MB),
  integrity check by format (ZIP test, gzip, XML parse), verified upload.
- Layout and receipts: one **core** receipt (schema_version 3: source, entity, area
  `data|metadata`, content `snapshot|delta`, version_token + version basis,
  ingested_at, source and final URL, sha256, size, object, part `{index,total}`,
  code_sha) plus a pattern-specific `detail` section (WDI members, NBP window,
  TERYT form control…). Bronze loaders read only the core; existing schema_version 2
  receipts stay readable (Bronze compatibility is part of each migration PR).
- Time budget and resumability: every run stops at its budget and the next run
  continues from Landing (missing entities first), as Eurostat and NBP do today.
- `inspect` / `verify` summaries and annotations are generic.

Source-specific logic that is not configuration stays a small hook module named in
the YAML (Eurostat inventory, NBP effective-date versioning, TERYT catalogue date,
BDL subgroup selection).

### Declaration (`extraction:` in the source YAML)

Existing keys stay (`initial`, `production`, `incremental_policy`, `schedule`);
`kind` and one block per kind are added. Example shapes:

```yaml
extraction:
  kind: bulk_file
  initial: bulk
  production: snapshot
  incremental_policy: changed_since_last
  bulk_file:
    files:
      - entity: WDI_CSV
        url: https://databank.worldbank.org/data/download/WDI_CSV.zip
        format: zip
        version: last_modified       # last_modified | etag | content_sha | hook
```

```yaml
extraction:
  kind: api
  initial: api
  api:
    base_url: https://api.nbp.pl/api
    entities:                         # each entity its own request and start
      - {entity: A,    request: "/exchangerates/tables/A/{start}/{end}/?format=json", start: "2002-01-02"}
      - {entity: GOLD, request: "/cenyzlota/{start}/{end}/?format=json",          start: "2013-01-02"}
    split: {kind: calendar_quarter, reopen_days: 10}   # page | offset | cursor | calendar_quarter | date_window
    quota: [{per: second, max: 2}]
    version: hook                     # nbp: effective date read from the response
```

The configuration check (`run.py schedule --check`, `ops/repo_check`) validates the
block per kind.

### Migration order and acceptance

1. `bulk_file`: WDI and PRG onto shared code, then TERYT (form postback), GLEIF and
   MF white list (which also settles their `to_be_decided` extraction).
2. `api`: NBP, then GUS DBW (rebuild, #304) directly on it; SDMX sources after.
3. `web`: BDL website export as the first module.
4. Eurostat stays on its own package for now (largest and most special source); it
   moves to `bulk_file` + inventory hook once the others run on the framework.

Acceptance per migrated source: old and new extractor run in the same window (or
against a recorded response) and land byte-identical files under the same prefix and
key pattern with the same `version_token`; core receipt fields match apart from
`schema_version` and `ingested_at`; Bronze loads the new receipts. The old package is deleted in the
same PR. New sources (DBW) are accepted by `landing.verify` plus a Bronze trial.
