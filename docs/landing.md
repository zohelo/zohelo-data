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
