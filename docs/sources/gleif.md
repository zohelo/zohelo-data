# GLEIF golden copy (`gleif`)

**Extraction (24 Oct 2026).** GLEIF publishes the complete LEI dataset three times a day
(00:00, 08:00, 16:00 UTC) as golden-copy files; the files are ready about 1.5 h after the
publish time. The stable URLs
`https://goldencopy.gleif.org/api/v2/golden-copies/publishes/{lei2|rr|repex}/latest.csv`
redirect (302) to the newest publish's ZIP and give its Last-Modified, so the shared
`bulk_file` pattern lands each file only when its SHA-256 changed (version = Last-Modified).
Sizes on 23 Oct 2026: lei2 3.47 M records, 506 MB ZIP / 5.0 GB CSV, 338 columns; rr 0.49 M
records, 25 MB / 245 MB, 54 columns; repex 62 MB / 716 MB, 12 columns. Full files rather than
the delta files: one complete state a day, each file one entity replaced whole (design #875). Bronze
keeps every CSV column as text; Silver (`warehouse/entity/gleif_*.sql`) types the core LEI, relationship and exception fields. A rejected file leaves the previous version in place.
