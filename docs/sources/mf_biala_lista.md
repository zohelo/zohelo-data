# MF VAT white list (flat file) (`mf_biala_lista`)

**Extraction (24 Oct 2026).** The Ministry of Finance publishes the "plik płaski" every day at
`https://plikplaski.mf.gov.pl/pliki/<YYYYMMDD>.7z`, valid for that Warsaw date and available
from about 22:30 Warsaw time the evening before. The 7z (~250 MB) holds `<date>.json` (~500 MB)
and its SHA-512. The JSON has a header (generation date, number of hash transformations, scheme)
and three lists: SHA-512 hashes of active taxpayers (3.33 M on 24 Oct 2026), of exempt taxpayers
(0.35 M) and virtual-account masks (1,506). The file carries no NIP or account in clear text:
a NIP + account pair is checked by hashing it the published number of times. The shared
`bulk_file` pattern lands the day's file (`url_date: Europe/Warsaw`, format `7z`, tested with
the runner's 7-Zip); Bronze keeps the list items as published, Silver splits hashes and masks.
