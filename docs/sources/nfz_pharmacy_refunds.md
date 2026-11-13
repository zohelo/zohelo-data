# NFZ statistics API: pharmacy refunds (`nfz_pharmacy_refunds`)

**Built (12 Nov 2026, register PLS-013, #2930).** NFZ publishes no bulk file for these statistics; the source is the
API `https://api.nfz.gov.pl/app-stat-api-ra/` (v1.0, no key, 10 requests/second per IP in the terms, at most 25 rows
a page; robots.txt: none). `ingestion/to_landing/nfz_pharmacy_refunds/` (the ICP extractor's fetch and write) lands
`/monthly-provisions` for 255 entities `<area>_<atc>`: area `PL` (no province filter) or the NFZ voivodeship code
`01`–`16` (`province=`), ATC selection `T` (no filter) or one of the 14 ATC main groups of `/atc` (`atc=` takes the
group **name**). Each entity is every page of its answer (one Landing file per page, `__part-<n>`, one receipt
each; complete when all parts of the newest download are there). Version = `end-date` of `/available-periods`:
a run whose version is held makes that one request. Answers carry the request time (`meta.date-modified`), so a
new download is compared by the SHA-256 of its `data` rows (`data_sha256` in the receipts); equal rows are not
written again. A full load is about 1,020 requests, one at a time, 1 s apart. Restart unit: one entity. The `/atc`
answer (the 14 group names the filters take) is landed as the metadata entity `atc` when its rows change; it has no
Bronze table (the names travel in the data receipts and Bronze rows) and is archived as `without_bronze`.

- Bronze `bronze.nfz_ra_monthly_provisions`: one row per record, number text as published, the selection from
  the receipt, `meta_count` and page. Silver `silver.nfz_ra_monthly_provisions`: grain area × ATC selection ×
  month, HISTORY. Gold `gold.build_nfz_pharmacy_refunds`: five measures (packages; refund, donation, patient
  payment, value in PLN) × PL and 16 voivodeships (`TERC:02`…`TERC:32`) × total and 14 ATC groups, monthly.
  KPIs (draft): `nfz_pharmacy_refund`, `nfz_pharmacy_patient_payment`, `nfz_reimbursed_packages`.
- `aggregationType` is not sent (the API's default); W, G13 and G14 only regroup package codes and gave the same
  monthly totals (Dev probe 43310762847, 2024-12). `limit` above 25 is refused (HTTP 400); an unknown ATC name
  is refused (HTTP 400), an unknown province code is not (HTTP 200), so the 16 codes are a fixed list and
  `tests/nfz_pharmacy_refunds_partition.sql` checks that the provinces sum to Poland every month.
- Definitions (API field descriptions): `quantity` reimbursed packages (fractions of a package as published),
  `refund` the NFZ refund, `donation` the Ministry of Health subsidy of the 75+ programme, `contribution-of-patient`
  the patient's co-payment, `value` the total cost, described as refund + co-payment + subsidy; in the published
  numbers `value` = `refund` + `contribution-of-patient` (VALUE_IDENTITY), so the subsidy is not added on top of
  the refund. Shown as published.
- The 14 ATC groups do not sum to the total: the API's group `-` (medicines without an ATC group) is only in
  the total; no remainder is derived. Probe: the 14 groups plus `-` equal the total exactly (2024-12).
- **Not loaded** (no claim of full coverage): package-level `/provisions` (21,504 package codes), the splits by
  gender, age group, 75+ entitlement (`privilegesAdditional`) and reimbursement-list announcement, the top
  lists, `/atc-provisions`, `/yearly-provisions`, and the medicine-product and active-substance dictionaries.
- Schedule: monthly check, 9th at 05:35 UTC (one request while the end-date is unchanged); the data move yearly
  or slower (in November 2026 they end at 2024-12; NFZ publishes no calendar). Refresh full (D32): about 25,000 rows replaced whole when the end-date moves.
- Terms: the API regulations v1.0 (https://api.nfz.gov.pl/app-stat-api-ra/terms, read in the probe): state the
  source by giving https://api.nfz.gov.pl/, do not modify the data, do not overload the API (10 requests/second
  per IP); no licence name, nothing on commercial reuse (register: `open-conditional`). Licence row `nfz_api_terms`; values are kept exactly as published (the terms forbid
  modifying the data), any figure derived later is labelled a zohelo calculation.
