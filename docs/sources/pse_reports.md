# PSE Reports API v2 (`pse_reports`)

**Built (26 Nov 2026, register PL-ENV-019, #2870).** Polskie Sieci Elektroenergetyczne publish their market and
system reports on https://raporty.pse.pl/ and through API v2, `https://api.raporty.pse.pl/api/<report>`: public, no
key, no published limit, OData-style JSON `{"value": [...]}`. Route: one request per report and business day,
`$filter=business_date eq 'YYYY-MM-DD'`, one at a time, 1.2 s apart (`ingestion/to_landing/pse_reports/`). Measured
from Actions (Dev probes 45790352597 and 45790863668, 26 Nov 2026): pages hold 100 rows and continue through
`nextLink` (`$after` cursor on `dtime_utc`); `$top`, `$skip` and `$count` are refused (400); a date-range filter
works but pages by 100 rows, so a month costs as many requests as its days; a day (96 periods, 92 and 100 on the DST
days) always fits one page; a repeated request returns identical bytes; the API answers 404 for its root and for
`$metadata`, so there is no entity catalogue to read. History starts on business day 2024-06-14 (rce-pln has no rows
for 2024-06-13; his-wlk-cal has one row there, its last period, left out); older reports are not in API v2.

Entity = report × business day (`rce_pln_<YYYYMMDD>`, `his_wlk_cal_<YYYYMMDD>`), the response's exact bytes;
version = the newest `publication_ts_utc` of the day. A day is landed only when it holds every period of the local
day at distinct UTC times; an empty or short answer is reported and asked again next run. Each run lands the missing
days up to yesterday (Europe/Warsaw), oldest first, then asks the newest 7 days again (PSE re-publishes corrected
days: his-wlk-cal of 2024-06-14 carries a publication time of 2026-01-18); unchanged bytes are not written. Restart
unit: one day of one report. Full load about 1,800 requests.

Bronze `bronze.pse_reports_rce_pln` and `bronze.pse_reports_his_wlk_cal` (one row per period, every field as
published text); a file with another field set, a non-scalar value or another business day is rejected into
`bronze.pse_reports_rejected_files`. Silver `silver.pse_reports_rce_pln` (RCE, PLN/MWh) and
`silver.pse_reports_his_wlk_cal` (demand, wind `wi`, photovoltaics `pv` and the report's other quantities under PSE's
codes, MW), grain business day × `dtime_utc`; the published local texts are kept (`dtime`, `period`: on the autumn
DST day PSE marks the repeated hour `02a:15`) beside `dtime_local`, the local end derived from `dtime_utc` and the
offset at the start of the period, and `periods_expected` from the Warsaw calendar; one completeness test per model
(every day from 2024-06-14 to the newest, all periods, one file, `dtime` equal to `dtime_local`; at a DST switch
instant PSE writes either offset, 2025-03-30 `02:00` and 2026-03-29 `03:00` for the same end, and both pass).
Gold `build_pse_reports`, frequency D, Poland: daily means of the 15-minute values of RCE (block `rce_pln`) and of
demand, wind and PV generation (block `his_wlk_cal`), complete days only, units as published; KPIs
`electricity_price_rce`, `electricity_demand`, `wind_generation`, `pv_generation` (draft, `environment.energy`).
Weekly schedule (Wednesday 04:37 UTC, O46).

Left out (next steps): every other report of the API (plans and forecasts such as `pk5l-wp`, balancing-market
reports, unit availability, cross-border capacity, monthly RCEm `rcem-*`), the generating-unit groups and exchange
balances of his-wlk-cal in Gold (they stay in Silver), shares of wind and PV in demand, and the reports before
2024-06-14 (no route in API v2).

Licence: not stated. No terms page on raporty.pse.pl (a single-page app whose bundle names none) or on
api.raporty.pse.pl; www.pse.pl/nota-prawna answers 404; the PSE announcement of the new reports offers data
download and API use but states no licence. `licence_key = unresolved`: built, internal, held back by the licence
gate. robots.txt: api.raporty.pse.pl has none (404); raporty.pse.pl disallows only its own `/api/` path, which this
route does not use.
