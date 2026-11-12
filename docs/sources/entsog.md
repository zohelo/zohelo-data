# ENTSOG Transparency Platform, GAZ-SYSTEM gas flows (`entsog`)

**Built (12 Nov 2026, register PL-ENV-021, #2962).** Daily physical gas flows of the Polish transmission operator
GAZ-SYSTEM (operator key `PL-TSO-0002`) from the ENTSOG Transparency Platform's public REST API; the source id is
the platform's, the operator is configuration (`OPERATORS` in `ingestion/to_landing/entsog/extract.py`), so other
operators can be added to the same build.

**Route.** `GET https://transparency.entsog.eu/api/v1/operationaldatas?operatorKey=PL-TSO-0002&indicator=Physical%20Flow&periodType=day&timezone=WET&limit=-1&from=<first day>&to=<last day>`,
one gas month per request, as the API User Manual v3.0 recommends ("Monthly ranges are strongly recommended"; 60 s
execution limit; back-off on failures, no rapid retries; without `limit=-1` the API returns 100 records). One request
at a time, 5 s apart, back-off 30/90/240 s on 429, 5xx and timeouts. No bulk file of operational data exists. The
API serves a rolling window of about five years: on 12 Nov 2026 January 2021 answered and December 2020 answered
404 "Data on the TP is archived on a rolling basis"; the TP Archives are not read.

**Entity, version, restart unit.** Entity = operator × indicator × gas month (`physical_flow__PL-TSO-0002__2026-10`);
version = the newest `lastUpdateDateTime` of the answer; restart unit = one gas month. Identical requests return
identical bytes, so a copy is landed only when its bytes change. The first run loads the whole window, newest month
first, and resumes within the time budget. Then each run requests the current month, every month not yet closed
and one closed month in rotation (confirmed values are revised: January 2021 was last updated in February 2025). A
month is closed when it has ended, holds every gas day and every record with a value is `Confirmed`; `isNA` records
have no value and no status and count as final. Daily at 07:40 UTC (GAZ-SYSTEM publishes gas day D provisionally on
D+1 at about 07:05 UTC), about three requests a run.

**Layers.**
- Landing `01_landing/entsog/snapshot/data/<entity>/`: the exact JSON answer and a receipt with the counts that
  decide whether the month is closed.
- Bronze `bronze.entsog_operational_data`: one row per record, the 35 API fields as text (`value_text` is `''` on
  `isNA` records); rejections in `bronze.entsog_rejected_files` (unknown field, other operator, indicator or period
  type, empty answer).
- Silver `silver.entsog_physical_flow`: grain operator × point × direction × indicator × period type × periodFrom;
  value in kWh/d as published, null (never 0) on `isNA` or empty values; `flow_status` Provisional/Confirmed;
  `last_update_utc`. Gas day = the date of periodFrom in WET: the gas day starts at 05:00 WET at most points and at
  03:00 or 04:00 at some (checked by `tests/entsog_gas_days.sql`). The newest month copy wins, then the newest
  `lastUpdateDateTime` per key; a tie fails the unique test. `silver.entsog_points`: point dimension (operator,
  point, direction, label, the API's `pointType`, first and last day, days with a value, `reports_flows`).
- Gold `gold.build_entsog` (block `physical_flow`, geography `PL`: the platform has no regional grain): entry and
  exit flows per day in kWh/d, all points of the direction (`is_total`) and per point group; monthly sums in kWh as
  a second measure key. Point groups are a reviewed list keyed by `pointKey` (`poc_entsog_point_groups` in
  `warehouse/macros/sources/entsog.sql`: LNG terminal, Baltic Pipe, interconnectors by neighbour, Yamal system
  (PWP), cross-border distribution, domestic production, storage, H/L-gas conversion, distribution, final
  consumers); an unlisted point that reports flows lands in group `unmapped` and `tests/entsog_point_groups.sql`
  warns. Totals rule: a point-direction reports in a month when it has a value on any day of it; a daily sum is
  built only when every reporting point of its scope has a value that day, a monthly sum only for an ended month
  whose every day is complete. `observation_status_key` is `provisional` when any summed value is Provisional.
  Draft KPIs `gas_entry_flow_daily`, `gas_exit_flow_daily`, `gas_entry_flow_monthly`, `gas_exit_flow_monthly`
  (environment.energy). Totals sum every point as published, so they include storage withdrawal or injection and
  the H/L-gas conversion points (nitrogen removal enters H-gas taken from L-gas): use the groups to separate them.

**Licence: `unresolved` (internal only).** ENTSOG TP "Privacy & T&C of Use" (TRA0394-16, Rev_3, 24 May 2018,
https://transparency.entsog.eu/pdf/TRA0394_20161115_ENTSOG_TP_TC_of_Use_Rev_2_KNS_LG_KNS.pdf): Art. 5.1 "you may
download, store and use the contents of the ENTSOG TP in good faith"; Art. 5.2 cite as "ENTSOG TP [DD-MM-YYYY]
https://transparency.entsog.eu/"; Art. 5.7 "The automate download of data is possible via the API tool of the ENTSOG
TP". Commercial reuse and modification are not stated (Art. 5.5 disclaims liability for altered, summarized or
processed data), so `source.csv` uses the shared `unresolved` licence row and the licence gate keeps every row out
of v1 and v2.

**Left out.** Other indicators of the same endpoint (nominations, renominations, allocations, firm and
interruptible capacities, GCV, interruptions), other period types (hour, month), the TP Archives before the API
window (2013–2020), other operators (Yamal system operator PL-TSO-0001, storage and LNG operators, neighbouring
TSOs), GAZ-SYSTEM's own SWI tables, and conversion of kWh to m³ or TWh (D33: units as published).
