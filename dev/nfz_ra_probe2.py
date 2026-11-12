"""Second read-only probe of the NFZ pharmacy-refund API (#2930): timing of a full-range paged request against
24- and 12-month dateFrom/dateTo windows, window boundaries, and how links.self echoes an ATC name. GET only, one
request at a time, 1.1 s apart, at most 30 requests."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://api.nfz.gov.pl/app-stat-api-ra"
LEFT = [30]


def api(path: str, **params):
    if LEFT[0] <= 0:
        raise SystemExit("budget used")
    LEFT[0] -= 1
    query = urllib.parse.urlencode({**params, "format": "json", "api-version": "1.0"})
    time.sleep(1.1)
    start = time.monotonic()
    req = urllib.request.Request(f"{BASE}/{path}?{query}", headers={"User-Agent": UA, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=180) as r:
            status, body = r.status, r.read()
    except urllib.error.HTTPError as e:
        status, body = e.code, e.read(300)
    except Exception as e:  # noqa: BLE001
        status, body = -1, str(e).encode()
    took = round(time.monotonic() - start, 2)
    try:
        doc = json.loads(body)
    except ValueError:
        doc = None
    return status, doc, took, body


def show(label, path, **params):
    s, d, took, body = api(path, **params)
    data = (d or {}).get("data") if isinstance(d, dict) else None
    rows = data if isinstance(data, list) else []
    meta = (d or {}).get("meta") or {} if isinstance(d, dict) else {}
    span = (f"{rows[0]['year']}-{rows[0]['month']:02d}..{rows[-1]['year']}-{rows[-1]['month']:02d}" if rows and "month" in rows[0] else "")
    print(f"{label:<34} HTTP {s} {took:>6}s count={meta.get('count')} rows={len(rows)} {span} "
          f"self={((d or {}).get('links') or {}).get('self') if isinstance(d, dict) else body[:120]!r}", flush=True)
    return d


def main():
    print("available-periods", api("available-periods")[1])
    atc = {x["attributes"]["code"]: x["attributes"]["name"] for x in show("atc", "atc", page=1, limit=25)["data"]}
    sel = {"province": "07", "atc": atc["C"]}
    for i in (1, 2):
        show(f"07/C full range page 1 #{i}", "monthly-provisions", page=1, limit=25, **sel)
        show(f"07/C full range page 4 #{i}", "monthly-provisions", page=4, limit=25, **sel)
        show(f"07/C 2023-2024 #{i}", "monthly-provisions", page=1, limit=25, dateFrom="2023-01-01", dateTo="2024-12-31", **sel)
        show(f"07/C 2017-2018 #{i}", "monthly-provisions", page=1, limit=25, dateFrom="2017-01-01", dateTo="2018-12-31", **sel)
        show(f"07/C 2024 #{i}", "monthly-provisions", page=1, limit=25, dateFrom="2024-01-01", dateTo="2024-12-31", **sel)
    show("07/C 2024-12 single month", "monthly-provisions", page=1, limit=25, dateFrom="2024-12-01", dateTo="2024-12-31", **sel)
    show("07/C 2023-02-01..2024-11-30", "monthly-provisions", page=1, limit=25, dateFrom="2023-02-01", dateTo="2024-11-30", **sel)
    for code, label in (("N", "PL/N"), ("L", "PL/L")):
        show(f"{label} full range page 1", "monthly-provisions", page=1, limit=25, atc=atc[code])
        show(f"{label} 2023-2024", "monthly-provisions", page=1, limit=25, atc=atc[code], dateFrom="2023-01-01", dateTo="2024-12-31")
    show("12/T full range page 1", "monthly-provisions", page=1, limit=25, province="12")
    show("12/T 2023-2024", "monthly-provisions", page=1, limit=25, province="12", dateFrom="2023-01-01", dateTo="2024-12-31")
    show("08/P 2017-2018", "monthly-provisions", page=1, limit=25, province="08", atc=atc["P"], dateFrom="2017-01-01", dateTo="2018-12-31")
    print("requests left", LEFT[0])


if __name__ == "__main__":
    main()
