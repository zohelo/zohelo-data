"""Third read-only probe of the PIG-PIB MIDAS WFS (PL-ENV-015, part of #2870): one full GetFeature per layer.

Probe 45818533713 paged every layer in GEOJSON (count=2000) without a retry. This one asks for each whole layer in
one request, outputFormat GEOJSON+ZIP (and GEOJSON for the smallest layer, to compare), after a resultType=hits
request, 20 s between requests, a 503 retried after 60, 120 and 240 s. Reports per layer: HTTP status, bytes,
seconds, ZIP members, the top-level keys of the GeoJSON (numberMatched / numberReturned / crs), features returned
against numberMatched of the hits request, OBJECTID repeats. Nothing is written to R2.
"""

from __future__ import annotations

import io
import json
import re
import time
import urllib.parse
import zipfile

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
WFS = "https://cbdgmapa.pgi.gov.pl/arcgis/services/midas/MapServer/WFSServer"
LAYERS = [
    "MIDAS___złoża_wybilansowane__wyselekcjonowane_dane_archiwalne_",
    "MIDAS_-_obszary_górnicze",
    "MIDAS_-_tereny_górnicze",
    "MIDAS_-_archiwalne_obszary_górnicze__wyselekcjonowane_dane_archiwalne_",
    "MIDAS_-_archiwalne_tereny_górnicze__wyselekcjonowane_dane_archiwalne_",
    "MIDAS_-_udokumentowane_złoża_kopalin",
]
RESULTS: list = []
RETRIES: list = []


def get(url: str):
    waits = [60, 120, 240]
    while True:
        time.sleep(20)
        t0 = time.monotonic()
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=(30, 900))
        except requests.RequestException as e:
            if waits:
                RETRIES.append([type(e).__name__, url[-60:]])
                time.sleep(waits.pop(0))
                continue
            return None, {"error": f"{type(e).__name__}: {str(e)[:100]}"}
        info = {"status": r.status_code, "bytes": len(r.content), "s": round(time.monotonic() - t0, 1),
                "type": r.headers.get("content-type", "")[:30]}
        if r.status_code in (429, 500, 502, 503, 504) and waits:
            RETRIES.append([r.status_code, url[-60:], r.headers.get("Retry-After")])
            time.sleep(int(r.headers.get("Retry-After") or 0) or waits.pop(0))
            continue
        return r.content, info


def layer(name: str, fmt: str) -> None:
    q = urllib.parse.quote(name)
    body, _ = get(f"{WFS}?service=WFS&request=GetFeature&version=2.0.0&typeNames=midas:{q}&resultType=hits")
    m = re.search(rb'numberMatched="(\d+)"', body or b"")
    matched = int(m.group(1)) if m else None
    body, info = get(f"{WFS}?service=WFS&request=GetFeature&version=2.0.0&typeNames=midas:{q}&outputFormat={fmt}")
    rec = {"layer": name[:34], "fmt": fmt, "hits": matched, **info}
    if body and info["status"] == 200:
        try:
            raw = body
            if fmt.endswith("+ZIP"):
                with zipfile.ZipFile(io.BytesIO(body)) as z:
                    rec["members"] = [(i.filename, i.file_size) for i in z.infolist()]
                    raw = z.read(z.infolist()[0].filename)
            doc = json.loads(raw)
            feats = doc.get("features") or []
            ids = [f.get("properties", {}).get("OBJECTID") for f in feats]
            rec.update({"keys": [k for k in doc if k != "features"], "features": len(feats),
                        "objectid_repeats": len(ids) - len(set(ids)),
                        "top": {k: str(v)[:80] for k, v in doc.items() if k not in ("features", "type")},
                        "first_id": feats[0].get("id") if feats else None})
        except (ValueError, zipfile.BadZipFile, KeyError) as e:
            rec["decode_error"] = f"{type(e).__name__}: {str(e)[:120]}"
            rec["head"] = body[:160].decode("utf-8", "replace")
    elif body is not None or info.get("status"):
        rec["head"] = (body or b"")[:160].decode("utf-8", "replace") if body else None
    RESULTS.append(rec)
    print(json.dumps(rec, ensure_ascii=False), flush=True)


def main() -> int:
    for name in LAYERS:
        layer(name, "GEOJSON+ZIP")
    layer(LAYERS[0], "GEOJSON")
    for i in range(0, len(RESULTS), 4):
        annotate("notice", f"midas full layers {i // 4 + 1}", RESULTS[i:i + 4])
    annotate("notice", "midas retries", RETRIES[:30])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
