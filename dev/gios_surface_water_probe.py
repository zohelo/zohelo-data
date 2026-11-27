"""Read-only Dev probe of the GIOŚ surface-water status services (#3312), second pass: the services answer no
pagination (resultOffset, HTTP 400 "Pagination is not supported", probe 45854625135), and a where clause with a FID
range was answered 403 (probe 45922872272; not tried again). This pass pages each layer by objectIds (ArcGIS's
documented paging for services without pagination: returnIdsOnly, then the ids in chunks), first one chunk of 1000 ids,
else 250, then reads every layer and profiles its attributes. One request at a time, 2 s apart. No writes."""

from __future__ import annotations

import collections
import json
import time

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

BASE = "https://wody.gios.gov.pl/arcgis/rest/services/PJWP"
LAYERS = {
    "Ocena_stanu_JCWP_rzeczne_2019_2024": 5,
    "Ocena_stanu_JCWP_rzeczne_zbiorniki_2019_2024": 3,
    "Ocena_stanu_JCWP_jeziorne_2019_2024": 5,
    "Ocena_stanu_JCWP_przejsciowe_2019_2024": 3,
    "Ocena_stanu_JCWP_przybrzezne_2019_2024": 3,
}


def get(url: str) -> tuple[int, bytes]:
    for attempt in range(3):
        try:
            time.sleep(2)
            r = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(20, 120))
            print(f"GET {r.url[:200]} ({len(r.url)} chars) -> {r.status_code} {len(r.content)} bytes", flush=True)
            if r.status_code != 200:
                print("BODY", r.content[:1500], flush=True)
            return r.status_code, r.content
        except requests.RequestException as exc:
            print("retry", type(exc).__name__, str(exc)[:200], flush=True)
            time.sleep(10 * (attempt + 1))
    return 0, b""


def ids_of(svc: str, lid: int) -> list[int]:
    st, body = get(f"{BASE}/{svc}/MapServer/{lid}/query?where=1%3D1&returnIdsOnly=true&f=json")
    return sorted(json.loads(body)["objectIds"])


def page(svc: str, lid: int, chunk: list[int]) -> tuple[int, dict | None]:
    url = (f"{BASE}/{svc}/MapServer/{lid}/query?objectIds={','.join(map(str, chunk))}"
           "&outFields=*&returnGeometry=false&f=json")
    st, body = get(url)
    if st != 200:
        return st, None
    return st, json.loads(body)


def main() -> int:
    first = "Ocena_stanu_JCWP_rzeczne_2019_2024"
    ids = ids_of(first, 0)
    size = None
    for n in (1000, 250):
        st, doc = page(first, 0, ids[:n])
        ok = st == 200 and doc is not None and "features" in doc
        annotate("notice" if ok else "warning", f"probe objectIds chunk {n}", {
            "status": st, "features": len(doc.get("features", [])) if doc else None,
            "exceeded": doc.get("exceededTransferLimit") if doc else None,
            "keys": sorted(doc) if doc else None, "error": (doc or {}).get("error")})
        if ok and len(doc["features"]) == n:
            size = n
            break
        if st == 403:
            annotate("error", "probe refused", {"chunk": n, "status": st})
            return 1
    if size is None:
        return 1
    codes_by: dict[str, set] = {}
    for svc, n_layers in LAYERS.items():
        for lid in range(n_layers):
            ids = ids_of(svc, lid)
            feats, keysets, fields = [], set(), None
            for i in range(0, len(ids), size):
                st, doc = page(svc, lid, ids[i:i + size])
                if doc is None or "features" not in doc:
                    annotate("error", f"probe {svc} {lid}", {"status": st, "doc": str(doc)[:300]})
                    return 1
                fields = [f["name"] for f in doc.get("fields", [])]
                keysets.add(tuple(sorted(doc)))
                feats += [f["attributes"] for f in doc["features"]]
            prof = {}
            for name in fields or []:
                vals = collections.Counter(str(f.get(name)) for f in feats)
                prof[name] = dict(vals.most_common(15)) if len(vals) <= 15 else {"distinct": len(vals), "null": vals.get("None", 0), "empty": vals.get("", 0)}
            codes = collections.Counter(f.get("KOD_JCWP") for f in feats)
            codes_by[f"{svc[12:]}:{lid}"] = set(codes)
            out = {"ids": len(ids), "features": len(feats), "fields": fields, "doc_keys": sorted(keysets),
                   "dup_codes": [c for c, v in codes.items() if v > 1][:10],
                   "prefixes": dict(collections.Counter(str(c)[:4] for c in codes)), "profile": prof,
                   "sample": feats[:1]}
            print("LAYER", svc, lid, json.dumps(out, ensure_ascii=False), flush=True)
    keys = sorted(codes_by)
    overlaps = {f"{a}~{b}": len(codes_by[a] & codes_by[b]) for i, a in enumerate(keys) for b in keys[i + 1:]
                if a.split(":")[0] != b.split(":")[0] and codes_by[a] & codes_by[b]}
    within = {}
    for svc in {k.split(":")[0] for k in keys}:
        ls = [k for k in keys if k.startswith(svc + ":")]
        within[svc] = {f"{a}~{b}": len(codes_by[a] & codes_by[b]) for i, a in enumerate(ls) for b in ls[i + 1:]}
    print("OVERLAPS", json.dumps(overlaps), json.dumps(within), flush=True)
    annotate("notice", "probe overlaps", {"across_services": overlaps, "within": within, "chunk": size})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
