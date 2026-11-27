"""Read-only Dev probe of the GIOŚ surface-water status services (#3312): services, layers, fields, counts, ids and the
distinct values of every short field, before modelling. One request at a time, 2 s apart. No writes. Never merged."""

from __future__ import annotations

import collections
import json
import time

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

BASE = "https://wody.gios.gov.pl/arcgis/rest/services/PJWP"
SERVICES = [
    "Ocena_stanu_JCWP_rzeczne_2019_2024",
    "Ocena_stanu_JCWP_rzeczne_zbiorniki_2019_2024",
    "Ocena_stanu_JCWP_jeziorne_2019_2024",
    "Ocena_stanu_JCWP_przejsciowe_2019_2024",
    "Ocena_stanu_JCWP_przybrzezne_2019_2024",
]


def get(url: str, params: dict | None = None) -> tuple[int, dict | str]:
    for attempt in range(3):
        try:
            time.sleep(2)
            r = requests.get(url, params=params, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(20, 120))
            print(f"GET {r.url[:300]} -> {r.status_code} {len(r.content)} bytes", flush=True)
            try:
                return r.status_code, r.json()
            except ValueError:
                return r.status_code, r.text[:300]
        except requests.RequestException as exc:
            print("retry", type(exc).__name__, str(exc)[:200], flush=True)
            time.sleep(10 * (attempt + 1))
    return 0, "no answer"


def main() -> int:
    st, root = get(f"{BASE}?f=json")
    print("FOLDER", json.dumps(root)[:3000], flush=True)
    st, robots = get("https://wody.gios.gov.pl/robots.txt")
    print("ROBOTS", st, str(robots)[:300], flush=True)
    for svc in SERVICES + ["JCWP_2022_2027"]:
        st, meta = get(f"{BASE}/{svc}/MapServer", {"f": "json"})
        if not isinstance(meta, dict):
            annotate("error", f"probe {svc}", {"status": st, "body": meta})
            continue
        layers = [(x["id"], x["name"]) for x in meta.get("layers", [])]
        out: dict = {"caps": meta.get("capabilities"), "layers": layers, "desc": (meta.get("serviceDescription") or "")[:200]}
        for lid, _ in layers:
            st, lm = get(f"{BASE}/{svc}/MapServer/{lid}", {"f": "json"})
            if not isinstance(lm, dict):
                out[lid] = {"status": st}
                continue
            fields = [(f["name"], f["type"].replace("esriFieldType", ""), f.get("alias")) for f in lm.get("fields") or []]
            oid = lm.get("objectIdField") or next((f[0] for f in fields if f[1] == "OID"), None)
            info: dict = {"name": lm.get("name"), "type": lm.get("type"), "fields": fields, "oid": oid,
                          "max": lm.get("maxRecordCount"), "adv": lm.get("advancedQueryCapabilities"),
                          "drawing": json.dumps((lm.get("drawingInfo") or {}).get("renderer"))[:1500]}
            if svc == "JCWP_2022_2027":
                out[lid] = info
                continue
            st, c = get(f"{BASE}/{svc}/MapServer/{lid}/query", {"where": "1=1", "returnCountOnly": "true", "f": "json"})
            info["count"] = c.get("count") if isinstance(c, dict) else c
            st, ids = get(f"{BASE}/{svc}/MapServer/{lid}/query", {"where": "1=1", "returnIdsOnly": "true", "f": "json"})
            idl = sorted(ids.get("objectIds") or []) if isinstance(ids, dict) else []
            info["ids"] = {"n": len(idl), "min": idl[0] if idl else None, "max": idl[-1] if idl else None,
                           "contiguous": idl == list(range(idl[0], idl[-1] + 1)) if idl else None,
                           "field": ids.get("objectIdFieldName") if isinstance(ids, dict) else None}
            feats = []
            if idl and oid:
                lo = idl[0]
                while lo <= idl[-1]:
                    st, page = get(f"{BASE}/{svc}/MapServer/{lid}/query", {
                        "where": f"{oid}>={lo} AND {oid}<{lo + 1000}", "outFields": "*", "returnGeometry": "false",
                        "f": "json"})
                    if not isinstance(page, dict) or "features" not in page:
                        info["page_error"] = str(page)[:300]
                        break
                    if page.get("exceededTransferLimit"):
                        info["exceeded"] = lo
                    feats += [f["attributes"] for f in page["features"]]
                    lo += 1000
            info["features"] = len(feats)
            prof = {}
            for name, _, _ in fields:
                vals = collections.Counter(str(f.get(name)) for f in feats)
                prof[name] = dict(vals.most_common(25)) if len(vals) <= 25 else {"distinct": len(vals), "nulls": vals.get("None", 0)}
            info["profile"] = prof
            codes = collections.Counter(f.get("KOD_JCWP") for f in feats)
            info["dup_codes"] = sum(1 for v in codes.values() if v > 1)
            info["sample"] = feats[:2]
            out[lid] = info
            print("LAYER", svc, lid, json.dumps(info, ensure_ascii=False), flush=True)
        print("SERVICE", svc, json.dumps(out, ensure_ascii=False)[:20000], flush=True)
        compact = {"caps": out["caps"], "layers": len(layers)}
        for lid, _ in layers:
            i = out.get(lid) or {}
            compact[str(lid)] = {"count": i.get("count"), "feat": i.get("features"), "dup": i.get("dup_codes"),
                                 "oid": i.get("oid"), "contig": (i.get("ids") or {}).get("contiguous"),
                                 "fields": [f[0] for f in i.get("fields", [])][:12]}
        annotate("notice", f"probe {svc}", compact)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
