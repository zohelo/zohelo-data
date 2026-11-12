"""PISA via the World Bank Education Statistics (read-only): source 12 metadata, LO.PISA indicator codes, cycles, Poland."""

import json

from dev.w3intl_probe import annotate, js, log
from dev.w3intl_probe3 import call

API = "https://api.worldbank.org/v2"


def main():
    out = {}
    r = call("GET", f"{API}/sources/12?format=json", timeout=120, tries=3)
    d = js(r) if "body" in r else None
    out["source12"] = d[1][0] if isinstance(d, list) and len(d) > 1 else {"st": r.get("st"), "sec": r.get("sec")}
    r = call("GET", f"{API}/indicator?source=12&format=json&per_page=20000", timeout=180, tries=3)
    d = js(r) if "body" in r else None
    if isinstance(d, list) and len(d) > 1:
        pisa = [(i["id"], i["name"], (i.get("sourceOrganization") or "")[:120]) for i in d[1] if "PISA" in i["id"]]
        out["pisa_indicators"] = [p[:2] for p in pisa]
        out["source_org"] = sorted({p[2] for p in pisa})[:3]
        log("pisa indicators", pisa)
    else:
        out["indicators_err"] = {"st": r.get("st"), "sec": r.get("sec")}
    url = f"{API}/country/all/indicator/LO.PISA.MAT;LO.PISA.MAT.FE;LO.PISA.MAT.MA?source=12&format=json&per_page=20000"
    r = call("GET", url, timeout=300, tries=3)
    d = js(r) if "body" in r else None
    if isinstance(d, list) and len(d) > 1:
        recs = d[1] or []
        vals = [x for x in recs if x.get("value") is not None]
        out["math"] = {"meta": d[0], "records": len(recs), "with_value": len(vals), "sec": r["sec"], "bytes": len(r["body"]),
                       "years": sorted({x["date"] for x in vals}), "areas": len({x["countryiso3code"] for x in vals}),
                       "pl": sorted([(x["indicator"]["id"], x["date"], x["value"]) for x in vals if x["countryiso3code"] == "POL"]),
                       "rec": json.dumps(vals[0])[:600] if vals else None,
                       "obs_status": sorted({str(x.get("obs_status")) for x in recs})}
    else:
        out["math"] = {"st": r.get("st"), "sec": r.get("sec"), "body": (r.get("body") or b"")[:300].decode("utf-8", "replace")}
    annotate("notice", "probe pisa wb 1/2", {k: out.get(k) for k in ("source12", "pisa_indicators", "source_org", "indicators_err")})
    annotate("notice", "probe pisa wb 2/2", out.get("math"))


if __name__ == "__main__":
    main()
