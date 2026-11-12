"""GEM round 3 (read-only): request timing for a series list × Poland and for one series × the EU27 members."""

import json

from dev.w3intl_probe import annotate, js, log
from dev.w3intl_probe3 import call

API = "https://api.worldbank.org/v2/sources/15"
SERIES = "CPTOTSAXNZGY;CPTOTSAXN;CORESA;IPTOTSAKD;UNEMPSA_;RETSALESSA;DPANUSSPB;NYGDPMKTPSAKD"
EU = "AUT;BEL;BGR;CYP;CZE;DEU;DNK;EST;ESP;FIN;FRA;GRC;HRV;HUN;IRL;ITA;LTU;LUX;LVA;MLT;NLD;POL;PRT;ROU;SWE;SVN;SVK"


def summary(r):
    d = js(r) if "body" in r else None
    if not isinstance(d, dict):
        return {"st": r.get("st"), "err": r.get("err"), "sec": r.get("sec")}
    recs = d["source"]["data"]
    per = {}
    for x in recs:
        v = {c["concept"]: c["id"] for c in x["variable"]}
        k = v.get("Series") or v.get("Country")
        t = v.get("Time", "")
        f = "M" if "M" in t else "Q" if "Q" in t else "A"
        per.setdefault(k, {}).setdefault(f, [0, 0])
        per[k][f][0] += 1
        per[k][f][1] += x.get("value") is not None
    return {"st": r["st"], "sec": r["sec"], "bytes": len(r["body"]), "total": d.get("total"), "pages": d.get("pages"), "per": per}


def main():
    out = {}
    out["pol_8_series"] = summary(call("GET", f"{API}/country/POL/series/{SERIES}/time/all?format=json&per_page=50000", tries=2))
    log("pol", out["pol_8_series"])
    r = call("GET", f"{API}/country/{EU}/series/CPTOTSAXNZGY/time/all?format=json&per_page=50000", tries=2)
    s = summary(r)
    out["eu27_one_series"] = {k: v for k, v in s.items() if k != "per"}
    out["eu27_one_series"]["countries"] = len(s.get("per", {}))
    log("eu27", s)
    annotate("notice", "probe3 gem timing", out)


if __name__ == "__main__":
    main()
