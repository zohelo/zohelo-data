"""QE check after zohelo-bi#334 (read-only): HICP y/y in published latest.json for PL, CZ, HU, DE vs Eurostat.

Also lists the geography names, so the bi tile filter `Polska|Czechia|Hungary|Germany` can be checked.
"""

import json
import urllib.request

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate

KEY = "06_published/v1/kpi/inflation/latest.json"
GEOS = ["PL", "CZ", "HU", "DE"]
URL = ("https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_manr"
       "?geo={geo}&coicop=CP00&unit=RCH_A&sinceTimePeriod=2025-06")


def eurostat(geo: str) -> dict:
    req_ = urllib.request.Request(URL.format(geo=geo), headers={"User-Agent": "zohelo-qe"})
    with urllib.request.urlopen(req_, timeout=60) as r:
        data = json.loads(r.read())
    index = data["dimension"]["time"]["category"]["index"]
    pos = {v: k for k, v in index.items()}
    vals = {pos[int(i)]: v for i, v in data["value"].items()}
    last = sorted(vals)[-3:]
    return {"updated": data.get("updated"), "last": {k: vals[k] for k in last}}


def main() -> int:
    latest = json.loads(s3().get_object(Bucket=req("STORAGE_LAKEHOUSE_BUCKET_NAME"), Key=KEY)["Body"].read())
    country = [v for v in latest["values"] if v.get("geography_level") == "country"]
    names = sorted({(v.get("geography_key"), v.get("geography_name")) for v in country
                    if v.get("geography_key") in GEOS or v.get("geography_name") in ("Hungary", "Węgry", "Czechia", "Germany")})
    annotate("notice", "qe inflation names", {"generated_at": latest.get("generated_at"),
                                               "unit_key": latest.get("unit_key"), "names": names,
                                               "row_keys": sorted(country[0].keys()) if country else []})
    for geo in GEOS:
        rows = [{k: v.get(k) for k in ("source_key", "geography_name", "date_key", "period", "value", "unit_key")}
                for v in country if v.get("geography_key") == geo]
        try:
            prov = eurostat(geo)
        except Exception as e:  # report, do not fail the check
            prov = f"error: {e}"
        annotate("notice", f"qe inflation {geo}", {"published": rows, "eurostat": prov})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
