"""#2567 (read-only): does Silver hold Eurostat prc_hicp_minr, and which codes give the all-items annual rate?"""

import json
import urllib.request

from ingestion.common.r2 import sql
from ingestion.common.report import annotate

API = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{ds}?{q}"


def eurostat(ds: str, q: str) -> dict:
    request = urllib.request.Request(API.format(ds=ds, q=q), headers={"User-Agent": "zohelo-dev"})
    with urllib.request.urlopen(request, timeout=120) as response:
        data = json.loads(response.read())
    dims = {}
    for dim in data.get("id", []):
        cats = data["dimension"][dim]["category"]
        labels = cats.get("label", {})
        codes = list(cats["index"]) if isinstance(cats["index"], dict) else cats["index"]
        dims[dim] = codes[:6] if dim == "time" else [(c, labels.get(c)) for c in codes[:12]]
        if dim == "time":
            dims["time_range"] = [codes[0], codes[-1], len(codes)]
    return {"updated": data.get("updated"), "size": data.get("size"), "dims": dims}


def run(title: str, fn) -> None:
    try:
        annotate("notice", title, fn())
    except Exception as exc:  # report and continue
        annotate("warning", title, {"error": f"{type(exc).__name__}: {str(exc)[:800]}"})


def main() -> int:
    ids = "('PRC_HICP_MINR', 'prc_hicp_minr', 'PRC_HICP_MANR', 'prc_hicp_manr')"
    run(
        "silver hicp datasets",
        lambda: sql(
            "select dataset_id, freq_code, unit_code, breakdown_dimensions, count(*) as n, "
            "min(time_period) as first, max(time_period) as last "
            f"from silver.eurostat_observations where dataset_id in {ids} and geo_code = 'PL' "
            "group by dataset_id, freq_code, unit_code, breakdown_dimensions"
        ),
    )
    run(
        "silver minr PL totals",
        lambda: sql(
            "select unit_code, breakdown_codes, min(time_period) as first, max(time_period) as last, count(*) as n "
            "from silver.eurostat_observations where dataset_id in ('PRC_HICP_MINR', 'prc_hicp_minr') "
            "and geo_code = 'PL' and (breakdown_codes like '%TOTAL%' or breakdown_codes like '%CP00%') "
            "group by unit_code, breakdown_codes"
        ),
    )
    run("eurostat minr PL", lambda: eurostat("prc_hicp_minr", "geo=PL&lastTimePeriod=2"))
    run("eurostat minr PL full span", lambda: eurostat("prc_hicp_minr", "geo=PL&unit=RCH_A&coicop18=TOTAL"))
    run("eurostat manr PL", lambda: eurostat("prc_hicp_manr", "geo=PL&unit=RCH_A&coicop=CP00&lastTimePeriod=2"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
