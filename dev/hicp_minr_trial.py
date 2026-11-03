"""Trial for #2567 (read-only): what silver.eurostat_observations holds for PRC_HICP_MINR, and the
all-items annual rate for PL, CZ, HU, DE next to the Eurostat API. Writes nothing."""

from __future__ import annotations

import json
import urllib.request

import duckdb
from pyiceberg.expressions import In

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

IDS = ["PRC_HICP_MINR", "prc_hicp_minr"]
GEOS = ["PL", "CZ", "HU", "DE"]
API = ("https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_minr"
       "?geo={geo}&sinceTimePeriod=2025-11&lang=en")


def api(geo: str) -> dict:
    r = urllib.request.Request(API.format(geo=geo), headers={"User-Agent": "zohelo-dev"})
    with urllib.request.urlopen(r, timeout=90) as f:
        d = json.loads(f.read())
    dims = d["id"]
    sizes = d["size"]
    cats = {k: {v: c for c, v in d["dimension"][k]["category"]["index"].items()} for k in dims}
    out = {}
    for flat, val in d["value"].items():
        i, codes = int(flat), {}
        for k, n in zip(reversed(dims), reversed(sizes)):
            codes[k] = cats[k][i % n]
            i //= n
        out[tuple(codes[k] for k in dims if k not in ("freq", "geo"))] = val
    keep = {",".join(k): v for k, v in out.items()
            if any(c in ("RCH_A",) for c in k) and any(c in ("CP00", "TOTAL") for c in k)}
    return {"updated": d.get("updated"), "dims": dims,
            "units": sorted(d["dimension"].get("unit", {}).get("category", {}).get("index", {}))[:20],
            "all_items_rch_a": dict(sorted(keep.items())[-24:])}


def main() -> None:
    cat = catalog()
    con = duckdb.connect()
    obs = cat.load_table(("silver", "eurostat_observations")).scan(
        row_filter=In("dataset_id", IDS),
        selected_fields=("dataset_id", "freq_code", "unit_code", "geo_code", "time_period", "value",
                         "breakdown_dimensions", "breakdown_codes", "source_version"),
    ).to_arrow()
    con.register("o", obs)
    annotate("notice", "minr overview", con.execute(
        """select dataset_id, freq_code, count(*), min(time_period), max(time_period),
                  string_agg(distinct unit_code, ','), string_agg(distinct breakdown_dimensions, ' | '),
                  any_value(source_version)
           from o group by all""").fetchall())
    dims = cat.load_table(("silver", "eurostat_dimensions")).scan(row_filter=In("dataset_id", IDS)).to_arrow()
    con.register("d", dims)
    annotate("notice", "minr dimensions", con.execute("select * from d").fetchall()[:30])
    annotate("notice", "minr all-items candidates PL", con.execute(
        """select unit_code, breakdown_codes, max(time_period), count(*)
           from o where geo_code = 'PL' and unit_code = 'RCH_A'
             and (list_contains(string_split(breakdown_codes, ','), 'CP00')
                  or list_contains(string_split(breakdown_codes, ','), 'TOTAL'))
           group by all""").fetchall())
    for geo in GEOS:
        rows = con.execute(
            """select time_period, value, breakdown_codes from o
               where geo_code = ? and unit_code = 'RCH_A' and freq_code = 'M'
                 and list_contains(string_split(breakdown_codes, ','), 'CP00')
                 and time_period >= '2025-10' order by time_period""", [geo]).fetchall()
        try:
            prov = api(geo)
        except Exception as e:  # report, do not fail the trial
            prov = f"error: {e}"
        annotate("notice", f"minr {geo}", {"silver": rows, "eurostat_api": prov})
    old = cat.load_table(("silver", "eurostat_observations")).scan(
        row_filter=In("dataset_id", ["PRC_HICP_MANR", "prc_hicp_manr"]),
        selected_fields=("geo_code", "unit_code", "time_period", "value", "breakdown_codes"),
    ).to_arrow()
    con.register("m", old)
    annotate("notice", "manr vs minr overlap PL 2025", con.execute(
        """select m.time_period, m.value as manr, n.value as minr
           from m join o as n on n.geo_code = m.geo_code and n.time_period = m.time_period
             and n.unit_code = 'RCH_A' and list_contains(string_split(n.breakdown_codes, ','), 'CP00')
           where m.geo_code = 'PL' and m.unit_code = 'RCH_A'
             and list_contains(string_split(m.breakdown_codes, ','), 'CP00')
             and m.time_period >= '2025-01' order by 1""").fetchall())


if __name__ == "__main__":
    main()
