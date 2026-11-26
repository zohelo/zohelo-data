"""Read-only probe of the PIG-PIB CBDG boreholes WFS (register PL-ENV-014, Task #3300).

GET only, one request at a time, 5 s apart, at most 14 requests, nothing written to R2. Reports: capabilities
(paging/sorting constraints, CountDefault, output formats), DescribeFeatureType, resultType=hits, a 3-feature GEOJSON
sample, the same 5,000-feature page twice with SORTBY and once without (stable?), and one large page to find the
server's maximum count; a DuckDB profile of the pages read (closed lists, key uniqueness, value shapes).
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import time

import duckdb
import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
WFS = "https://cbdgmapa.pgi.gov.pl/arcgis/services/cbdg_otwory/MapServer/WFSServer"
TYPE = "cbdg_otwory:Otwory_wiertnicze_CBDG"
OUT = Path(os.environ.get("DEV_OUTPUT_DIR", tempfile.gettempdir()))
LEFT = {"n": 10, "req": 8}


def note(title: str, data) -> None:
    print(f"[{title}] {json.dumps(data, ensure_ascii=False, default=str)[:20000]}", flush=True)
    if LEFT["n"] > 0:
        LEFT["n"] -= 1
        annotate("notice", title, data)


def get(params: dict, name: str) -> tuple[int, bytes, float]:  # noqa: D103
    if LEFT["req"] <= 0:
        raise RuntimeError("request budget spent")
    LEFT["req"] -= 1
    time.sleep(5)
    started = time.monotonic()
    r = requests.get(WFS, params={"SERVICE": "WFS", "VERSION": "2.0.0", **params}, headers={"User-Agent": UA},
                     timeout=(30, 900))
    seconds = round(time.monotonic() - started, 1)
    (OUT / name).write_bytes(r.content)
    return r.status_code, r.content, seconds


def feature_page(count: int, start: int, sort: str | None, name: str) -> dict:
    params = {"REQUEST": "GetFeature", "TYPENAMES": TYPE, "OUTPUTFORMAT": "GEOJSON", "COUNT": count,
              "STARTINDEX": start}
    if sort:
        params["SORTBY"] = sort
    status, body, seconds = get(params, name)
    out = {"http": status, "bytes": len(body), "seconds": seconds, "sha": hashlib.sha256(body).hexdigest()[:16]}
    try:
        doc = json.loads(body)
        feats = doc.get("features", [])
        ids = [f["properties"].get("OBJECTID") for f in feats]
        out.update({"features": len(feats), "first_ids": ids[:3], "last_id": ids[-1:] if ids else None,
                    "ids_sorted": ids == sorted(ids, key=lambda v: (v is None, v)), "keys": sorted(doc)[:10]})
    except ValueError:
        out["head"] = body[:400].decode("utf-8", "replace")
    return out


def main() -> int:
    status, sample, _ = get({"REQUEST": "GetFeature", "TYPENAMES": TYPE, "OUTPUTFORMAT": "GEOJSON", "COUNT": 2},
                            "sample.json")
    note("boreholes sample", {"http": status, "body": sample[:3500].decode("utf-8", "replace")})
    pages = {
        "a": feature_page(5000, 150000, None, "p_a.json"),
        "b": feature_page(5000, 150000, None, "p_b.json"),
        "first": feature_page(5000, 0, None, "p_first.json"),
        "last": feature_page(5000, 335000, None, "p_last.json"),
    }
    note("boreholes pages", pages)
    big = feature_page(25000, 60000, None, "p_big.json")
    note("boreholes big page", big)

    con = duckdb.connect()
    files = [str(OUT / n) for n in ("p_big.json", "p_a.json", "p_first.json", "p_last.json") if (OUT / n).exists()]
    con.execute(
        "create table f as select unnest(from_json(j->'features', '[\"JSON\"]')) as f from "
        f"(select content::JSON as j from read_text({files!r}))"
    )
    keys = [r[0] for r in con.execute("select distinct unnest(json_keys(f->'properties')) from f").fetchall()]
    prof: dict = {"rows": con.execute("select count(*) from f").fetchone()[0], "keys": keys,
                  "geometry_types": con.execute("select f->'geometry'->>'type', count(*) from f group by 1").fetchall()}
    for k in keys:
        col = f"f->'properties'->>'{k}'"
        prof[k] = con.execute(
            f"select count(distinct {col}), count(*) filter (where {col} is null), "
            f"count(*) filter (where trim({col}) in ('', 'null', 'Null', '-', 'brak', 'b.d.')), "
            f"list(distinct regexp_replace({col}, '[0-9]', '9', 'g'))[1:6], list(distinct {col})[1:4] from f"
        ).fetchone()
    small = {}
    for k in keys:
        col = f"f->'properties'->>'{k}'"
        n = con.execute(f"select count(distinct {col}) from f").fetchone()[0]
        if n <= 40:
            small[k] = con.execute(f"select {col}, count(*) from f group by 1 order by 2 desc").fetchall()
    note("boreholes profile", prof)
    note("boreholes closed lists", small)
    dup = {}
    for k in ("ID_CBDG", "OBJECTID", "Nr_inwentarzowy"):
        if k in keys:
            dup[k] = con.execute(
                f"select count(*), count(distinct f->'properties'->>'{k}') from f").fetchone()
    note("boreholes keys", {"dup": dup, "sample_geom": con.execute("select (f->'geometry')::varchar from f limit 2")
                            .fetchall()})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
