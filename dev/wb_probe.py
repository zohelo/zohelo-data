"""Read-only probe of the World Bank sources beyond WDI (Findex, ICP, IDS, WGI): file structure for Bronze/Silver design.

Downloads to the runner's temp dir only; writes nothing to R2. Prints structure to the job log.
"""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path
import sys
import tempfile
import zipfile

import requests

from ingestion.to_landing.patterns import bulk_file

UA = {"User-Agent": "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"}
EU = "AUT BEL BGR HRV CYP CZE DNK EST FIN FRA DEU GRC HUN IRL ITA LVA LTU LUX MLT NLD POL PRT ROU SVK SVN ESP SWE".split()


def get(url: str) -> bytes:
    r = requests.get(url, headers=UA, timeout=600)
    print("GET", url, r.status_code, len(r.content), r.headers.get("Last-Modified"), r.headers.get("Content-Type"))
    r.raise_for_status()
    return r.content


def show_csv(name: str, text: str, key_cols=("Country Code",), max_rows=3) -> None:
    rows = list(csv.reader(io.StringIO(text)))
    header = rows[0]
    print(f"--- {name}: {len(rows) - 1} rows, {len(header)} cols")
    print("HEADER", header[:60])
    for r in rows[1 : 1 + max_rows]:
        print("ROW", r[:40])
    for k in key_cols:
        if k in header:
            i = header.index(k)
            codes = sorted({r[i] for r in rows[1:] if len(r) > i})
            print(f"DISTINCT {k}: {len(codes)}", codes[:400])
    return rows


def bulk_zip(url: str) -> None:
    body = get(url)
    z = zipfile.ZipFile(io.BytesIO(body))
    for m in z.infolist():
        print("MEMBER", m.filename, m.file_size)
    for m in z.infolist():
        if not m.filename.lower().endswith(".csv"):
            continue
        text = z.read(m).decode("utf-8-sig")
        rows = show_csv(m.filename, text, key_cols=("Country Code", "Series Code", "Indicator Code", "Counterpart-Area Code", "Code", "CountryCode", "SeriesCode"))
        header = rows[0]
        if "Country Code" in header and len(rows) > 1000:
            ci = header.index("Country Code")
            ii = header.index("Indicator Code") if "Indicator Code" in header else header.index("Series Code") if "Series Code" in header else None
            pl = [r for r in rows[1:] if r[ci] == "POL"]
            print("POL rows", len(pl))
            for r in pl[:80]:
                print("POL", [c for c in r[:6]], "last values", [v for v in r[6:] if v][-4:])
            for agg in ("EUU", "EMU", "WLD", "HIC"):
                print(agg, "rows", sum(1 for r in rows[1:] if r[ci] == agg))
            print("EU members present", [c for c in EU if any(r[ci] == c for r in rows[1:])])
            if ii is not None:
                print("indicators", len({r[ii] for r in rows[1:]}))
                print("INDICATORS", sorted({(r[ii], r[ii - 1]) for r in rows[1:]})[:200])


def findex() -> None:
    spec = bulk_file.declaration("world_bank_findex")[0]
    links = bulk_file.page_links(spec)
    print("FINDEX links", links)
    text = get(links[0]).decode("utf-8-sig", errors="replace")
    rows = list(csv.reader(io.StringIO(text)))
    header = rows[0]
    print("FINDEX rows", len(rows) - 1, "cols", len(header))
    print("HEADER", header[:80])
    print("HEADER tail", header[80:200])
    for r in rows[1:6]:
        print("ROW", r[:20])
    for i, h in enumerate(header[:12]):
        vals = sorted({r[i] for r in rows[1:] if len(r) > i})
        print(f"COL {i} {h}: {len(vals)} distinct", vals[:300])
    code = next((i for i, h in enumerate(header) if "code" in h.lower()), None)
    if code is not None:
        for r in rows[1:]:
            if r[code] in ("POL", "EUU", "ECS"):
                print("POLROW", r[:30])


def icp() -> None:
    from ingestion.to_landing.world_bank_icp import extract as ex

    s = requests.Session()
    s.headers.update(UA)
    version, series = ex.catalogue(s)
    print("ICP version", version, "series", len(series), series)
    meta = json.loads(get(f"{ex.API}/sources/{ex.SOURCE_ID}/concepts?format=json"))
    print("ICP concepts", json.dumps(meta)[:3000])
    for sid in series[:1] + [x for x in series if "PPP" in x.upper() or "PLI" in x.upper()][:2]:
        body = get(ex.series_url(sid, 1))
        p = json.loads(body)
        print("ICP page keys", list(p) if isinstance(p, dict) else type(p), json.dumps(p)[:1500])
        data = p["source"]["data"] if isinstance(p, dict) else p[1]
        concepts = {}
        for d in data:
            for v in d["variable"]:
                concepts.setdefault(v["concept"], set()).add(v["id"])
        print("ICP", sid, "obs", len(data), {k: (len(v), sorted(v)[:60]) for k, v in concepts.items()})
        for d in data:
            ids = {v["concept"]: v["id"] for v in d["variable"]}
            if ids.get("Country") in ("POL", "EUU") and d.get("value") is not None:
                print("ICPPOL", sid, d["variable"], d["value"])
                break
    for sid in series:
        body = get(f"{ex.API}/sources/{ex.SOURCE_ID}/country/POL/series/{sid}/time/all?format=json&per_page=50000")
        p = json.loads(body)
        data = p["source"]["data"] if isinstance(p, dict) else p[1]
        sample = [(([v["id"] for v in d["variable"]]), d.get("value")) for d in data if d.get("value") is not None]
        print("ICP POL", sid, len(data), "non-null", len(sample), sample[:3])


def main() -> int:
    which = sys.argv[1:] or ["wgi", "ids", "findex", "icp"]
    for w in which:
        print("=" * 20, w)
        try:
            if w == "wgi":
                bulk_zip("https://databank.worldbank.org/data/download/WGI_CSV.zip")
            elif w == "ids":
                bulk_zip("https://databank.worldbank.org/data/download/IDS_CSV.zip")
            elif w == "findex":
                findex()
            elif w == "icp":
                icp()
        except Exception as exc:  # noqa: BLE001
            print("FAILED", w, type(exc).__name__, str(exc)[:500])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
