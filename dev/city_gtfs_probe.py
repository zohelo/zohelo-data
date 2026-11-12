"""Read-only probe of the four city GTFS feeds (PL-ENV-029..032), #2977-#2980. No R2 access, no writes.

One request at a time; 10 s between requests to one host (Gdańsk robots.txt Crawl-delay 10, used everywhere).
Gdańsk: only the fixed download URL (robots.txt disallows the CKAN API). Profiles each ZIP with DuckDB and
prints one JSON line per feed (job log) plus a compact annotation.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import tempfile
import time
import zipfile

import duckdb
import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns.bulk_file import USER_AGENT, links_matching

FEEDS = {
    "gdansk": [
        "https://ckan.multimediagdansk.pl/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/"
        "30e783e4-2bec-4a7d-bb22-ee3e3b26ca96/download/gtfsgoogle.zip"
    ],
    "poznan": ["https://www.ztm.poznan.pl/pl/dla-deweloperow/getGTFSFile"],
    "krakow": [
        "https://gtfs.ztp.krakow.pl/GTFS_KRK_A.zip",
        "https://gtfs.ztp.krakow.pl/GTFS_KRK_M.zip",
        "https://gtfs.ztp.krakow.pl/GTFS_KRK_T.zip",
    ],
}
PAGES = {
    "krakow": "https://gtfs.ztp.krakow.pl/",
    "wroclaw": "https://open-data.cui.wroclaw.pl/hdb/ft/6/",
}
ROBOTS = ["https://www.ztm.poznan.pl/robots.txt", "https://gtfs.ztp.krakow.pl/robots.txt",
          "https://open-data.cui.wroclaw.pl/robots.txt"]
PAUSE = 10
OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))


def get(url: str, stream_to: Path | None = None) -> dict:
    time.sleep(PAUSE)
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"}, timeout=(30, 300),
                         stream=stream_to is not None)
    except requests.RequestException as exc:
        return {"url": url, "error": f"{type(exc).__name__}: {str(exc)[:200]}"}
    info = {"url": url, "status": r.status_code, "final": r.url,
            "headers": {k: r.headers.get(k) for k in ("Content-Type", "Content-Length", "Last-Modified", "ETag",
                                                       "Content-Disposition")}}
    if stream_to is not None and r.status_code == 200:
        with stream_to.open("wb") as h:
            for block in r.iter_content(8 << 20):
                h.write(block)
        info["bytes"] = stream_to.stat().st_size
    elif stream_to is None:
        info["text"] = r.text
    return info


def q(con, sql: str):
    try:
        return con.execute(sql).fetchall()
    except duckdb.Error as exc:
        return f"error: {str(exc)[:200]}"


def profile(path: Path) -> dict:
    out: dict = {}
    with zipfile.ZipFile(path) as z:
        out["members"] = {i.filename: i.file_size for i in z.infolist()}
        d = Path(tempfile.mkdtemp())
        z.extractall(d)
    heads = {}
    for f in sorted(d.rglob("*.txt")):
        raw = f.open("rb").readline()
        heads[f.name] = {"bom": raw.startswith(b"\xef\xbb\xbf"), "header": raw.decode("utf-8-sig", "replace").strip()}
        try:
            f.read_bytes().decode("utf-8")
            heads[f.name]["utf8"] = True
        except UnicodeDecodeError as exc:
            heads[f.name]["utf8"] = str(exc)[:120]
    out["headers"] = heads
    con = duckdb.connect()

    def t(name: str) -> str:
        return f"read_csv('{d / name}', all_varchar=true, header=true)"

    names = {p.name for p in d.rglob("*.txt")}
    for name in sorted(names):
        out.setdefault("rows", {})[name] = q(con, f"select count(*) from {t(name)}")[0][0] if name != "shapes.txt" else None
    if "routes.txt" in names:
        out["route_type"] = q(con, f"select route_type, count(*) from {t('routes.txt')} group by 1 order by 1")
        out["routes_dup"] = q(con, f"select count(*) - count(distinct route_id) from {t('routes.txt')}")
        out["routes_sample"] = q(con, f"select * from {t('routes.txt')} limit 3")
    if "stops.txt" in names:
        out["stops_dup"] = q(con, f"select count(*) - count(distinct stop_id) from {t('stops.txt')}")
        out["stops_sample"] = q(con, f"select * from {t('stops.txt')} limit 3")
        out["stops_null_coord"] = q(con, f"select count(*) from {t('stops.txt')} where stop_lat is null or stop_lon is null")
    if "trips.txt" in names:
        out["trips_dup"] = q(con, f"select count(*) - count(distinct trip_id) from {t('trips.txt')}")
        out["trips_sample"] = q(con, f"select * from {t('trips.txt')} limit 2")
        out["trip_services"] = q(con, f"select count(distinct service_id) from {t('trips.txt')}")
        out["trips_route_missing"] = q(con, f"select count(*) from {t('trips.txt')} where route_id not in (select route_id from {t('routes.txt')})")
    if "calendar.txt" in names:
        out["calendar"] = q(con, f"select count(*), count(distinct service_id), min(start_date), max(end_date) from {t('calendar.txt')}")
        out["calendar_sample"] = q(con, f"select * from {t('calendar.txt')} limit 3")
    if "calendar_dates.txt" in names:
        out["calendar_dates"] = q(con, f"select exception_type, count(*), count(distinct service_id), min(date), max(date) from {t('calendar_dates.txt')} group by 1")
        out["calendar_dates_dup"] = q(con, f"select count(*) from (select service_id, date from {t('calendar_dates.txt')} group by all having count(*) > 1)")
    if "trips.txt" in names:
        known = []
        if "calendar.txt" in names:
            known.append(f"select service_id from {t('calendar.txt')}")
        if "calendar_dates.txt" in names:
            known.append(f"select service_id from {t('calendar_dates.txt')}")
        if known:
            out["trip_services_unknown"] = q(con, f"select count(distinct service_id) from {t('trips.txt')} where service_id not in ({' union '.join(known)})")
    if "stop_times.txt" in names:
        out["stop_times"] = q(con, f"with c as (select trip_id, count(*) as n from {t('stop_times.txt')} group by 1) select count(*), sum(n), min(n), max(n) from c")
        out["trips_without_stop_times"] = q(con, f"select count(*) from {t('trips.txt')} where trip_id not in (select trip_id from {t('stop_times.txt')})")
        out["stop_times_sample"] = q(con, f"select * from {t('stop_times.txt')} limit 2")
        out["stop_times_max_departure"] = q(con, f"select max(departure_time), min(departure_time), count(*) filter (where departure_time is null) from {t('stop_times.txt')}")
    if "feed_info.txt" in names:
        out["feed_info"] = q(con, f"select * from {t('feed_info.txt')}")
    if "agency.txt" in names:
        out["agency"] = q(con, f"select * from {t('agency.txt')}")
    return out


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    report: dict = {}
    for url in ROBOTS:
        r = get(url)
        report[url] = {k: r.get(k) for k in ("status", "error")} | {"text": (r.get("text") or "")[:600]}
        print(json.dumps(report[url]), flush=True)
    for city, page in PAGES.items():
        r = get(page)
        text = r.pop("text", "") or ""
        r["zip_links"] = links_matching(text, r.get("final") or page, r"\.zip$|/hdb/download/")[:15]
        if city == "wroclaw":
            r["snippets"] = [m.group(0)[:300] for m in re.finditer(r"<a[^>]+/hdb/download/[^>]*>.*?</a>", text, re.S)][:12]
            r["dates"] = re.findall(r"GTFS_\d{8}", text)[:15]
        report[page] = r
        print(json.dumps({"page": page, **r}, default=str), flush=True)
    wro = report[PAGES["wroclaw"]].get("zip_links") or []
    FEEDS["wroclaw"] = wro[:1]
    for city, urls in FEEDS.items():
        for url in urls:
            path = Path(tempfile.mkdtemp()) / "feed.zip"
            r = get(url, path)
            if r.get("status") == 200 and path.exists():
                try:
                    r["profile"] = profile(path)
                except (zipfile.BadZipFile, OSError) as exc:
                    r["profile_error"] = str(exc)[:200]
            line = json.dumps({"city": city, **r}, default=str)
            print(line, flush=True)
            (OUT / f"{city}_{url.rsplit('/', 1)[-1]}.json").write_text(line)
            p = r.get("profile") or {}
            annotate("notice", f"probe {city} {url.rsplit('/', 1)[-1]}", {
                "status": r.get("status"), "error": r.get("error"), "bytes": r.get("bytes"), "headers": r.get("headers"),
                "rows": p.get("rows"), "route_type": p.get("route_type"), "calendar": p.get("calendar"),
                "calendar_dates": p.get("calendar_dates"), "feed_info": p.get("feed_info"),
                "unknown_services": p.get("trip_services_unknown"), "stop_times": p.get("stop_times"),
                "no_stop_times": p.get("trips_without_stop_times"),
                "dups": [p.get("routes_dup"), p.get("stops_dup"), p.get("trips_dup"), p.get("calendar_dates_dup")],
            })
    annotate("notice", "probe pages", {k: {"status": v.get("status"), "links": v.get("zip_links"),
                                            "dates": v.get("dates")} for k, v in report.items() if "zip_links" in v})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
