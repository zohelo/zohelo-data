"""Read-only Dev probe for OpenStreetMap Poland (CD-027, #3262): Geofabrik pages and HEADs, two voivodeship
extracts read with DuckDB spatial ST_ReadOSM (columns, kinds, timing, tag counts). No R2 writes. Deleted before merge."""

from __future__ import annotations

import html
import json
import re
import resource
import sys
import tempfile
import time
import zlib
from pathlib import Path

import duckdb
import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://download.geofabrik.de/europe/poland"


def text_of(url: str) -> tuple[int, str]:
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=60)
        return r.status_code, r.text
    except requests.RequestException as exc:
        return 0, repr(exc)


def strip(page: str) -> str:
    page = re.sub(r"<script.*?</script>|<style.*?</style>", " ", page, flags=re.S | re.I)
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", page))).strip()


def around(text: str, word: str, width: int = 400) -> list[str]:
    return [text[max(0, m.start() - width // 2): m.start() + width] for m in re.finditer(word, text)][:3]


def varint(buf: bytes, i: int) -> tuple[int, int]:
    shift = value = 0
    while True:
        b = buf[i]
        i += 1
        value |= (b & 0x7F) << shift
        if b < 0x80:
            return value, i
        shift += 7


def fields(buf: bytes) -> dict[int, list]:
    out: dict[int, list] = {}
    i = 0
    while i < len(buf):
        key, i = varint(buf, i)
        num, wire = key >> 3, key & 7
        if wire == 0:
            v, i = varint(buf, i)
        elif wire == 2:
            n, i = varint(buf, i)
            v, i = buf[i:i + n], i + n
        elif wire == 1:
            v, i = buf[i:i + 8], i + 8
        elif wire == 5:
            v, i = buf[i:i + 4], i + 4
        else:
            raise ValueError(f"wire type {wire}")
        out.setdefault(num, []).append(v)
    return out


def pbf_header(path: Path) -> dict:
    with path.open("rb") as f:
        n = int.from_bytes(f.read(4), "big")
        bh = fields(f.read(n))
        size = bh[3][0]
        blob = fields(f.read(size))
    data = zlib.decompress(blob[3][0]) if 3 in blob else blob[1][0]
    hb = fields(data)
    return {
        "type": bh[1][0].decode(),
        "required": [x.decode() for x in hb.get(4, [])],
        "optional": [x.decode() for x in hb.get(5, [])],
        "writingprogram": hb.get(16, [b""])[0].decode(errors="replace"),
        "replication_timestamp": hb.get(32, [None])[0],
        "replication_sequence": hb.get(33, [None])[0],
        "replication_base_url": hb.get(34, [b""])[0].decode(errors="replace"),
    }


KEYS = ["amenity", "building", "craft", "emergency", "healthcare", "highway", "leisure", "office",
        "public_transport", "railway", "shop", "tourism"]


def main(argv) -> int:
    out: dict = {}
    code, page = text_of(f"{BASE}.html")
    txt = strip(page)
    links = sorted(set(re.findall(r'href="(?:\./)?(?:poland/)?([a-z\-]+)-latest\.osm\.pbf"', page)))
    out["poland_page"] = {"http": code, "pbf_links": links, "licence_text": around(txt, "ODbL|Open Database|licen", 500)}
    print(json.dumps(out["poland_page"], ensure_ascii=False), flush=True)
    code, idx = text_of("https://download.geofabrik.de/")
    out["index_licence"] = around(strip(idx), "ODbL|Open Database|licen|OpenStreetMap contributors", 500)
    code, cp = text_of("https://www.openstreetmap.org/copyright/en")
    out["osm_copyright_en"] = {"http": code, "quote": around(strip(cp), "Open Data Commons Open Database License", 600)[:1]}
    code, cpl = text_of("https://www.openstreetmap.org/copyright/pl")
    out["osm_copyright_pl"] = {"http": code, "quote": around(strip(cpl), "Open Data Commons Open Database License", 600)[:1]}
    code, sub = text_of(f"{BASE}/mazowieckie.html")
    out["sub_page"] = {"http": code, "text": around(strip(sub), "ODbL|licen|contain", 400)}
    print(json.dumps(out, ensure_ascii=False), flush=True)
    annotate("notice", "osm probe pages", out)
    heads = {}
    for name in links:
        r = requests.head(f"{BASE}/{name}-latest.osm.pbf", headers={"User-Agent": UA}, timeout=60, allow_redirects=True)
        heads[name] = [r.status_code, int(r.headers.get("Content-Length") or 0), r.headers.get("Last-Modified"), r.headers.get("ETag")]
        time.sleep(1)
    annotate("notice", "osm probe heads", {"files": len(heads), "bytes": sum(v[1] for v in heads.values()), "heads": heads})
    if not heads:
        return 1
    picks = argv or [min(heads, key=lambda k: heads[k][1]), max(heads, key=lambda k: heads[k][1])]
    con = duckdb.connect()
    t = time.monotonic()
    con.execute("INSTALL spatial; LOAD spatial;")
    out2: dict = {"install_s": round(time.monotonic() - t, 1), "duckdb": duckdb.__version__}
    out2["fn"] = con.sql("select function_name, parameters from duckdb_functions() where function_name ilike '%osm%'").fetchall()
    annotate("notice", "osm probe duckdb", out2)
    for name in picks:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / f"{name}.osm.pbf"
            t = time.monotonic()
            with requests.get(f"{BASE}/{name}-latest.osm.pbf", headers={"User-Agent": UA}, stream=True, timeout=(30, 300)) as r:
                r.raise_for_status()
                with path.open("wb") as f:
                    for block in r.iter_content(8 << 20):
                        f.write(block)
            res: dict = {"bytes": path.stat().st_size, "download_s": round(time.monotonic() - t, 1)}
            try:
                res["header"] = pbf_header(path)
            except Exception as exc:
                res["header_error"] = repr(exc)[:300]
            p = str(path)
            res["describe"] = [r[:2] for r in con.sql(f"describe select * from st_readosm('{p}')").fetchall()]
            t = time.monotonic()
            res["kinds"] = con.sql(f"""select kind, count(*), count(*) filter (where cardinality(tags) > 0),
                count(*) filter (where tags is null) from st_readosm('{p}') group by all order by 1""").fetchall()
            res["kinds_s"] = round(time.monotonic() - t, 1)
            keys = ", ".join(f"'{k}'" for k in KEYS)
            t = time.monotonic()
            con.execute(f"""create or replace table c as select kind, e.key as tag_key, e.value as tag_value, count(*) as n
                from (select kind, unnest(map_entries(tags)) as e from st_readosm('{p}') where cardinality(tags) > 0)
                where e.key in ({keys}) group by all""")
            res["counts_s"] = round(time.monotonic() - t, 1)
            res["maxrss_mb"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
            odd = "not regexp_full_match(tag_value, '[a-z0-9_:;.\\-]{1,64}')"
            res["per_key"] = con.sql(f"""select tag_key, count(*) values_, sum(n), count(*) filter (where {odd}) odd_values,
                sum(n) filter (where {odd}) odd_elements from c group by 1 order by 1""").fetchall()
            res["odd_sample"] = con.sql(f"select tag_key, tag_value, sum(n) from c where {odd} group by all order by 3 desc limit 25").fetchall()
            res["top"] = con.sql("""select tag_key || '=' || tag_value, sum(n) s, string_agg(kind || ':' || n, ' ') from c
                group by tag_key, tag_value order by s desc limit 60""").fetchall()
            res["sel"] = con.sql("""select tag_key || '=' || tag_value, sum(n) from c where tag_value in ('school','kindergarten','pharmacy',
                'hospital','charging_station','fuel','supermarket','convenience','bus_stop','station','hotel','post_office','library','townhall',
                'police','fire_station','restaurant','cafe','atm','bank','playground','park','motorway','primary','residential','cycleway','no',
                'doctors','dentist','clinic','platform','stop_position','yes')
                group by 1 order by 1""").fetchall()
            print(json.dumps({name: res}, default=str, ensure_ascii=False)[:30000], flush=True)
            annotate("notice", f"osm probe {name}", {k: v for k, v in res.items() if k not in ("top", "odd_sample", "sel")})
            annotate("notice", f"osm probe {name} values", {"sel": res["sel"], "odd_sample": res["odd_sample"][:15]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
