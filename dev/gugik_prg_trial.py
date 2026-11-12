"""Dev trial (PL-ENV-007, #2870): PRG address points as entity `punkty_adresowe` of gugik_prg.

Mode `probe` (read-only, no writes): HEAD of the national SHP package, then the ZIP read through HTTP Range requests
(central directory, the head of each .dbf/.prj/.cpg/.shp member) to list the members, the DBF field list and
record counts, and a few decoded records, without downloading the 564 MB file. One annotation per section.
"""

from __future__ import annotations

import io
import json
import struct
import sys
import zipfile

import requests

from ingestion.common.report import annotate

URL = "https://opendata.geoportal.gov.pl/prg/adresy/PunktyAdresowe/POLSKA.zip"
UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"


class RangeFile(io.RawIOBase):
    """A read-only seekable view of a remote file through HTTP Range requests (256 kB read-ahead)."""

    def __init__(self, url: str, size: int) -> None:
        self.url, self.size, self.pos, self.requests = url, size, 0, 0
        self.buf_start, self.buf = 0, b""

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.pos

    def seek(self, offset: int, whence: int = 0) -> int:
        self.pos = {0: offset, 1: self.pos + offset, 2: self.size + offset}[whence]
        return self.pos

    def read(self, n: int = -1) -> bytes:
        if n is None or n < 0:
            n = self.size - self.pos
        n = min(n, self.size - self.pos)
        if n <= 0:
            return b""
        end = self.pos + n
        if not (self.buf_start <= self.pos and end <= self.buf_start + len(self.buf)):
            fetch_end = min(self.size, max(end, self.pos + (256 << 10))) - 1
            if self.requests >= 200:
                raise RuntimeError("more than 200 range requests")
            r = requests.get(self.url, headers={"User-Agent": UA, "Range": f"bytes={self.pos}-{fetch_end}"},
                             timeout=(30, 120))
            self.requests += 1
            if r.status_code != 206:
                raise RuntimeError(f"range request answered {r.status_code}")
            self.buf_start, self.buf = self.pos, r.content
        out = self.buf[self.pos - self.buf_start: end - self.buf_start]
        self.pos += len(out)
        return out

    def readinto(self, b) -> int:
        data = self.read(len(b))
        b[: len(data)] = data
        return len(data)


def dbf_head(data: bytes, encoding: str) -> dict:
    records, header_len, record_len = struct.unpack("<IHH", data[4:12])
    fields, pos = [], 32
    while data[pos] != 0x0D:
        name = data[pos: pos + 11].split(b"\0")[0].decode("ascii", "replace")
        fields.append([name, chr(data[pos + 11]), data[pos + 16], data[pos + 17]])
        pos += 32
    rows = []
    for i in range(4):
        start = header_len + i * record_len
        rec = data[start: start + record_len]
        if len(rec) < record_len:
            break
        offset, row = 1, {}
        for name, _kind, length, _dec in fields:
            row[name] = rec[offset: offset + length].decode(encoding, "replace").strip()
            offset += length
        rows.append(row)
    return {"records": records, "header_len": header_len, "record_len": record_len, "fields": fields, "rows": rows}


def probe() -> None:
    head = requests.head(URL, headers={"User-Agent": UA, "Accept-Encoding": "identity"}, timeout=60,
                         allow_redirects=True)
    keep = ("Content-Length", "Last-Modified", "ETag", "Accept-Ranges", "Content-Type", "Cache-Control")
    headers = {k: head.headers.get(k) for k in keep}
    annotate("notice", "prg addresses HEAD", {"status": head.status_code, "url": head.url, **headers})
    size = int(head.headers["Content-Length"])
    remote = RangeFile(head.url, size)
    archive = zipfile.ZipFile(io.BufferedReader(remote, buffer_size=1 << 16))
    members = [[i.filename, i.file_size, i.compress_size, i.compress_type] for i in archive.infolist()]
    annotate("notice", "prg addresses ZIP members", {"count": len(members), "members": members[:60]})
    texts = {}
    for info in archive.infolist():
        low = info.filename.lower()
        if low.endswith((".prj", ".cpg", ".txt", ".xml", ".pdf")) and info.file_size < 20000:
            texts[info.filename] = archive.read(info).decode("utf-8", "replace")[:1500]
    annotate("notice", "prg addresses side files", texts)
    encoding = "utf-8"
    for name, text in texts.items():
        if name.lower().endswith(".cpg"):
            encoding = {"1250": "cp1250", "65001": "utf-8"}.get(text.strip().upper().replace("UTF-8", "65001"),
                                                                    text.strip() or "utf-8")
    heads = {}
    for info in archive.infolist():
        if info.filename.lower().endswith(".dbf") and len(heads) < 4:
            with archive.open(info) as member:
                data = member.read(48 << 10)
            heads[info.filename] = dbf_head(data, encoding)
        if info.filename.lower().endswith(".shp") and len(heads) < 4:
            with archive.open(info) as member:
                data = member.read(100)
            heads[info.filename] = {"shape_type": struct.unpack("<i", data[32:36])[0],
                                    "bbox": struct.unpack("<4d", data[36:68]),
                                    "file_length_bytes": struct.unpack(">i", data[24:28])[0] * 2}
    for name, item in heads.items():
        annotate("notice", f"prg addresses head of {name}", {"encoding": encoding, **item})
    annotate("notice", "prg addresses probe done", {"range_requests": remote.requests})


def disk() -> str:
    import shutil

    return f"{shutil.disk_usage('/tmp').free / 1e9:.1f} GB free"


def profile() -> None:
    """Download once to the runner (no R2 writes), extract with 7z, profile the layers with DuckDB spatial."""
    import hashlib
    from pathlib import Path
    import subprocess
    import tempfile
    import time

    import duckdb

    work = Path(tempfile.mkdtemp(prefix="prg-addr-"))
    raw = work / "POLSKA.zip"
    t0 = time.monotonic()
    digest, size = hashlib.sha256(), 0
    with requests.get(URL, headers={"User-Agent": UA, "Accept-Encoding": "identity"}, stream=True,
                      timeout=(30, 300)) as r:
        r.raise_for_status()
        with raw.open("wb") as handle:
            for block in r.iter_content(8 << 20):
                handle.write(block)
                digest.update(block)
                size += len(block)
        lm, etag = r.headers.get("Last-Modified"), r.headers.get("ETag")
    t_dl = round(time.monotonic() - t0)
    listing = subprocess.run(["7z", "l", "-slt", str(raw)], capture_output=True, text=True, check=True).stdout
    methods = [line.split(" = ", 1)[1] for line in listing.splitlines() if line.startswith("Method = ")]
    t0 = time.monotonic()
    tested = subprocess.run(["7z", "t", "-bd", str(raw)], capture_output=True, text=True)
    t_test = round(time.monotonic() - t0)
    t0 = time.monotonic()
    subprocess.run(["7z", "x", "-bd", "-y", f"-o{work}/x", str(raw)], capture_output=True, text=True, check=True)
    t_x = round(time.monotonic() - t0)
    annotate("notice", "prg addresses download and 7z", {
        "bytes": size, "sha256": digest.hexdigest(), "last_modified": lm, "etag": etag, "download_s": t_dl,
        "methods": sorted(set(methods)), "7z_test_rc": tested.returncode, "7z_test_tail": tested.stdout[-300:],
        "test_s": t_test, "extract_s": t_x, "disk_after_extract": disk(),
        "files": [[p.name, p.stat().st_size] for p in sorted((work / "x").rglob("*"))]})
    heads = {}
    for dbf in sorted((work / "x").rglob("*.dbf")):
        with dbf.open("rb") as handle:
            data = handle.read(64 << 10)
        h = dbf_head(data, "utf-8")
        records, header_len, record_len = h["records"], h["header_len"], h["record_len"]
        bad_utf8 = sample_n = 0
        with dbf.open("rb") as handle:
            handle.seek(header_len)
            for _ in range(20000):
                rec = handle.read(record_len)
                if len(rec) < record_len:
                    break
                sample_n += 1
                try:
                    rec.decode("utf-8")
                except UnicodeDecodeError:
                    bad_utf8 += 1
        heads[dbf.name] = {"ldid": data[29], "records": records, "record_len": record_len, "fields": h["fields"],
                           "utf8_invalid_of_sample": [bad_utf8, sample_n], "first_rows": h["rows"][:2]}
    main_dbf = "PRG_PunktyAdresowe_POLSKA.dbf"
    annotate("notice", f"prg addresses DBF {main_dbf}", heads[main_dbf])
    annotate("notice", "prg addresses DBF of the other layers", {
        n: {"records": h["records"], "ldid": h["ldid"], "fields": [f[0] for f in h["fields"]]}
        for n, h in heads.items() if n != main_dbf})
    shp = next((work / "x").rglob("PRG_PunktyAdresowe_POLSKA.shp"))
    con = duckdb.connect()
    con.execute("SET memory_limit = '8GB'; SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory = '{work}'")
    con.execute("INSTALL spatial; LOAD spatial;")
    enc = "UTF-8" if heads["PRG_PunktyAdresowe_POLSKA.dbf"]["utf8_invalid_of_sample"][0] == 0 else "CP1250"
    t0 = time.monotonic()
    con.execute(f"CREATE TABLE p AS SELECT * EXCLUDE (geom), ST_X(geom) AS x, ST_Y(geom) AS y, "
                f"ST_GeometryType(geom)::VARCHAR AS gtype FROM ST_Read('{shp}', open_options=['ENCODING={enc}'])")
    t_read = round(time.monotonic() - t0)
    cols = [r[0] for r in con.execute("DESCRIBE p").fetchall()]
    types = {r[0]: r[1] for r in con.execute("DESCRIBE p").fetchall()}
    total = con.execute("SELECT count(*) FROM p").fetchone()[0]
    annotate("notice", "prg addresses table", {
        "rows": total, "read_s": t_read, "encoding": enc, "columns": [[c, types[c]] for c in cols],
        "gtypes": con.execute("SELECT gtype, count(*) FROM p GROUP BY 1").fetchall(),
        "xy_range": [str(v) for v in con.execute("SELECT min(x), max(x), min(y), max(y) FROM p").fetchone()],
        "memory": con.execute("SELECT sum(memory_usage_bytes) FROM duckdb_memory()").fetchone()[0]})
    prof = []
    for c in cols:
        if c in ("x", "y", "gtype"):
            continue
        q = (f'SELECT count(*) FILTER (WHERE "{c}" IS NULL), count(*) FILTER (WHERE CAST("{c}" AS VARCHAR) = \'\'), '
             f'approx_count_distinct("{c}"), min(length(CAST("{c}" AS VARCHAR))), '
             f'max(length(CAST("{c}" AS VARCHAR))) FROM p')
        nulls, empty, distinct, lmin, lmax = con.execute(q).fetchone()
        top = con.execute(f'SELECT CAST("{c}" AS VARCHAR), count(*) FROM p GROUP BY 1 ORDER BY 2 DESC LIMIT 4'
                          ).fetchall()
        prof.append([c, nulls, empty, distinct, lmin, lmax, [[str(v)[:40], n] for v, n in top]])
    chunk = []
    for item in prof:
        chunk.append(item)
        if len(json.dumps(chunk, ensure_ascii=False)) > 3600:
            annotate("notice", "prg addresses column profile", {"cols": chunk[:-1]})
            chunk = [item]
    if chunk:
        annotate("notice", "prg addresses column profile", {"cols": chunk})
    sample = con.execute("SELECT * EXCLUDE (gtype) FROM p USING SAMPLE 3 ROWS").fetchall()
    annotate("notice", "prg addresses sample rows", {"cols": cols, "rows": [[str(v)[:60] for v in r] for r in sample]})


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else "probe"
    try:
        if mode == "probe":
            probe()
        elif mode == "profile":
            profile()
        return 0
    except Exception as exc:
        annotate("error", "prg addresses probe failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
        print(json.dumps({"error": str(exc)[:500]}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
