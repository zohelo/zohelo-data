"""Dev probe (PL-ENV-007, #3106): GUGiK PRG address points, national SHP package, read-only.

One HEAD (size, Last-Modified, ETag, Accept-Ranges), then ranged GETs only: the ZIP's end (central directory: members
and sizes), the small members (.prj, .cpg) and the first ~2 MB of each .dbf member, inflated in memory to read the dBASE
header (record count, field list) and the first records. The 0.56 GB package is never downloaded. Writes nothing.
"""

from __future__ import annotations

import struct
import time
import zlib

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns.bulk_file import USER_AGENT

URL = "https://opendata.geoportal.gov.pl/prg/adresy/PunktyAdresowe/POLSKA.zip"
HEADERS = {"User-Agent": USER_AGENT, "Accept-Encoding": "identity"}


def ranged(start: int, end: int) -> bytes:
    for attempt in range(3):
        try:
            r = requests.get(URL, headers={**HEADERS, "Range": f"bytes={start}-{end}"}, timeout=120)
            if r.status_code == 206:
                return r.content
            raise RuntimeError(f"HTTP {r.status_code} for a range request")
        except requests.RequestException:
            if attempt == 2:
                raise
            time.sleep(10 * (attempt + 1))
    raise RuntimeError("unreachable")


def central_directory(size: int) -> list[dict]:
    tail_start = max(0, size - 70000)
    tail = ranged(tail_start, size - 1)
    eocd = tail.rfind(b"PK\x05\x06")
    entries, cd_size, cd_offset = struct.unpack("<HII", tail[eocd + 10 : eocd + 20])
    loc = tail.rfind(b"PK\x06\x07", 0, eocd)
    if loc >= 0:  # zip64
        z64_offset = struct.unpack("<Q", tail[loc + 8 : loc + 16])[0]
        rec = ranged(z64_offset, z64_offset + 55)
        entries, cd_size, cd_offset = struct.unpack("<QQQ", rec[32:56])
    if cd_offset >= tail_start:
        cd = tail[cd_offset - tail_start : cd_offset - tail_start + cd_size]
    else:
        time.sleep(2)
        cd = ranged(cd_offset, cd_offset + cd_size - 1)
    out, pos = [], 0
    for _ in range(entries):
        (sig, _vm, _vn, _flag, method, _t, _d, _crc, csize, usize, nlen, xlen, clen, _disk, _ia, _ea, offset) = (
            struct.unpack("<IHHHHHHIIIHHHHHII", cd[pos : pos + 46])
        )
        if sig != 0x02014B50:
            raise RuntimeError("bad central directory")
        name = cd[pos + 46 : pos + 46 + nlen].decode("cp437")
        extra = cd[pos + 46 + nlen : pos + 46 + nlen + xlen]
        values = []
        x = 0
        while x + 4 <= len(extra):
            hid, hlen = struct.unpack("<HH", extra[x : x + 4])
            if hid == 1:
                values = list(struct.unpack(f"<{hlen // 8}Q", extra[x + 4 : x + 4 + (hlen // 8) * 8]))
            x += 4 + hlen
        if usize == 0xFFFFFFFF and values:
            usize = values.pop(0)
        if csize == 0xFFFFFFFF and values:
            csize = values.pop(0)
        if offset == 0xFFFFFFFF and values:
            offset = values.pop(0)
        out.append({"name": name, "method": method, "csize": csize, "usize": usize, "offset": offset})
        pos += 46 + nlen + xlen + clen
    return out


def member_start(m: dict) -> int:
    head = ranged(m["offset"], m["offset"] + 29)
    nlen, xlen = struct.unpack("<HH", head[26:30])
    return m["offset"] + 30 + nlen + xlen


def member_head(m: dict, want: int) -> bytes:
    start = member_start(m)
    raw = ranged(start, start + min(m["csize"], want) - 1)
    if m["method"] == 0:
        return raw
    return zlib.decompressobj(-15).decompress(raw)


def dbf(head: bytes, encoding: str) -> dict:
    version, yy, mm, dd, nrec, hlen, rlen = struct.unpack("<BBBBIHH", head[:12])
    fields, pos = [], 32
    while head[pos] != 0x0D:
        name = head[pos : pos + 11].split(b"\x00")[0].decode("ascii", "replace")
        fields.append([name, chr(head[pos + 11]), head[pos + 16], head[pos + 17]])
        pos += 32
    records = []
    for i in range(3):
        rec = head[hlen + i * rlen : hlen + (i + 1) * rlen]
        if len(rec) < rlen:
            break
        values, p = {}, 1
        for name, _t, length, _dec in fields:
            values[name] = rec[p : p + length].decode(encoding, "replace").strip()
            p += length
        records.append(values)
    return {"version": version, "date": f"{1900 + yy}-{mm:02d}-{dd:02d}", "records": nrec, "header_len": hlen,
            "record_len": rlen, "fields": fields, "sample": records}


def main() -> int:
    h = requests.head(URL, headers=HEADERS, timeout=60, allow_redirects=True)
    info = {k: h.headers.get(k) for k in ("Content-Length", "Last-Modified", "ETag", "Accept-Ranges", "Content-Type")}
    info["status"] = h.status_code
    size = int(h.headers["Content-Length"])
    members = central_directory(size)
    annotate("notice", "prg address package (HEAD, central directory)", {
        **info, "members": [[m["name"], m["method"], m["csize"], m["usize"]] for m in members]})
    small = {}
    for m in members:
        if m["name"].lower().endswith((".prj", ".cpg", ".xml")) and m["csize"] < 200000:
            time.sleep(2)
            small[m["name"]] = member_head(m, m["csize"]).decode("utf-8", "replace")[:600]
    annotate("notice", "prg address small members", small)
    encoding = "utf-8"
    for text in small.values():
        if "1250" in text:
            encoding = "cp1250"
    for m in [m for m in members if m["name"].lower().endswith(".dbf")][:3]:
        time.sleep(2)
        annotate("notice", f"prg address dbf {m['name']}", {**dbf(member_head(m, 2_000_000), encoding),
                                                           "encoding_used": encoding})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
