"""Read-only probe 4 of the PKW election result files (register PLG-029, #486). Deleted before merge.

Probe 3 (run 43812026752): the site's script reads ``<base>/data/config.blob`` (protobuf ``Konfiguracja``) and
builds a file link as ``data/csv/<name>_csv.<ts>.zip`` with ``ts = index.directories["csv"].ts`` when positive,
else ``index.directories["csv"].files["<name>_csv.zip"]``. This probe fetches config.blob of pe2024 and
prezydent2025, decodes the protobuf wire format without a schema, prints every map entry whose key is ``csv`` or
ends in ``_csv.zip`` with its value, derives each file's link, and fetches and profiles the area-level files.
GET only, one request at a time, 3 s apart, at most 30 requests. Writes nothing to R2.
"""

from __future__ import annotations

import sys

from dev import pkw_probe3 as p3

FILES = {
    "pe2024": ["wyniki_gl_na_listy_po_wojewodztwach", "wyniki_gl_na_listy_po_wojewodztwach_proc",
               "wyniki_gl_na_listy_po_powiatach", "wyniki_gl_na_listy_po_powiatach_proc",
               "wyniki_gl_na_listy_po_gminach", "wyniki_gl_na_listy_po_gminach_proc"],
    "prezydent2025": ["wyniki_gl_na_kandydatow_po_wojewodztwach", "wyniki_gl_na_kandydatow_po_powiatach",
                      "wyniki_gl_na_kandydatow_po_gminach", "wyniki_gl_na_kandydatow_po_wojewodztwach_w_drugiej_turze",
                      "wyniki_gl_na_kandydatow_po_powiatach_w_drugiej_turze",
                      "wyniki_gl_na_kandydatow_po_gminach_w_drugiej_turze"],
}


def varint(b: bytes, i: int) -> tuple[int, int]:
    v, s = 0, 0
    while True:
        c = b[i]
        i += 1
        v |= (c & 0x7F) << s
        s += 7
        if c < 0x80:
            return v, i


def fields(b: bytes) -> list[tuple[int, int, object]] | None:
    """(field, wire type, value) of a message, or None when the bytes are not a well-formed message."""
    out, i = [], 0
    try:
        while i < len(b):
            key, i = varint(b, i)
            f, t = key >> 3, key & 7
            if f == 0:
                return None
            if t == 0:
                v, i = varint(b, i)
            elif t == 2:
                n, i = varint(b, i)
                if i + n > len(b):
                    return None
                v, i = b[i:i + n], i + n
            elif t == 1:
                v, i = b[i:i + 8], i + 8
            elif t == 5:
                v, i = b[i:i + 4], i + 4
            else:
                return None
            out.append((f, t, v))
    except IndexError:
        return None
    return out if i == len(b) else None


def walk(b: bytes, path: str, found: list) -> None:
    fs = fields(b)
    if not fs:
        return
    if len(fs) == 2 and fs[0][:2] == (1, 2):  # a map entry: key string, value
        try:
            key = fs[0][2].decode("utf-8")
        except UnicodeDecodeError:
            key = None
        if key is not None and (key == "csv" or key.endswith("_csv.zip")):
            value = fs[1][2]
            sub = fields(value) if isinstance(value, bytes) else None
            found.append((path, key, value if not isinstance(value, bytes) else
                          [(f, t, v if t != 2 else len(v)) for f, t, v in (sub or [])]))
    for f, t, v in fs:
        if t == 2 and len(v) > 1:
            walk(v, f"{path}/{f}", found)


def main(argv: list[str]) -> int:
    for election, names in FILES.items():
        _, _, blob = p3.get(f"https://{election}.pkw.gov.pl/{election}/data/config.blob")
        found: list = []
        walk(blob, "", found)
        dir_ts, files = None, {}
        for path, key, value in found:
            print(f"    entry {path} {key!r}: {value}"[:600], flush=True)
            if key == "csv" and isinstance(value, list):
                ts = [v for f, t, v in value if t == 0]
                dir_ts = ts[0] if ts and ts[0] > 0 else None
            elif isinstance(value, int):
                files[key] = value
        print(f"### {election} csv directory ts {dir_ts}, file ts {len(files)}", flush=True)
        for name in names:
            ts = dir_ts or files.get(f"{name}_csv.zip")
            print(f"### {election} {name} -> ts {ts}", flush=True)
            if ts:
                p3.profile(election, f"{name}_csv.{ts}.zip")
    print(f"### requests left {p3.BUDGET[0]}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
