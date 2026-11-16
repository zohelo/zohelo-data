"""Read-only probe 3 of the PKW election result files (register PLG-029, #486). Deleted before merge.

Probe 2 (run 43807885985) showed that pe2024 and prezydent2025 link ``/data/csv/<name>_csv.<ts>.zip`` where ``ts``
comes from ``api.getTs("csv/<name>_csv.zip", config)``; the plain ``<name>_csv.zip`` answers the HTML shell.
This probe prints the bundle code of getTs, getCached and the configuration load, fetches the data resources the
bundle names (at most 6 per election) and prints, for each target file name found in them, the bytes that follow
(the timestamp). With ``name=<election>:<file>`` arguments it fetches and profiles those exact files.
GET only, one request at a time, 3 s apart, at most 40 requests. Writes nothing to R2.
"""

from __future__ import annotations

import csv
import io
import re
import sys
import time
import zipfile

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
ELECTIONS = ["pe2024", "prezydent2025"]
BUDGET = [40]
NUMBER = re.compile(r"^-?\d+([.,]\d+)?$")


def get(url: str) -> tuple[int | None, dict, bytes]:
    if BUDGET[0] <= 0:
        return None, {}, b""
    BUDGET[0] -= 1
    time.sleep(3.0)
    try:
        r = requests.get(url, headers={"User-Agent": UA, "Accept-Encoding": "identity"}, timeout=(30, 180))
    except requests.RequestException as exc:
        print(f"--- {url}: {type(exc).__name__}", flush=True)
        return None, {}, b""
    keep = {k: v[:90] for k, v in r.headers.items() if k.lower() in ("content-type", "last-modified", "content-length")}
    print(f"=== GET {url} -> {r.status_code} {len(r.content)} B {keep}", flush=True)
    return r.status_code, dict(r.headers), r.content


def show(code: str, needle: str, before: int, after: int, limit: int = 3) -> None:
    for m in list(re.finditer(re.escape(needle), code))[:limit]:
        text = code[max(0, m.start() - before) : m.start() + after]
        for i in range(0, len(text), 900):
            print(f"... {needle}@{m.start()}[{i}]: {text[i:i + 900]!r}", flush=True)


def explore(election: str) -> None:
    host = f"{election}.pkw.gov.pl"
    _, _, page = get(f"https://{host}/{election}/pl/dane_w_arkuszach")
    scripts = re.findall(r'src="([^"]+/j/[^"]+\.min\.js)"', page.decode("utf-8", "replace"))
    _, _, js = get(f"https://{host}{scripts[0]}")
    code = js.decode("utf-8", "replace")
    for needle in ("getTs=function", "getCached=function", "Konfiguracja.decode", "BASE_URL,\"/data/"):
        show(code, needle, 300, 1500)
    literals = sorted(set(re.findall(r'"((?:/?data/)?[a-z_]*(?:konfig|config|pliki|ts|wersj)[a-z_./]*)"', code)))
    print(f"### {election} candidate literals {literals[:40]}", flush=True)
    names = [n for _, n in re.findall(r'\[\[((?:"[A-Z0-9_]+",?)+)\],"([A-Za-z0-9_\-]+)"\]', code)]
    tried = 0
    for lit in literals:
        if tried >= 6 or len(lit) < 4:
            continue
        path = lit.lstrip("/")
        url = f"https://{host}/{election}/{path if path.startswith('data/') else 'data/' + path}"
        tried += 1
        status, headers, body = get(url)
        if status != 200 or b"<html" in body[:500].lower():
            continue
        print(f"--- {url}: {len(body)} B, head {body[:80]!r}", flush=True)
        for name in names:
            for m in list(re.finditer(re.escape(f"{name}_csv.zip".encode()), body))[:1]:
                print(f"    {name}: {body[m.end():m.end() + 40]!r}", flush=True)


def profile(election: str, file: str) -> None:
    url = f"https://{election}.pkw.gov.pl/{election}/data/csv/{file}"
    status, headers, body = get(url)
    if body[:4] != b"PK\x03\x04":
        print(f"--- not a ZIP: {headers.get('Content-Type')}", flush=True)
        return
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        for info in z.infolist():
            print(f"    member {info.filename!r} {info.file_size} B", flush=True)
            if not info.filename.lower().endswith(".csv"):
                continue
            raw = z.read(info)
            try:
                text, enc = raw.decode("utf-8-sig"), "utf-8"
            except UnicodeDecodeError:
                text, enc = raw.decode("cp1250"), "cp1250"
            rows = list(csv.reader(io.StringIO(text), delimiter=";"))
            print(f"    encoding {enc}, bom {raw[:3] == b'\xef\xbb\xbf'}, rows {len(rows)}, "
                  f"widths {sorted({len(r) for r in rows})}", flush=True)
            for row in rows[1:3] + rows[-2:]:
                print(f"    row: {row}"[:900], flush=True)
            for i, col in enumerate(rows[0]):
                values = [r[i] for r in rows[1:] if len(r) > i]
                other = sorted({v for v in values if v.strip() and not NUMBER.match(v.strip())})
                print(f"    h{i}: {col!r} empty {sum(1 for v in values if not v.strip())} lengths "
                      f"{sorted({len(v) for v in values})[:8]} other {len(other)}: {other[:5]}"[:500], flush=True)


def main(argv: list[str]) -> int:
    files = [a[5:].split(":", 1) for a in argv if a.startswith("name=")]
    if not files:
        for election in ELECTIONS:
            explore(election)
    for election, file in files:
        profile(election, file)
    print(f"### requests left {BUDGET[0]}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
