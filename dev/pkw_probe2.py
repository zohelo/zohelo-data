"""Read-only probe 2 of the PKW election result files (register PLG-029, #486). Deleted before merge.

Probe 1 (run 43343143051) showed that each site's bundle builds the CSV names in a module (``csv_links``) as
``[[labels], "<name>"]`` pairs, and that samorzad2024 has its own controller (``SamorzadCsvController``) linking
``/data/csv/<name>.xlsx``. This probe lists every pair of each bundle, prints the Samorzad controller whole, and
fetches the area-level result files (voivodeship, powiat, gmina, country; never polling districts, protocols or
committee members) once: status, Content-Type, Last-Modified, size, members, and a profile per CSV member (encoding,
separator, rows, header, first and last rows, per column: empty, numeric and the distinct non-numeric strings) or
per XLSX sheet (first rows). GET only, one request at a time, 3 s apart, at most 80 requests; a 401/403 or a
rejection page stops the probe. Writes nothing to R2; files go to DEV_OUTPUT_DIR (the run's artifact).
"""

from __future__ import annotations

import csv
import io
import os
from pathlib import Path
import re
import sys
import time
import zipfile

import requests

from ingestion.common import xlsx_cells

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
ELECTIONS = ["sejmsenat2023", "pe2024", "samorzad2024", "prezydent2025"]
BUDGET = [80]
STOP = [False]
OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
KEEP = re.compile(r"(wojew|powiat|gmin|kraj|miasto)", re.I)
SKIP = re.compile(r"(obwod|protok|sklad|czlonk|senat|kandydatow_po_(gminach|powiatach|wojewodztwach)_sejm)", re.I)
PAIR = re.compile(r'\[\[((?:"[A-Z0-9_]+",?)+)\],"([A-Za-z0-9_\-]+)"\]')
NUMBER = re.compile(r"^-?\d+([.,]\d+)?$")


def get(url: str) -> tuple[int | None, dict, bytes]:
    if STOP[0] or BUDGET[0] <= 0:
        return None, {}, b""
    BUDGET[0] -= 1
    time.sleep(3.0)
    try:
        r = requests.get(url, headers={"User-Agent": UA, "Accept-Encoding": "identity"}, timeout=(30, 180))
        status, headers, body = r.status_code, dict(r.headers), r.content
    except requests.RequestException as exc:
        print(f"--- {url}: {type(exc).__name__} {str(exc)[:200]}", flush=True)
        return None, {}, b""
    keep = {k: v[:90] for k, v in headers.items() if k.lower() in ("content-type", "last-modified", "etag", "content-length")}
    print(f"=== GET {url} -> {status} {len(body)} B {keep}", flush=True)
    if status in (401, 403) or b"request rejected" in body[:4000].lower():
        print("--- REFUSED, probe stops", flush=True)
        STOP[0] = True
    return status, headers, body


def bundle(election: str) -> tuple[list[str], list[str]]:
    host = f"{election}.pkw.gov.pl"
    _, _, page = get(f"https://{host}/{election}/pl/dane_w_arkuszach")
    scripts = re.findall(r'src="([^"]+/j/[^"]+\.min\.js)"', page.decode("utf-8", "replace"))
    csv_names: list[str] = []
    xlsx_names: list[str] = []
    for src in scripts:
        _, _, js = get(f"https://{host}{src}")
        code = js.decode("utf-8", "replace")
        pairs = PAIR.findall(code)
        print(f"### {election} pairs ({len(pairs)}):", flush=True)
        for labels, name in pairs:
            print(f"    {name}  <- {labels}", flush=True)
        start = code.find("SamorzadCsvController=void 0")
        if start >= 0:
            end = code.find("n.SamorzadCsvController=s", start)
            text = code[start - 300 : end + 40]
            for i in range(0, len(text), 900):
                print(f"... samorzad[{i}]: {text[i:i + 900]!r}", flush=True)
            xlsx_names = sorted(set(re.findall(r'"(\d{4}-[a-z0-9\-]+)"', text)))
            print(f"### {election} xlsx names {xlsx_names}", flush=True)
        csv_names += [name for _, name in pairs]
    return sorted(set(csv_names)), xlsx_names


def profile_csv(name: str, raw: bytes) -> None:
    for enc in ("utf-8-sig", "cp1250"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        print("    no decoding", flush=True)
        return
    first = text.splitlines()[0] if text else ""
    sep = max([";", ",", "\t"], key=first.count)
    rows = list(csv.reader(io.StringIO(text), delimiter=sep))
    widths = sorted({len(r) for r in rows})
    print(f"    encoding {enc}, bom {raw[:3] == b'\xef\xbb\xbf'}, sep {sep!r}, rows {len(rows)}, widths {widths}, "
          f"crlf {b'\r\n' in raw[:5000]}, trailing sep {first.endswith(sep)}", flush=True)
    header = rows[0]
    for row in rows[1:3] + rows[-2:]:
        print(f"    row: {row}", flush=True)
    for i, col in enumerate(header):
        values = [r[i] for r in rows[1:] if len(r) > i]
        empty = sum(1 for v in values if v.strip() == "")
        numeric = sum(1 for v in values if NUMBER.match(v.strip()))
        other = sorted({v for v in values if v.strip() and not NUMBER.match(v.strip())})
        lengths = sorted({len(v) for v in values})[:8]
        print(f"    h{i}: {col!r} empty {empty} numeric {numeric} lengths {lengths} other {len(other)}: "
              f"{other[:6] if len(other) > 25 else other}"[:600], flush=True)


def profile(election: str, name: str, kind: str) -> None:
    suffix = "_csv.zip" if kind == "csv" else ".xlsx"
    url = f"https://{election}.pkw.gov.pl/{election}/data/csv/{name}{suffix}"
    status, headers, body = get(url)
    print(f"--- {name}{suffix}: status {status}, type {headers.get('Content-Type')}, PK {body[:4] == b'PK\x03\x04'}", flush=True)
    if body[:4] != b"PK\x03\x04":
        print(f"--- not a ZIP container: {body[:120]!r}", flush=True)
        return
    (OUT / election).mkdir(parents=True, exist_ok=True)
    (OUT / election / f"{name}{suffix}").write_bytes(body)
    if kind == "xlsx":
        rows: dict[tuple[str, int], dict[int, str]] = {}
        for cell in xlsx_cells.cells(body):
            rows.setdefault((cell["sheet_name"], cell["row_number"]), {})[cell["column_number"]] = cell["value_text"]
        sheets = sorted({s for s, _ in rows})
        for sheet in sheets:
            numbers = sorted(r for s, r in rows if s == sheet)
            print(f"    sheet {sheet!r}: rows {len(numbers)} ({numbers[0]}..{numbers[-1]})", flush=True)
            for r in numbers[:8] + numbers[-3:]:
                print(f"      r{r}: {[rows[(sheet, r)].get(c) for c in sorted(rows[(sheet, r)])][:40]}"[:900], flush=True)
        return
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        for info in z.infolist():
            print(f"    member {info.filename!r} {info.file_size} B {info.date_time}", flush=True)
            if info.filename.lower().endswith(".csv"):
                profile_csv(name, z.read(info))


def main(argv: list[str]) -> int:
    for election in argv or ELECTIONS:
        csv_names, xlsx_names = bundle(election)
        picked = [n for n in csv_names if KEEP.search(n) and not SKIP.search(n)]
        print(f"### {election} picked csv {picked} xlsx {xlsx_names}", flush=True)
        for name in picked:
            profile(election, name, "csv")
        for name in xlsx_names:
            profile(election, name, "xlsx")
    print(f"### requests left {BUDGET[0]}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
