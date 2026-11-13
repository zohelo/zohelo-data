"""Read-only probe of the PKW election result files (register PLG-029, #486). Deleted before merge.

Per election site (https://<election>.pkw.gov.pl/): robots.txt first (a Disallow covering a path stops requests to
it), then the "dane w arkuszach" page and its script bundle (/<election>/j/<stamp>.min.js), printing the file names
the bundle builds (``data/csv/<name>_csv.zip``) and the code around them. Then the result files of the selection
(country, voivodeship, powiat, gmina; never polling districts or committee members) are fetched once: status,
Content-Type, Last-Modified, size, the ZIP members and, per CSV member, the encoding that decodes it, the separator,
the header and the first rows. Files are kept in DEV_OUTPUT_DIR (the run's artifact). GET only, one request at a
time, 3 s apart, at most 160 requests; a 401/403 or a rejection page stops the probe. Writes nothing to R2.
"""

from __future__ import annotations

import csv
import io
import os
from pathlib import Path
import re
import sys
import time
from urllib import robotparser
import zipfile

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
ELECTIONS = ["sejmsenat2023", "pe2024", "samorzad2024", "prezydent2025", "prezydent20200628"]
BUDGET = [200]
STOP = [False]
MODULE = [False]  # arg "module": print the bundle's CSV-name module instead of the csv/name context
OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
KEEP = re.compile(r"(wojew|powiat|gmin|kraj|polsk|okreg|zagranic|statk|kandydat)", re.I)
SKIP = re.compile(r"(obwod|komisj|sklad|czlonk|protok)", re.I)
ROBOTS: dict[str, robotparser.RobotFileParser | None] = {}


def get(url: str, n: int = 20_000_000) -> tuple[int | None, dict, bytes]:
    if STOP[0] or BUDGET[0] <= 0:
        return None, {}, b""
    host = url.split("/")[2]
    rp = ROBOTS.get(host)
    if rp is not None and not rp.can_fetch(UA, url):
        print(f"--- robots.txt disallows {url}: not requested", flush=True)
        return None, {}, b""
    BUDGET[0] -= 1
    time.sleep(3.0)
    try:
        r = requests.get(url, headers={"User-Agent": UA, "Accept-Encoding": "identity"}, timeout=(30, 120))
        body = r.content[:n]
        status, headers = r.status_code, dict(r.headers)
    except requests.RequestException as exc:
        print(f"--- {url}: {type(exc).__name__} {str(exc)[:200]}", flush=True)
        return None, {}, b""
    keep = {k: v[:90] for k, v in headers.items() if k.lower() in ("content-type", "last-modified", "etag", "content-length")}
    print(f"=== GET {url} -> {status} {len(body)} B {keep}", flush=True)
    text = body[:4000].decode("utf-8", "replace").lower()
    if status in (401, 403) or "request rejected" in text:
        print("--- REFUSED, probe stops: " + text[:300].replace("\n", " "), flush=True)
        STOP[0] = True
    return status, headers, body


def robots(host: str) -> None:
    status, headers, body = get(f"https://{host}/robots.txt", 200_000)
    text = body.decode("utf-8", "replace")
    print(f"--- robots.txt of {host}: status {status}, type {headers.get('Content-Type')}\n{text[:800]}", flush=True)
    if status == 200 and "text/plain" in (headers.get("Content-Type") or "") and "<html" not in text.lower():
        rp = robotparser.RobotFileParser()
        rp.parse(text.splitlines())
        ROBOTS[host] = rp
    else:
        ROBOTS[host] = None  # no robots.txt (404 or an HTML shell): nothing disallowed


def bundle_names(election: str) -> list[str]:
    host = f"{election}.pkw.gov.pl"
    robots(host)
    status, _, body = get(f"https://{host}/{election}/pl/dane_w_arkuszach", 2_000_000)
    html = body.decode("utf-8", "replace")
    scripts = re.findall(r'src="([^"]+\.js[^"]*)"', html)
    print(f"--- page {len(html)} chars; scripts {scripts}\n{html[:1500]}", flush=True)
    names: list[str] = []
    for src in scripts:
        url = src if src.startswith("http") else f"https://{host}{src if src.startswith('/') else '/' + election + '/' + src}"
        if "/j/" not in url and "min.js" not in url:
            continue
        status, _, js = get(url, 30_000_000)
        code = js.decode("utf-8", "replace")
        print(f"--- bundle {url}: {len(code)} chars", flush=True)
        names += re.findall(r"([A-Za-z0-9_\-]+)_csv\.zip", code)
        if MODULE[0]:  # the names are built at run time by csv_links(config) in this module: print it whole
            start = code.find('"@election/models/csv":[function')
            end = code.find('}],"@', start + 40) if start >= 0 else -1
            text = code[start:end + 3] if start >= 0 else ""
            print(f"--- module @election/models/csv: {len(text)} chars", flush=True)
            for i in range(0, min(len(text), 40_000), 900):
                print(f"... module[{i}]: {text[i:i + 900]!r}", flush=True)
            for m in list(re.finditer(r'"(?:/|\.\./)?data/[^"]{0,80}"|ostatniaAktualizacjaTs', code))[:30]:
                print(f"... data@{m.start()}: {code[max(0, m.start() - 150): m.start() + 150]!r}", flush=True)
            continue
        seen = 0
        for m in re.finditer(r"csv", code):
            if seen >= 25:
                break
            seen += 1
            print(f"... csv@{m.start()}: {code[max(0, m.start() - 300): m.start() + 200]!r}", flush=True)
        for m in list(re.finditer(r"wyniki_|frekwencja|_po_", code))[:80]:
            print(f"... name@{m.start()}: {code[max(0, m.start() - 60): m.start() + 120]!r}", flush=True)
        for m in list(re.finditer(r"(?i)licencj|prawa autorsk|ponowne wykorzyst|warunki korzyst|creative commons", code))[:10]:
            print(f"... terms@{m.start()}: {code[max(0, m.start() - 200): m.start() + 300]!r}", flush=True)
    for m in list(re.finditer(r"(?i)licencj|prawa autorsk|ponowne wykorzyst|warunki korzyst|creative commons", html))[:10]:
        print(f"... page terms@{m.start()}: {html[max(0, m.start() - 200): m.start() + 300]!r}", flush=True)
    names = sorted(set(names))
    print(f"### {election} names from the bundle ({len(names)}): {names}", flush=True)
    return names


def profile(election: str, name: str) -> None:
    url = f"https://{election}.pkw.gov.pl/{election}/data/csv/{name}_csv.zip"
    status, headers, body = get(url)
    is_zip = body[:4] == b"PK\x03\x04"
    print(f"--- {name}: status {status}, type {headers.get('Content-Type')}, zip signature {is_zip}", flush=True)
    if not is_zip:
        print(f"--- not a ZIP: {body[:200]!r}", flush=True)
        return
    (OUT / election).mkdir(parents=True, exist_ok=True)
    (OUT / election / f"{name}_csv.zip").write_bytes(body)
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        for info in z.infolist():
            raw = z.read(info)
            print(f"    member {info.filename!r} {info.file_size} B", flush=True)
            for enc in ("utf-8-sig", "cp1250", "iso-8859-2"):
                try:
                    text = raw.decode(enc)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                print("    no decoding", flush=True)
                continue
            first = text.splitlines()[0] if text else ""
            sep = max([";", ",", "\t"], key=first.count)
            rows = list(csv.reader(io.StringIO(text), delimiter=sep))
            print(f"    encoding {enc}, bom {raw[:3] == b'\xef\xbb\xbf'}, separator {sep!r}, lines {len(rows)}, "
                  f"crlf {b'\r\n' in raw[:5000]}, columns {len(rows[0]) if rows else 0}", flush=True)
            for i, col in enumerate(rows[0] if rows else []):
                print(f"    h{i}: {col!r}", flush=True)
            for row in rows[1:4] + rows[-3:]:
                print(f"    row: {row[:14]}", flush=True)
            if rows:
                code_cols = [i for i, h in enumerate(rows[0]) if re.search(r"(?i)teryt|kod", h)]
                for i in code_cols:
                    values = [r[i] for r in rows[1:] if len(r) > i]
                    lengths: dict[int, int] = {}
                    for v in values:
                        lengths[len(v)] = lengths.get(len(v), 0) + 1
                    odd = sorted({v for v in values if not v.isdigit() or len(v) not in (6, 7)})[:20]
                    print(f"    code column {rows[0][i]!r}: lengths {lengths}; non 6/7-digit {odd}", flush=True)
                tail = [r for r in rows[1:] if any(re.search(r"(?i)zagranic|statk|polska|razem|suma|ogółem", c) for c in r[:4])]
                for r in tail[:8]:
                    print(f"    special row: {r[:10]}", flush=True)


def main(argv: list[str]) -> int:
    MODULE[0] = "module" in argv
    argv = [a for a in argv if a != "module"]
    only = [a for a in argv if not a.startswith("name=")]
    extra = [a[5:] for a in argv if a.startswith("name=")]  # name=<election>:<file name>
    for election in only or ELECTIONS:
        names = bundle_names(election)
        picked = [n for n in names if KEEP.search(n) and not SKIP.search(n)]
        print(f"### {election} picked {picked}", flush=True)
        for name in picked[:24]:
            profile(election, name)
    for item in extra:
        election, name = item.split(":", 1)
        profile(election, name)
    print(f"### requests left {BUDGET[0]}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
