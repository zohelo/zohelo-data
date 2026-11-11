"""Read-only probe (FAOSTAT, WHO data.who.int, UNESCO UIS, ITU DataHub): fetch URLs from a runner, print findings.

Directive lines in dev/fao_probe.txt: id<TAB>mode<TAB>url<TAB>arg. Modes: head, raw (first N chars), grep (regex
contexts in the body), links (anchor hrefs matching regex), jsgrep (grep regex in the page and its scripts),
zip (download, list members, print the first lines of the first CSV member), json (print top-level shape).
Nothing is written anywhere.
"""

import concurrent.futures as cf
import io
import json
import re
import ssl
import sys
import urllib.parse
import urllib.request
import zipfile

UA = "zohelo-data/1.0 (+https://zohelo.com; open-data research probe)"


def get(url, n=3_000_000, method="GET"):
    req = urllib.request.Request(url, method=method, headers={"User-Agent": UA, "Accept": "*/*"})
    r = urllib.request.urlopen(req, timeout=60, context=ssl.create_default_context())
    raw = r.read(n) if method == "GET" else b""
    return r, raw


def one(line):
    parts = line.split("\t")
    sid, mode, url = parts[0], parts[1], parts[2]
    arg = parts[3] if len(parts) > 3 else ""
    out = [f"=== {sid} {mode} {url}"]
    try:
        r, raw = get(url, 400_000_000 if mode == "zip" else 6_000_000, "HEAD" if mode == "head" else "GET")
        h = r.headers
        out.append(
            f"status={r.status} final={r.geturl()} ctype={h.get('content-type')} len={h.get('content-length')}"
            f" lastmod={h.get('last-modified')} etag={h.get('etag')} read={len(raw)}"
        )
        txt = raw.decode("utf-8", "replace")
        if mode == "raw":
            out.append(txt[: int(arg or 1500)])
        elif mode == "grep":
            n, last = 0, -9999
            for m in re.finditer(arg, txt, re.I):
                if m.start() - last < 400:
                    continue
                last = m.start()
                out.append("  >> " + re.sub(r"\s+", " ", txt[max(0, m.start() - 250) : m.start() + 600]))
                n += 1
                if n >= 12:
                    break
            if not n:
                out.append("  (no match)")
        elif mode == "links":
            seen = set()
            for m in re.finditer(r'(?is)<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', txt):
                href, label = m.group(1), re.sub(r"(?s)<[^>]+>|\s+", " ", m.group(2)).strip()
                if re.search(arg, href + " " + label, re.I) and href not in seen:
                    seen.add(href)
                    out.append(f"  {label[:80]} -> {href}")
                    if len(seen) >= 60:
                        break
        elif mode == "jsgrep":
            found = set()
            for src in re.findall(r'<script[^>]+src=["\']([^"\']+)["\']', txt)[:25]:
                full = urllib.parse.urljoin(r.geturl(), src)
                try:
                    _, js = get(full, 15_000_000)
                except Exception as exc:  # noqa: BLE001
                    out.append(f"  script {full}: {type(exc).__name__}")
                    continue
                for m in re.finditer(arg, js.decode("utf-8", "replace"), re.I):
                    found.add(m.group(0)[:200])
            for m in re.finditer(arg, txt, re.I):
                found.add(m.group(0)[:200])
            out.extend("  " + f for f in sorted(found)[:80])
            if not found:
                out.append("  (no match)")
        elif mode == "zip":
            z = zipfile.ZipFile(io.BytesIO(raw))
            for i in z.infolist()[:12]:
                out.append(f"  member {i.filename} {i.file_size} bytes")
            csvs = [i for i in z.infolist() if i.filename.lower().endswith(".csv")]
            if csvs:
                with z.open(csvs[0]) as f:
                    head = f.read(int(arg or 1200)).decode("utf-8", "replace")
                out.append(head)
                with z.open(csvs[0]) as f:
                    lines = sum(1 for _ in f)
                out.append(f"  lines in {csvs[0].filename}: {lines}")
                if len(sys.argv) > 1 and "pl" in sys.argv[1:]:
                    with z.open(csvs[0]) as f:
                        pl = [ln.decode("utf-8", "replace") for ln in f if b'"Poland"' in ln or b",Poland," in ln]
                    out.append(f"  Poland lines: {len(pl)}; " + " | ".join(p.strip()[:160] for p in pl[-3:]))
        elif mode == "json":
            data = json.loads(txt)
            if isinstance(data, list):
                out.append(f"  list of {len(data)}; first: {json.dumps(data[:2])[:1500]}")
            else:
                out.append(f"  keys {list(data)[:30]}; {json.dumps(data)[: int(arg or 1500)]}")
    except Exception as exc:  # noqa: BLE001
        out.append(f"ERROR {type(exc).__name__}: {str(exc)[:300]}")
    return "\n".join(out)


if __name__ == "__main__":
    lines = [ln.rstrip("\n") for ln in open("dev/fao_probe.txt", encoding="utf-8") if ln.count("\t") >= 2]
    with cf.ThreadPoolExecutor(6) as ex:
        for res in ex.map(one, lines):
            print(res, flush=True)
            first = res.splitlines()
            print(f"::notice title={first[0][4:60]}::{' / '.join(first[1:4])[:900]}", flush=True)
