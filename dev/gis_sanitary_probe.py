"""Read-only probe for GIS "Stan sanitarny kraju" (register PLS-018, Epic #442), 26 Nov 2026.

GET only, one request at a time, 5 s between gov.pl requests, robots.txt honoured, platform User-Agent. Nothing is
written to R2. For each attachment of the report page: file name, bytes, SHA-256, page count, text-layer
characters per page, a table-title index (lines starting "Tabela"/"Tab.") and the layout text of pages whose text
names a table and inspections or samples. Printed to the job log only (no text is stored).
"""

import hashlib
import html
import io
import re
import time
import urllib.parse
import urllib.request
import urllib.robotparser

PAGE = "https://www.gov.pl/web/gis/raport---stan-sanitarny-kraju"
UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
PAUSE = 5.0
LAST = [0.0]
KEY = re.compile(r"(?i)skontrolowan|kontrol|prób|kwestionowan|nie\s?odpowiad|nieodpowiad|przekrocz")
TITLE = re.compile(r"^\s*(Tabela|Tab\.|TABELA)\s*\S*")
CAP = 260_000  # characters of layout text per edition


def get(url: str, timeout: int = 180) -> tuple[bytes, dict]:
    wait = PAUSE - (time.time() - LAST[0])
    if wait > 0:
        time.sleep(wait)
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                body = r.read()
                return body, {k.lower(): v for k, v in r.headers.items()}
        except Exception as exc:  # bounded retries with backoff
            print(f"retry {attempt + 1} {url}: {type(exc).__name__}: {str(exc)[:120]}", flush=True)
            time.sleep(10 * (attempt + 1))
        finally:
            LAST[0] = time.time()
    raise RuntimeError(f"failed: {url}")


def main() -> None:
    from pypdf import PdfReader

    rp = urllib.robotparser.RobotFileParser()
    robots, _ = get("https://www.gov.pl/robots.txt", 30)
    rp.parse(robots.decode("utf-8", "replace").splitlines())
    if not rp.can_fetch(UA, PAGE):
        print("::error::robots.txt disallows the page")
        return
    page, _ = get(PAGE)
    text = page.decode("utf-8", "replace")
    found = []
    for m in re.finditer(r'(?is)<a\b[^>]*href="(/attachment/[0-9a-f-]{36})"[^>]*>(.*?)</a>', text):
        label = re.sub(r"\s+", " ", html.unescape(re.sub(r"(?s)<[^>]+>", " ", m.group(2)))).strip()
        url = urllib.parse.urljoin(PAGE, m.group(1))
        if url not in [u for u, _ in found]:
            found.append((url, label))
    print(f"=== PAGE attachments {len(found)}", flush=True)
    for url, label in found:
        print(f"ATT {url} | {label}", flush=True)
    summary = []
    for url, label in found:
        body, head = get(url)
        sha = hashlib.sha256(body).hexdigest()
        cd = head.get("content-disposition", "")
        print(f"=== FILE {url} | {cd} | {len(body)} | {sha} | lm={head.get('last-modified')}", flush=True)
        if not body.startswith(b"%PDF-"):
            print(f"not a PDF: {body[:20]!r}")
            continue
        started = time.time()
        reader = PdfReader(io.BytesIO(body))
        plain = []
        for p in reader.pages:
            try:
                plain.append(p.extract_text() or "")
            except Exception as exc:
                plain.append(f"<<error {type(exc).__name__}>>")
        chars = [len(t.strip()) for t in plain]
        low = sum(1 for c in chars if c < 200)
        print(f"PAGES {len(plain)} chars_total={sum(chars)} pages_under_200_chars={low} secs={time.time() - started:.0f}")
        print("CHARS " + " ".join(str(c) for c in chars))
        for i, t in enumerate(plain, start=1):
            for line in t.split("\n"):
                if TITLE.match(line):
                    print(f"T p{i}: {line.strip()[:220]}")
        budget = CAP
        for i, t in enumerate(plain, start=1):
            if budget <= 0:
                print("CAP reached")
                break
            if not any(TITLE.match(line) for line in t.split("\n")) or not KEY.search(t):
                continue
            try:
                lay = reader.pages[i - 1].extract_text(extraction_mode="layout")
            except Exception as exc:
                lay = f"<<layout error {type(exc).__name__}>>"
            lines = [line.rstrip() for line in lay.split("\n") if line.strip()]
            block = "\n".join(lines)
            budget -= len(block)
            print(f"--- LAYOUT p{i}\n{block}\n--- END p{i}", flush=True)
        summary.append(f"{cd[-60:]} {len(body)}B pages={len(plain)} low={low}")
    print("::notice title=gis_sanitary probe::" + " ; ".join(summary)[:3900])


if __name__ == "__main__":
    main()
