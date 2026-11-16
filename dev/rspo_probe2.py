"""Read-only probe 2 of the RSPO public search (rspo.gov.pl, PLS-021, Epic #445): what the Angular bundle calls.

GET only, one request at a time, one second apart, at most 40 requests; no login, no retry around 401/403.
Reports from the public JavaScript bundles: every absolute URL, every string that looks like an API path, and
the code around export/download words (CSV, XLSX, eksport, pobierz), plus runtime config files under assets/.
No register record is fetched or reported.
"""

from __future__ import annotations

import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

UA = "zohelo-data/1.0 (+https://zohelo.com)"
BUDGET = [40]
ROOT = "https://rspo.gov.pl/"


def note(section: str, item) -> None:
    print(f"[{section}] {json.dumps(item, ensure_ascii=False, default=str)[:30000]}", flush=True)


def fetch(url: str, limit: int = 10_000_000, accept: str = "*/*"):
    if BUDGET[0] <= 0:
        return None, {"status": "budget used"}
    BUDGET[0] -= 1
    time.sleep(1.0)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": accept})
        with urllib.request.urlopen(req, timeout=60) as r:
            body = r.read(limit)
            return body, {"status": r.status, "type": r.headers.get("content-type"), "bytes": len(body)}
    except urllib.error.HTTPError as e:
        return None, {"status": e.code}
    except Exception as e:  # noqa: BLE001
        return None, {"status": f"{type(e).__name__}: {str(e)[:120]}"}


def main(argv: list[str]) -> int:
    body, info = fetch(ROOT, accept="text/html")
    html = (body or b"").decode("utf-8", "replace")
    scripts = re.findall(r"""<script[^>]+src=["']([^"']+)["']""", html)
    note("index", {**info, "scripts": scripts, "links": re.findall(r"""<link[^>]+href=["']([^"']+)["']""", html)[:20]})
    queue = [urllib.parse.urljoin(ROOT, s) for s in scripts]
    seen: set[str] = set()
    while queue and len(seen) < 25:
        js = queue.pop(0)
        if js in seen or "rspo.gov.pl" not in js:
            continue
        seen.add(js)
        jb, ji = fetch(js)
        src = (jb or b"").decode("utf-8", "replace")
        # lazy-loaded Angular chunks: "<n>.<hash>.js" names in the runtime
        for m in re.finditer(r"""["']([0-9a-z_\-]+\.[0-9a-f]{16}\.js)["']""", src):
            queue.append(urllib.parse.urljoin(ROOT, m.group(1)))
        for m in re.finditer(r"""(\d+):["']([0-9a-f]{16})["']""", src):
            queue.append(urllib.parse.urljoin(ROOT, f"{m.group(1)}.{m.group(2)}.js"))
        urls = sorted(set(re.findall(r"""https?://[A-Za-z0-9.\-]+(?:/[^"'`\s<>)]*)?""", src)))
        urls = [u for u in urls if not re.search(r"(w3\.org|angular\.io|github\.com|mozilla|unicode\.org|ietf)", u)]
        paths = sorted(
            {m.group(1) for m in re.finditer(r"""["'`](/?(?:api|rest|public|v\d)/[A-Za-z0-9_\-./{}$]*)["'`]""", src)}
        )
        words = sorted({m.group(1) for m in re.finditer(r"""["'`]([A-Za-z0-9_\-./]*(?:export|eksport|csv|xlsx|xls|download|pobierz|plik)[A-Za-z0-9_\-./]*)["'`]""", src, re.I)})
        ctx = []
        for m in re.finditer(r"(eksport|export|csv|xlsx|pobierz|Pobierz|download|responseType|apiUrl|baseUrl|environment)", src):
            ctx.append(src[max(0, m.start() - 220): m.end() + 220].replace("\n", " "))
            if len(ctx) >= 25:
                break
        note("js", {"url": js, **ji, "urls": urls[:80], "paths": paths[:80], "words": words[:80]})
        for c in ctx:
            note("ctx", {"file": js.rsplit("/", 1)[-1], "text": c})
    for extra in ("assets/config.json", "assets/env.js", "assets/environment.json", "assets/config/config.json",
                  "assets/app-config.json", "config.json"):
        b, i = fetch(urllib.parse.urljoin(ROOT, extra), limit=20000)
        head = (b or b"").decode("utf-8", "replace")[:600]
        note("config", {"url": extra, **i, "head": re.sub(r"\s+", " ", head) if not head.lstrip().startswith("<!DOCTYPE") else "(SPA index)"})
    # dane.gov.pl timed out from the runner in probe 1 (30 s): once more with a longer wait, metadata only
    for u in ("https://api.dane.gov.pl/1.4/datasets?q=RSPO&per_page=10", "https://dane.gov.pl/pl/dataset?q=RSPO"):
        try:
            time.sleep(1.0)
            req = urllib.request.Request(u, headers={"User-Agent": UA, "Accept": "application/json, text/html"})
            with urllib.request.urlopen(req, timeout=150) as r:
                b = r.read(400_000)
            titles = re.findall(r'"title":\s*"([^"]{0,140})"', b.decode("utf-8", "replace"))[:20]
            note("dane", {"url": u, "status": 200, "bytes": len(b), "titles": titles})
        except Exception as e:  # noqa: BLE001
            note("dane", {"url": u, "status": f"{type(e).__name__}: {str(e)[:120]}"})
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
