"""Read-only route probe for PLG-025 Senate (#482), 26 Nov 2026. Writes nothing to R2.

1. www.senat.gov.pl: GET /robots.txt, and only if that answers 200, one page (/prace/posiedzenia/). Same user agent
   as the extractor; a 401/403/429 is printed with the server header and the first words and never retried.
2. Alternatives for the same data, one GET each, at least 2 s apart: dane.gov.pl search and institutions for the
   Senate; the Sejm API (term 10) processes, to see whether a process lists Senate stages and Senate votes.
"""

from __future__ import annotations

import json
import time

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
S = requests.Session()
S.headers.update({"User-Agent": UA, "Accept-Language": "pl"})
LAST = [0.0]


def get(url: str) -> requests.Response | None:
    wait = 2.0 - (time.monotonic() - LAST[0])
    if wait > 0:
        time.sleep(wait)
    try:
        r = S.get(url, timeout=(15, 60))
    except requests.RequestException as exc:
        print(f"ERROR {url} {type(exc).__name__}: {exc}", flush=True)
        return None
    finally:
        LAST[0] = time.monotonic()
    head = r.content[:300].decode("utf-8", "replace").replace("\n", " ")
    print(f"GET {r.status_code} {len(r.content)}B {url} server={r.headers.get('Server')!r} "
          f"ct={r.headers.get('Content-Type')!r} cf-ray={r.headers.get('CF-RAY')!r} head={head[:200]!r}", flush=True)
    return r


def main() -> int:
    print("== www.senat.gov.pl", flush=True)
    robots = get("https://www.senat.gov.pl/robots.txt")
    if robots is not None and robots.status_code == 200:
        print("ROBOTS\n" + robots.text, flush=True)
        page = get("https://www.senat.gov.pl/prace/posiedzenia/")
        if page is not None and page.status_code == 200:
            print(f"SITTINGS przebieg-links={page.text.count('przebieg,')}", flush=True)
    else:
        print("www.senat.gov.pl refused or unreachable: not retried, no other page read", flush=True)

    print("== dane.gov.pl", flush=True)
    for url in ("https://api.dane.gov.pl/1.4/search?q=senat&per_page=25",
                "https://api.dane.gov.pl/1.4/institutions?q=senat&per_page=25",
                "https://api.dane.gov.pl/1.4/datasets?q=senat%20g%C5%82osowania&per_page=25"):
        r = get(url)
        if r is None or r.status_code != 200:
            continue
        try:
            data = r.json().get("data", [])
        except ValueError:
            continue
        for item in data:
            a = item.get("attributes", {})
            inst = (a.get("institution") or {}).get("title") if isinstance(a.get("institution"), dict) else ""
            print(f"  {item.get('type')} {item.get('id')} | {(a.get('title') or '')[:120]} | {inst} "
                  f"| modified={a.get('modified')}", flush=True)

    print("== api.sejm.gov.pl", flush=True)
    r = get("https://api.sejm.gov.pl/sejm/term10/processes?limit=5&sort_by=-processStartDate&documentType=projekt%20ustawy")
    nums = []
    if r is not None and r.status_code == 200:
        try:
            nums = [p.get("number") for p in r.json()]
        except ValueError:
            nums = []
    r = get("https://api.sejm.gov.pl/sejm/term10/processes?limit=200&passed=true")
    if r is not None and r.status_code == 200:
        try:
            done = [p.get("number") for p in r.json()][:2]
            nums = done + nums[:1]
        except ValueError:
            pass
    for n in nums[:3]:
        d = get(f"https://api.sejm.gov.pl/sejm/term10/processes/{n}")
        if d is None or d.status_code != 200:
            continue

        def walk(stages, depth=0):
            for st in stages or []:
                keys = sorted(k for k in st if k not in ("children",))
                name = st.get("stageName", "")
                if "senat" in json.dumps(st, ensure_ascii=False).lower() or depth == 0:
                    print(f"  {'  ' * depth}{n}: {name} | {st.get('date')} | keys={keys} "
                          f"| voting={json.dumps(st.get('voting'), ensure_ascii=False)[:200]}", flush=True)
                walk(st.get("children"), depth + 1)

        walk(d.json().get("stages"))
    r = get("https://api.sejm.gov.pl/sejm/term10/votings")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
