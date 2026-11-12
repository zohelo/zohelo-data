"""Read-only reachability and layout probe of the Sejm API and the ELI API (PLG-024 #481, PLG-026 #483).

GET only, one request at a time, three seconds apart, at most 45 requests. robots.txt first: a rejection page,
an HTTP 401/403 or a disallow of /sejm/ or /eli/ stops the probe. A refusal (401, 403, a "Request Rejected"
page) stops it too and is not retried. A 503 or a timeout gets one retry after 60 seconds. Prints status,
content type, size and the first bytes or the JSON shape of each answer to the job log. Writes nothing.
"""

import json
import time
import urllib.error
import urllib.request
import urllib.robotparser

UA = "zohelo-data/1.0 (+https://zohelo.com; open-data research)"
BASE = "https://api.sejm.gov.pl"
BUDGET = [45]
RP = urllib.robotparser.RobotFileParser()
STOP = [False]


def refused(status, body):
    text = (body or b"")[:3000].decode("utf-8", "replace").lower()
    return status in (401, 403) or "request rejected" in text or "the requested url was rejected" in text


def fetch(url, accept="application/json", n=30_000_000):
    """One GET; returns (status, headers, body)."""
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": accept})
    try:
        with urllib.request.urlopen(req, timeout=90) as r:
            return r.status, dict(r.headers.items()), r.read(n)
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers.items()), e.read(4000)
    except Exception as e:  # noqa: BLE001
        return None, {}, f"{type(e).__name__}: {str(e)[:200]}".encode()


def get(url, accept="application/json"):
    if STOP[0] or BUDGET[0] <= 0:
        return None, {}, b""
    if url.startswith(BASE) and not RP.can_fetch(UA, url):
        print(f"\n=== {url}: robots.txt disallows, not requested", flush=True)
        return None, {}, b""
    BUDGET[0] -= 1
    time.sleep(3.0)
    t0 = time.time()
    status, hdr, body = fetch(url, accept)
    if status in (None, 502, 503, 504) and BUDGET[0] > 0:
        print(f"--- {url}: {status} {body[:200]!r}; one retry in 60 s", flush=True)
        BUDGET[0] -= 1
        time.sleep(60)
        t0 = time.time()
        status, hdr, body = fetch(url, accept)
    ms = int((time.time() - t0) * 1000)
    keep = {k: v[:100] for k, v in hdr.items() if k.lower() in ("content-type", "last-modified", "etag", "retry-after", "content-length", "server", "cache-control", "x-total-count") or "ratelimit" in k.lower()}
    print(f"\n=== GET {url}\n--- status {status} | {len(body)} bytes | {ms} ms | {keep}", flush=True)
    if refused(status, body):
        print("--- REFUSED: " + body[:600].decode("utf-8", "replace").replace("\n", " "), flush=True)
        STOP[0] = True
    return status, hdr, body


def shape(doc, depth=0):
    if isinstance(doc, dict):
        return {k: shape(v, depth + 1) if depth < 2 else type(v).__name__ for k, v in doc.items()}
    if isinstance(doc, list):
        return [f"list[{len(doc)}]", shape(doc[0], depth + 1) if doc and depth < 2 else None]
    return type(doc).__name__


def show(url, head=1500, accept="application/json"):
    status, _hdr, body = get(url, accept)
    if status is None:
        return None
    text = body.decode("utf-8", "replace")
    try:
        doc = json.loads(text)
    except ValueError:
        print("--- first bytes: " + text[:min(head, 800)].replace("\n", " | "))
        return None
    print("--- shape: " + json.dumps(shape(doc), ensure_ascii=False)[:1500])
    print("--- head: " + json.dumps(doc, ensure_ascii=False)[:head])
    return doc


def first_last(doc, n=600):
    if isinstance(doc, list) and doc:
        print("--- first item: " + json.dumps(doc[0], ensure_ascii=False)[:n])
        print("--- last item: " + json.dumps(doc[-1], ensure_ascii=False)[:n])
        keys = set()
        for d in doc:
            if isinstance(d, dict):
                keys |= set(d)
        print(f"--- items {len(doc)}; union of keys: {sorted(keys)}")


def robots():
    status, _hdr, body = fetch(BASE + "/robots.txt", accept="text/plain")
    text = body.decode("utf-8", "replace")
    print(f"=== robots.txt: status {status} | {len(body)} bytes\n{text[:1500]}", flush=True)
    if refused(status, body):
        print("--- robots.txt is a refusal: stop")
        return False
    if status == 200 and "<html" not in text[:400].lower():
        RP.parse(text.splitlines())
    else:
        RP.parse([])  # no robots.txt (404): everything allowed
    for path in ("/sejm/term10", "/eli/acts", "/eli/acts/DU/2025"):
        print(f"--- robots allows {path}: {RP.can_fetch(UA, BASE + path)}")
    return all(RP.can_fetch(UA, BASE + p) for p in ("/sejm/term10", "/eli/acts/DU/2025"))


def main():
    if not robots():
        print("STOP after robots.txt")
        return
    # the four calls of the brief, in order
    term = show(f"{BASE}/sejm/term10", 3000)
    acts = show(f"{BASE}/eli/acts", 3000)
    du = show(f"{BASE}/eli/acts/DU/2025", 1200)
    if isinstance(du, dict):
        items = du.get("items") or []
        print(f"--- DU 2025: count={du.get('count')} totalCount={du.get('totalCount')} offset={du.get('offset')} items={len(items)}")
        first_last(items, 900)
    if STOP[0]:
        print("STOP: refused")
        return

    # ELI layout: paging, another publisher, the oldest year, changed-since, one act's detail
    mp = show(f"{BASE}/eli/acts/MP/2025", 400)
    if isinstance(mp, dict):
        print(f"--- MP 2025: count={mp.get('count')} totalCount={mp.get('totalCount')} items={len(mp.get('items') or [])}")
        first_last(mp.get("items") or [], 700)
    old = show(f"{BASE}/eli/acts/DU/1918", 400)
    if isinstance(old, dict):
        print(f"--- DU 1918: count={old.get('count')} totalCount={old.get('totalCount')} items={len(old.get('items') or [])}")
        first_last(old.get("items") or [], 500)
    show(f"{BASE}/eli/acts/DU/2025?limit=10&offset=5", 600)
    show(f"{BASE}/eli/acts/DU/2025/1", 2500)
    show(f"{BASE}/eli/changes/acts?since=2026-11-10", 800)
    show(f"{BASE}/eli/statuses", 800)
    show(f"{BASE}/eli/types", 800)
    if STOP[0]:
        print("STOP: refused")
        return

    # Sejm layout: entity lists of term 10
    for res in ("MP", "clubs", "committees", "proceedings", "votings"):
        d = show(f"{BASE}/sejm/term10/{res}", 600)
        first_last(d, 900)
        if STOP[0]:
            print("STOP: refused")
            return
    d = show(f"{BASE}/sejm/term10/votings/1", 400)
    first_last(d, 1200)
    for res in ("prints", "processes", "interpellations"):
        d = show(f"{BASE}/sejm/term10/{res}?limit=5&offset=0", 400)
        first_last(d, 1200)
        d = show(f"{BASE}/sejm/term10/{res}?limit=500&offset=0", 200)
        first_last(d, 300)
        if STOP[0]:
            print("STOP: refused")
            return
    show(f"{BASE}/sejm/term10/prints?limit=5&offset=100000", 300)
    show(f"{BASE}/sejm/term10/processes/1", 2500)
    show(f"{BASE}/sejm/term", 1500)
    show(f"{BASE}/eli/acts/DU", 1500)
    print("\nrequests left", BUDGET[0])


if __name__ == "__main__":
    main()
