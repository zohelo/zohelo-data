"""Read-only reachability and layout probe of the Sejm API and the ELI API (PLG-024 #481, PLG-026 #483).

GET only, one request at a time, three seconds apart, at most 40 requests. robots.txt first: a disallow of
/sejm/ or /eli/ stops the probe. A rejection page on /robots.txt alone (the API host may not serve the file) is
recorded and the documented API calls go on; a refusal of an API call (401, 403, a "Request Rejected" page)
stops the probe and is never retried. A plain 503 or a timeout gets one retry after 60 seconds. Prints status,
content type, size and the first bytes or the JSON shape of each answer to the job log. Writes nothing.
"""

import json
import time
import urllib.error
import urllib.request
import urllib.robotparser

UA = "zohelo-data/1.0 (+https://zohelo.com; open-data research)"
BASE = "https://api.sejm.gov.pl"
BUDGET = [40]
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
    if status in (None, 502, 503, 504) and not refused(status, body) and BUDGET[0] > 0:
        print(f"--- {url}: {status} {body[:200]!r}; one retry in 60 s", flush=True)
        BUDGET[0] -= 1
        time.sleep(60)
        t0 = time.time()
        status, hdr, body = fetch(url, accept)
    ms = int((time.time() - t0) * 1000)
    keep = {k: v[:100] for k, v in hdr.items() if k.lower() in ("content-type", "last-modified", "etag", "retry-after", "content-length", "server", "cache-control", "x-total-count") or "ratelimit" in k.lower()}
    print(f"\n=== GET {url}\n--- status {status} | ctype {hdr.get('Content-Type', hdr.get('content-type'))} | {len(body)} bytes | {ms} ms | {keep}", flush=True)
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
        print("--- ROBOTS_REFUSED: /robots.txt answered with a refusal page; recorded, going on with the documented API")
        RP.parse([])
        return True
    if status == 200 and "<html" not in text[:400].lower():
        RP.parse(text.splitlines())
    else:
        RP.parse([])  # no robots.txt (404): everything allowed
    for path in ("/sejm/term10", "/eli/acts", "/eli/acts/DU/2025"):
        print(f"--- robots allows {path}: {RP.can_fetch(UA, BASE + path)}")
    return all(RP.can_fetch(UA, BASE + p) for p in ("/sejm/term10", "/eli/acts/DU/2025"))


def keys_only(doc, label):
    """Personal-data-safe summary: count, key union and the counts of a few closed fields; no values."""
    if not isinstance(doc, list):
        return
    keys = set()
    for d in doc:
        if isinstance(d, dict):
            keys |= set(d)
    print(f"--- {label}: items {len(doc)}; union of keys: {sorted(keys)}")
    for f in ("club", "active", "voivodeship", "districtNum"):
        vals = {}
        for d in doc:
            if isinstance(d, dict):
                vals[str(d.get(f))] = vals.get(str(d.get(f)), 0) + 1
        print(f"--- {label} {f}: {len(vals)} values; " + json.dumps(dict(sorted(vals.items())[:60]), ensure_ascii=False)[:900])


def eli_year(pub, year, head=600):
    d = show(f"{BASE}/eli/acts/{pub}/{year}", 300)
    if isinstance(d, dict):
        items = d.get("items") or []
        print(f"--- {pub} {year}: count={d.get('count')} totalCount={d.get('totalCount')} offset={d.get('offset')} items={len(items)}")
        first_last(items, head)
        for f in ("type", "status", "inForce"):
            vals = {}
            for it in items:
                vals[str(it.get(f))] = vals.get(str(it.get(f)), 0) + 1
            print(f"--- {pub} {year} {f}: " + json.dumps(vals, ensure_ascii=False)[:900])


def main():
    if not robots():
        print("STOP after robots.txt")
        return
    show(f"{BASE}/sejm/term", 1500)
    show(f"{BASE}/sejm/term10", 3000)
    d = show(f"{BASE}/sejm/term10/proceedings", 300)
    first_last(d, 700)
    d = show(f"{BASE}/sejm/term10/votings", 300)
    first_last(d, 400)
    d = show(f"{BASE}/sejm/term10/votings/1", 300)
    first_last(d, 1500)
    d = show(f"{BASE}/sejm/term10/votings/1/1", 200)
    if isinstance(d, dict):
        print(f"--- voting detail keys: {sorted(d)}; votes list {len(d.get('votes') or [])}")
    if STOP[0]:
        print("STOP: refused")
        return
    d = show(f"{BASE}/sejm/term10/prints", 300)
    first_last(d, 1200)
    if isinstance(d, list):
        print("--- prints documentDate range: " + str(min((x.get("documentDate") or "~") for x in d)) + " .. " + str(max((x.get("documentDate") or "") for x in d)))
    show(f"{BASE}/sejm/term10/prints?limit=5&offset=0", 300)
    _s, _h, body = get(f"{BASE}/sejm/term10/MP")
    try:
        keys_only(json.loads(body), "MP term10")
    except ValueError:
        print("--- MP: not JSON")
    d = show(f"{BASE}/sejm/term10/clubs", 300)
    first_last(d, 500)
    d = show(f"{BASE}/sejm/term9/votings", 200)
    first_last(d, 300)
    d = show(f"{BASE}/sejm/term9/prints?limit=1", 200)
    if STOP[0]:
        print("STOP: refused")
        return

    show(f"{BASE}/eli/acts", 2000)
    show(f"{BASE}/eli/acts/DU", 3000)
    show(f"{BASE}/eli/acts/MP", 3000)
    eli_year("DU", 2025, 1500)
    eli_year("MP", 2025, 900)
    eli_year("DU", 2026, 300)
    eli_year("DU", 1918, 600)
    eli_year("MP", 1918, 600)
    eli_year("DU", 1990, 300)
    show(f"{BASE}/eli/acts/DU/2025?limit=10&offset=5", 300)
    d = show(f"{BASE}/eli/acts/DU/2025/1", 300)
    if isinstance(d, dict):
        print(f"--- act detail keys: {sorted(d)}")
    show(f"{BASE}/eli/changes/acts?since=2026-11-10", 800)
    show(f"{BASE}/eli/statuses", 1500)
    show(f"{BASE}/eli/types", 2500)
    print("\nrequests left", BUDGET[0])


if __name__ == "__main__":
    main()
