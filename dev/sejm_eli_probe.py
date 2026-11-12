"""Read-only probe of api.sejm.gov.pl for PLG-024 (Sejm API) and PLG-026 (ELI), 12 Nov 2026.

GET only, one request at a time, 2 s between requests, a clear user agent, 40 s timeout, no retries.
robots.txt is read first and honoured; a refusal (403, a "Request Rejected" page, a 503) stops the probe
for the host: nothing is retried another way. No personal data is printed: for MPs only field names and
counts. Results are annotations (at most 10).
"""
import json
import re
import time
import urllib.error
import urllib.request
import urllib.robotparser

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://zohelo.com; open-data research, polite)"
BASE = "https://api.sejm.gov.pl"
LAST = [0.0]
STOP = {"reason": None}


def fetch(path):
    if STOP["reason"]:
        return {"skipped": STOP["reason"]}
    wait = 2.0 - (time.time() - LAST[0])
    if wait > 0:
        time.sleep(wait)
    t0 = time.time()
    try:
        req = urllib.request.Request(BASE + path, headers={"User-Agent": UA, "Accept": "application/json, */*"})
        with urllib.request.urlopen(req, timeout=40) as r:
            body = r.read()
            h = r.headers
            out = {"st": r.status, "ct": (h.get("content-type") or "")[:40], "bytes": len(body),
                   "lm": h.get("last-modified"), "etag": (h.get("etag") or "")[:30] or None,
                   "cc": h.get("cache-control"), "body": body}
    except urllib.error.HTTPError as e:
        b = e.read(3000)
        out = {"st": e.code, "ct": (e.headers.get("content-type") or "")[:40], "bytes": len(b), "body": b}
    except Exception as e:  # noqa: BLE001
        out = {"err": f"{type(e).__name__}: {str(e)[:80]}"}
    LAST[0] = time.time()
    out["sec"] = round(time.time() - t0, 1)
    txt = out.get("body", b"")[:3000].decode("utf-8", "replace")
    if out.get("st") in (403, 503) or "Request Rejected" in txt:
        STOP["reason"] = f"{path}: HTTP {out.get('st')} {'Request Rejected' if 'Request Rejected' in txt else ''}"
        out["refused_text"] = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", txt))[:200]
    return out


def jinfo(r, personal=False, sample=1):
    m = {k: r[k] for k in ("st", "ct", "bytes", "lm", "etag", "cc", "sec", "err", "skipped", "refused_text") if r.get(k)}
    if r.get("st") != 200:
        return m
    try:
        j = json.loads(r["body"])
    except Exception as e:  # noqa: BLE001
        m["json_err"] = str(e)[:60]
        return m
    if isinstance(j, list):
        m["n"] = len(j)
        if j and isinstance(j[0], dict):
            keys = sorted({k for it in j for k in it})
            m["keys"] = keys
            if not personal:
                m["sample"] = [{k: str(v)[:60] for k, v in it.items()} for it in j[:sample]]
    elif isinstance(j, dict):
        m["top"] = {k: (v if not isinstance(v, (list, dict)) else f"{type(v).__name__}[{len(v)}]") for k, v in j.items()}
        for k, v in j.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                m[f"{k}_keys"] = sorted({kk for it in v for kk in it})
                m[f"{k}_sample"] = [{kk: str(vv)[:60] for kk, vv in it.items()} for it in v[:sample]]
    return m


def legal(r):
    t = r.get("body", b"").decode("utf-8", "replace")
    p = re.sub(r"\s+", " ", re.sub(r"(?s)<[^>]+>", " ", re.sub(r"(?is)<(script|style).*?</\1>", " ", t)))
    hits = []
    for m in re.finditer(r"(?i)licen|copyright|prawa autorsk|ponowne wykorzyst|terms of use|warunki|attribution|rate limit|limit", p):
        s = p[max(0, m.start() - 80): m.end() + 160]
        if s not in hits:
            hits.append(s)
        if len(hits) >= 6:
            break
    return {k: r[k] for k in ("st", "bytes", "lm", "skipped", "err") if r.get(k)} | {"hits": hits}


def main():
    res = {}
    rb = fetch("/robots.txt")
    rtxt = rb.get("body", b"").decode("utf-8", "replace")
    res["robots"] = {k: rb[k] for k in ("st", "ct", "bytes", "sec", "err", "refused_text") if rb.get(k)} | {"text": rtxt[:600] if rb.get("st") == 200 else None}
    rp = urllib.robotparser.RobotFileParser()
    rp.parse(rtxt.splitlines() if rb.get("st") == 200 and "<html" not in rtxt[:300].lower() else [])
    if rb.get("st") in (403, 503) or "Request Rejected" in rtxt:
        annotate("warning", "PLG-024/026 probe: robots.txt refused", res)
        return
    sejm = [("term10", "/sejm/term10", False), ("proceedings", "/sejm/term10/proceedings", False),
            ("votings", "/sejm/term10/votings", False), ("votings_s1", "/sejm/term10/votings/1", False),
            ("prints", "/sejm/term10/prints?limit=3", False), ("processes", "/sejm/term10/processes?limit=3", False),
            ("committees", "/sejm/term10/committees", False), ("clubs", "/sejm/term10/clubs", False),
            ("MP", "/sejm/term10/MP", True), ("interpellations", "/sejm/term10/interpellations?limit=2", False),
            ("writtenQuestions", "/sejm/term10/writtenQuestions?limit=2", False)]
    eli = [("acts", "/eli/acts", False), ("DU", "/eli/acts/DU", False), ("DU_2025", "/eli/acts/DU/2025", False),
           ("MP_2025", "/eli/acts/MP/2025", False)]
    out = {"sejm": {}, "eli": {}}
    for grp, items in (("sejm", sejm), ("eli", eli)):
        for name, path, personal in items:
            if not rp.can_fetch("zohelo-data", BASE + path) or not rp.can_fetch("*", BASE + path):
                out[grp][name] = "robots disallow"
                continue
            out[grp][name] = jinfo(fetch(path), personal=personal)
    docs = {}
    for name, path in (("sejm_doc", "/sejm.html"), ("eli_doc", "/eli.html"), ("root", "/")):
        docs[name] = legal(fetch(path))
    annotate("notice", "PLG-024/026 probe: robots and stop", {"robots": res["robots"], "stop": STOP["reason"]})
    s = out["sejm"]
    annotate("notice", "Sejm term10, proceedings, votings", {k: s.get(k) for k in ("term10", "proceedings", "votings", "votings_s1")})
    annotate("notice", "Sejm prints, processes, interpellations, questions", {k: s.get(k) for k in ("prints", "processes", "interpellations", "writtenQuestions")})
    annotate("notice", "Sejm committees, clubs, MP (keys only)", {k: s.get(k) for k in ("committees", "clubs", "MP")})
    annotate("notice", "ELI acts, DU", {k: out["eli"].get(k) for k in ("acts", "DU")})
    annotate("notice", "ELI DU 2025, MP 2025", {k: out["eli"].get(k) for k in ("DU_2025", "MP_2025")})
    annotate("notice", "Docs pages: licence and limit text", docs)


if __name__ == "__main__":
    main()
