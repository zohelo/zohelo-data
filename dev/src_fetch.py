"""Read-only probe: fetch documentation URLs from a runner and print status, headers and text excerpts."""
import re, sys, concurrent.futures as cf, urllib.request, ssl

KW = re.compile(r"(licenc|license|creative commons|CC[ -]BY|ponown\w+ wykorzyst|ponowne|re-?use|terms of (use|service)|regulamin|warunki|prawa autorskie|copyright|attribution|źródło:|zrodlo:|rate limit|limit zapyta|dane publiczne|otwarte dane|open data|bulk|download|pobierz|dump)", re.I)
UA = "Mozilla/5.0 (compatible; zohelo-data-research/1.0; +https://zohelo.com)"

def text_of(raw, ctype):
    t = raw.decode("utf-8", "replace") if raw else ""
    if "html" in ctype:
        t = re.sub(r"(?is)<(script|style|noscript).*?</\1>", " ", t)
        t = re.sub(r"(?s)<[^>]+>", " ", t)
        t = re.sub(r"&nbsp;", " ", t)
    return re.sub(r"\s+", " ", t).strip()

def one(line):
    sid, url = line.split("\t")
    out = [f"=== {sid} {url}"]
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*"})
        with urllib.request.urlopen(req, timeout=25, context=ssl.create_default_context()) as r:
            raw = r.read(600000)
            ct = r.headers.get("content-type", "")
            out.append(f"status={r.status} final={r.geturl()} ctype={ct} len={r.headers.get('content-length')} lastmod={r.headers.get('last-modified')} bytes_read={len(raw)}")
            if not any(x in ct for x in ("zip", "pdf", "octet", "image", "excel", "spreadsheet")):
                t = text_of(raw, ct)
                out.append("HEAD: " + t[:700])
                n = 0
                last = -1000
                for m in KW.finditer(t):
                    if m.start() - last < 350: continue
                    last = m.start()
                    out.append("  ..." + t[max(0, m.start() - 120): m.start() + 380] + "...")
                    n += 1
                    if n >= 7: break
    except Exception as e:
        out.append(f"ERROR {type(e).__name__}: {str(e)[:200]}")
    return "\n".join(out)

if __name__ == "__main__":
    lines = [l.rstrip("\n") for l in open(f"dev/src_urls_{sys.argv[1]}.txt") if "\t" in l]
    with cf.ThreadPoolExecutor(12) as ex:
        for res in ex.map(one, lines):
            print(res, flush=True)
