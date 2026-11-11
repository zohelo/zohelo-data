"""Read-only probe v2: directive file dev/src_dir_<name>.txt, lines: id<TAB>url<TAB>mode<TAB>arg."""
import re, sys, html, concurrent.futures as cf, urllib.request, ssl
UA = "Mozilla/5.0 (compatible; zohelo-data-research/1.0; +https://zohelo.com)"

def get(url, n=900000):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*", "Accept-Language": "pl,en"})
    with urllib.request.urlopen(req, timeout=30, context=ssl.create_default_context()) as r:
        raw = r.read(n)
        ct = r.headers.get("content-type", "")
        m = re.search(r"charset=([\w-]+)", ct)
        enc = m.group(1) if m else None
        if not enc:
            m2 = re.search(rb"charset=([\w-]+)", raw[:3000])
            enc = m2.group(1).decode() if m2 else "utf-8"
        try:
            txt = raw.decode(enc, "replace")
        except LookupError:
            txt = raw.decode("utf-8", "replace")
        return r, raw, ct, txt

def plain(t):
    t = re.sub(r"(?is)<(script|style|noscript).*?</\1>", " ", t)
    t = re.sub(r"(?s)<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", html.unescape(t)).strip()

def one(line):
    parts = line.split("\t")
    sid, url, mode = parts[0], parts[1], parts[2]
    arg = parts[3] if len(parts) > 3 else ""
    out = [f"=== {sid} {mode} {url}"]
    try:
        r, raw, ct, txt = get(url, 8000000 if mode=="pdf" else 900000)
        out.append(f"status={r.status} final={r.geturl()} ctype={ct} len={r.headers.get('content-length')} lastmod={r.headers.get('last-modified')} read={len(raw)}")
        if mode == "head":
            pass
        elif mode == "raw":
            out.append(txt[: int(arg or 800)])
        elif mode == "full":
            t = plain(txt) if re.match(r"\s*<", txt) else txt
            out.append(t[: int(arg or 3000)])
        elif mode == "text":
            t = plain(txt) if re.match(r"\s*<", txt) else re.sub(r"\s+", " ", txt)
            n, last = 0, -9999
            for m in re.finditer(arg, t, re.I):
                if m.start() - last < 1000: continue
                last = m.start()
                out.append("  >> " + t[max(0, m.start() - 200): m.start() + 1100])
                n += 1
                if n >= 4: break
            if n == 0: out.append("  (no match)")
        elif mode == "pdf":
            import subprocess, tempfile, os
            f = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False); f.write(raw); f.close()
            try:
                t = subprocess.run(["pdftotext", "-layout", f.name, "-"], capture_output=True, text=True, timeout=60).stdout
            except FileNotFoundError:
                subprocess.run([sys.executable, "-m", "pip", "install", "-q", "pypdf"], capture_output=True)
                from pypdf import PdfReader
                t = "\n".join((pg.extract_text() or "") for pg in PdfReader(f.name).pages[:40])
            t = re.sub(r"\s+", " ", t)
            n, last = 0, -9999
            for m in re.finditer(arg or ".", t, re.I):
                if m.start() - last < 1000: continue
                last = m.start()
                out.append("  >> " + t[max(0, m.start() - 200): m.start() + 1100]); n += 1
                if n >= 4: break
            if n == 0: out.append("  (no match) pdf chars=%d" % len(t))
        elif mode == "links":
            n = 0
            for m in re.finditer(r'(?is)<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', txt):
                h, a = m.group(1), plain(m.group(2))
                if re.search(arg, h + " " + a, re.I):
                    out.append(f"  {a[:90]} -> {h}")
                    n += 1
                    if n >= 45: break
            if n == 0: out.append("  (no links)")
    except Exception as e:
        out.append(f"ERROR {type(e).__name__}: {str(e)[:200]}")
    return "\n".join(out)

if __name__ == "__main__":
    lines = [l.rstrip("\n") for l in open(f"dev/src_dir_{sys.argv[1]}.txt") if l.count("\t") >= 2]
    with cf.ThreadPoolExecutor(12) as ex:
        for res in ex.map(one, lines):
            print(res, flush=True)
