"""Read-only probe of the BZP read WebService (PLG-022, Task #1382): instruction ZIP, swagger, list shape.

GET only, one request at a time, 1.5 s apart, at most 60 requests; robots.txt read first and honoured.
Personal data are never printed: for notice answers only key names, value types and a fixed set of
non-personal fields (ids, types, dates, CPV, province) are shown.
"""

import io
import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
import urllib.robotparser
import zipfile

UA = "zohelo-data/1.0 (+https://zohelo.com; public statistics platform)"
HOST = "https://ezamowienia.gov.pl"
API = f"{HOST}/mo-client-board/api/notices"
ZIP = "https://media.ezamowienia.gov.pl/pod/2022/08/Zalacznik-3-Instrukcja-integracji-z-API-BZP.zip"
BUDGET = [60]
RP = urllib.robotparser.RobotFileParser()
SAFE = re.compile(r"(?i)(id$|type|date|number|cpv|province|kind|status|result|isTender|currency|count|^is)")


def get(url, n=30_000_000, check_robots=True):
    if BUDGET[0] <= 0:
        return None, "budget used", {}
    BUDGET[0] -= 1
    if check_robots and url.startswith(HOST) and not RP.can_fetch(UA, url):
        return None, "robots disallow", {}
    time.sleep(1.5)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json, */*"})
        with urllib.request.urlopen(req, timeout=120) as r:
            body = r.read(n)
            return body, f"{r.status}", dict(r.headers)
    except urllib.error.HTTPError as e:
        return e.read(1500), f"HTTP {e.code}", dict(e.headers or {})
    except Exception as e:  # noqa: BLE001
        return None, f"{type(e).__name__}: {str(e)[:160]}", {}


def shape(value):
    if isinstance(value, dict):
        return {k: shape(v) for k, v in value.items()}
    if isinstance(value, list):
        return [shape(value[0]), f"len={len(value)}"] if value else []
    if isinstance(value, str):
        return f"str({len(value)})"
    return type(value).__name__


def safe_values(item):
    out = {}
    for k, v in item.items():
        low = k.lower()
        if isinstance(v, (str, int, float, bool)) or v is None:
            if SAFE.search(k) and "name" not in low and "mail" not in low and "phone" not in low and "national" not in low:
                out[k] = v if not isinstance(v, str) else v[:80]
    return out


def notices(url, label):
    body, st, hdr = get(url)
    keep = {
        k: v
        for k, v in hdr.items()
        if k.lower() in ("content-type", "content-length", "last-modified", "etag", "retry-after")
        or any(w in k.lower() for w in ("rate", "page", "total", "count"))
    }
    print(f"\n=== {label}\nGET {url}\n--- {st} {keep} bytes={len(body) if body else 0}", flush=True)
    if body is None:
        return None
    text = body.decode("utf-8", "replace")
    try:
        doc = json.loads(text)
    except ValueError:
        print("non-JSON:", re.sub(r"\s+", " ", text)[:800])
        return None
    if not st.startswith("200"):
        print(json.dumps(doc, ensure_ascii=False)[:1500])
        return None
    items = doc if isinstance(doc, list) else next((v for v in doc.values() if isinstance(v, list)), [])
    if isinstance(doc, dict):
        print("top-level:", {k: (v if not isinstance(v, (list, dict)) else type(v).__name__) for k, v in doc.items()})
    print("items:", len(items))
    if items:
        print("shape of item 0:", json.dumps(shape(items[0]), ensure_ascii=False)[:5000])
        for it in items[:5]:
            print("safe:", json.dumps(safe_values(it), ensure_ascii=False))
        types = {}
        for it in items:
            t = str(it.get("noticeType"))
            types[t] = types.get(t, 0) + 1
        print("noticeType counts:", types)
    return items


def zip_text():
    body, st, _ = get(ZIP, check_robots=False)
    print(f"\n=== ZIP {ZIP}: {st} bytes={len(body) if body else 0}", flush=True)
    if not body or not st.startswith("200"):
        return
    z = zipfile.ZipFile(io.BytesIO(body))
    for info in z.infolist():
        print("member:", info.filename, info.file_size)
    for info in z.infolist():
        name = info.filename.lower()
        data = z.read(info)
        text = ""
        if name.endswith(".docx"):
            inner = zipfile.ZipFile(io.BytesIO(data))
            xml = inner.read("word/document.xml").decode("utf-8", "replace")
            xml = re.sub(r"</w:p>", "\n", xml)
            text = re.sub(r"<[^>]+>", "", xml)
        elif name.endswith(".pdf"):
            subprocess.run([sys.executable, "-m", "pip", "install", "-q", "pypdf==5.1.0"], check=False)
            try:
                from pypdf import PdfReader

                text = "\n".join(p.extract_text() or "" for p in PdfReader(io.BytesIO(data)).pages)
            except Exception as e:  # noqa: BLE001
                text = f"pdf error {e}"
        elif name.endswith((".json", ".txt", ".md", ".yaml", ".yml", ".xml", ".html")):
            text = data.decode("utf-8", "replace")
        if text:
            print(f"\n----- TEXT {info.filename} ({len(text)} chars)\n{text[:60000]}\n----- END", flush=True)


def swagger():
    for path in (
        "/mo-client-board/swagger/v1/swagger.json",
        "/mo-client-board/swagger/index.html",
        "/mo-client-board/swagger.json",
    ):
        body, st, _ = get(HOST + path)
        print(f"\n=== SWAGGER {path}: {st}", flush=True)
        if body and st.startswith("200"):
            try:
                doc = json.loads(body)
            except ValueError:
                print(re.sub(r"\s+", " ", body.decode("utf-8", "replace"))[:500])
                continue
            for p, ops in doc.get("paths", {}).items():
                for verb, op in ops.items():
                    params = [
                        (q.get("name"), q.get("in"), (q.get("schema") or {}).get("type"), (q.get("schema") or {}).get("enum"))
                        for q in op.get("parameters", [])
                    ]
                    print(verb.upper(), p, json.dumps(params, ensure_ascii=False)[:1500])
            for name, schema in (doc.get("components", {}).get("schemas", {}) or {}).items():
                print("SCHEMA", name, json.dumps(schema.get("enum") or list((schema.get("properties") or {}).keys()), ensure_ascii=False)[:2000])
            return


def main():
    body, st, _ = get(f"{HOST}/robots.txt", check_robots=False)
    print("robots.txt", st, (body or b"").decode("utf-8", "replace")[:1500])
    RP.parse(body.decode("utf-8", "replace").splitlines() if body and st.startswith("200") else [])
    zip_text()
    swagger()
    notices(API + "/", "bare")
    d1, d2 = "2026-11-03T00:00:00", "2026-11-04T00:00:00"
    notices(f"{API}?PublicationDateFrom={d1}&PublicationDateTo={d2}&PageSize=5", "dates only")
    notices(f"{API}?NoticeType=ContractNotice&PublicationDateFrom={d1}&PublicationDateTo={d2}&PageSize=5&PageNumber=1", "contract notice p1")
    notices(f"{API}?NoticeType=TenderResultNotice&PublicationDateFrom={d1}&PublicationDateTo={d2}&PageSize=5&PageNumber=1", "result p1")
    notices(f"{API}?NoticeType=ContractNotice&PublicationDateFrom={d1}&PublicationDateTo={d2}&PageSize=1000", "contract one day big page")
    notices(f"{API}/search?NoticeType=ContractNotice&PublicationDateFrom={d1}&PublicationDateTo={d2}&PageSize=5", "search path")
    print("\nrequests left:", BUDGET[0])


if __name__ == "__main__":
    main()
