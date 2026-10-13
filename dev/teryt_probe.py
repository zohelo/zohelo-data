"""Probe the public eTERYT full-file download page: form fields, download controls, one TERC download.
Polite: a handful of requests, one at a time. Writes nothing to R2."""
import re
import time

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://eteryt.stat.gov.pl/eTeryt/rejestr_teryt/udostepnianie_danych/"
PAGES = [BASE + "baza_teryt/baza_teryt.aspx", BASE + "baza_internetowa/pliki_pelne.aspx",
         BASE + "baza_teryt/uzytkownicy_indywidualni/pobieranie/pliki_pelne.aspx"]
s = requests.Session()
s.headers["User-Agent"] = UA
out = {}
found_page = None
for url in PAGES:
    time.sleep(1)
    try:
        r = s.get(url, timeout=60)
    except Exception as exc:
        out[url] = f"error {exc}"[:200]
        continue
    links = sorted(set(re.findall(r'href="([^"]*pliki[^"]*)"', r.text, re.I)))[:15]
    inputs = re.findall(r'<input[^>]+(?:type="submit"|type="image")[^>]*>', r.text, re.I)
    anchors = re.findall(r"__doPostBack\((?:'|&#39;|&quot;)([^'&]+)", r.text)
    out[url] = {"status": r.status_code, "final": r.url, "len": len(r.text), "pliki_links": links,
                "submits": [re.sub(r"\s+", " ", i)[:160] for i in inputs][:20], "postbacks": sorted(set(anchors))[:30]}
    if r.status_code == 200 and r.url.endswith("pobieranie/pliki_pelne.aspx"):
        found_page = (r.url, r.text)
download = None
fields = {}
if found_page:
    url, html = found_page
    fields = dict(re.findall(r'<input[^>]+name="(__[A-Z]+)"[^>]+value="([^"]*)"', html))
    targets = [t for t in re.findall(r"__doPostBack\((?:'|&#39;|&quot;)([^'&]+)", html) if re.search(r"TERC", t, re.I)]
    snippets = [re.sub(r"\s+", " ", html[max(0, m.start() - 300): m.start() + 300]) for m in re.finditer(r"TERC", html)][:4]
    del snippets
    buttons = re.findall(r'<input[^>]+type="(?:submit|image)"[^>]+name="([^"]+)"[^>]*>', html)
    terc_buttons = [b for b in buttons if re.search(r"terc", b, re.I)]
    data = dict(fields)
    if targets:
        data.update({"__EVENTTARGET": targets[0], "__EVENTARGUMENT": ""})
    elif terc_buttons:
        data[terc_buttons[0]] = "Pobierz"
    if targets or terc_buttons:
        time.sleep(2)
        r = s.post(url, data=data, timeout=120)
        download = {"via": (targets or terc_buttons)[0], "status": r.status_code,
                    "content_type": r.headers.get("Content-Type"),
                    "disposition": r.headers.get("Content-Disposition"), "bytes": len(r.content),
                    "head": r.content[:40].hex()}
annotate("notice", "teryt probe", {"download": download, "fields": sorted(fields) if found_page else None})
