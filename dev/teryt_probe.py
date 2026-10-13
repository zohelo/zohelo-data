"""Probe all eTERYT full files: zip members, sizes, first bytes. 9 requests, one at a time."""
import io
import re
import time
import zipfile

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
URL = ("https://eteryt.stat.gov.pl/eTeryt/rejestr_teryt/udostepnianie_danych/baza_teryt/"
       "uzytkownicy_indywidualni/pobieranie/pliki_pelne.aspx")
s = requests.Session()
s.headers["User-Agent"] = UA
html = s.get(URL, timeout=60).text
fields = dict(re.findall(r'<input[^>]+name="(__[A-Z]+)"[^>]+value="([^"]*)"', html))
targets = sorted(set(re.findall(r"__doPostBack\((?:'|&#39;)([^'&]+)", html)))
stan = re.findall(r"[Ss]tan na[^0-9]{0,40}([0-9]{2,4}[-.][0-9]{2}[-.][0-9]{2,4})", html)[:3]
out = {"stan_na": stan, "targets": targets, "files": {}}
for target in targets:
    time.sleep(2)
    r = s.post(URL, data={**fields, "__EVENTTARGET": target, "__EVENTARGUMENT": ""}, timeout=180)
    info = {"status": r.status_code, "type": r.headers.get("Content-Type"),
            "name": (r.headers.get("Content-Disposition") or "")[-60:], "bytes": len(r.content)}
    if r.content[:2] == b"PK":
        z = zipfile.ZipFile(io.BytesIO(r.content))
        info["members"] = [(i.filename, i.file_size) for i in z.infolist()][:4]
        first = z.read(z.infolist()[0])[:300]
        info["first"] = first.decode("utf-8", "replace")
    out["files"][target.split("$")[-1]] = info
annotate("notice", "teryt probe", out)
