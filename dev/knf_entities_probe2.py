"""Read-only Dev probe 2 for PLG-016 (#2994): the registers site's Legal Notice footer target, the reduced field list
(fl) and its byte stability. Writes nothing. Deleted before merge."""

from __future__ import annotations

from collections import Counter
from hashlib import sha256
import re
import time

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
FL = ("id,ae_dbId,ae_entityName,ae_commercialName,ae_entityTypeCode,ae_entityTypeLabel,ae_lei,ae_headOfficeLei,"
      "ae_competentAuthority,ae_homeMemberState,ae_hostMemberStates,ae_officeType,ae_status,"
      "ae_authorisationNotificationDate,ae_authorisationWithdrawalEndDate,ae_lastUpdate,ae_legalform,ae_isSme,"
      "no_of_funds")
URL = ("https://registers.esma.europa.eu/solr/esma_registers_upreg/select?q=*%3A*"
       "&fq=ae_homeMemberState%3A%22POLAND%22&fq=type_s%3Aparent&fl=" + FL +
       "&sort=id+asc&rows=1000&start=0&wt=json&omitHeader=true")


def text_of(html):
    return " ".join(re.sub(r"<[^>]+>", " ", re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", html)).split())


r = requests.get("https://registers.esma.europa.eu/publication/legalNoticePage", headers={"User-Agent": UA}, timeout=60)
t = text_of(r.text)
print("LEGAL", r.status_code, r.url, "history", [h.status_code for h in r.history], "len", len(t))
print("LEGAL TEXT", t[:4000])
for m in re.finditer(r'href="([^"]+)"', r.text):
    if re.search(r"legal|notice|copyright|esma\.europa\.eu/(about|legal)", m.group(1), re.I):
        print("LEGAL LINK", m.group(1))
time.sleep(3)
a = requests.get(URL, headers={"User-Agent": UA}, timeout=120)
time.sleep(3)
b = requests.get(URL, headers={"User-Agent": UA}, timeout=120)
print("FL", a.status_code, len(a.content), "same", sha256(a.content).hexdigest() == sha256(b.content).hexdigest(), a.content[:300])
docs = a.json()["response"]["docs"]
print("numFound", a.json()["response"]["numFound"], "docs", len(docs), "keys", sorted({k for d in docs for k in d}))
print("status x withdrawal filled", Counter((d["ae_status"], bool(d.get("ae_authorisationWithdrawalEndDate"))) for d in docs))
print("host states list lengths", Counter(len(d.get("ae_hostMemberStates") or []) for d in docs))
print("no_of_funds by type", Counter((d["ae_entityTypeCode"], d.get("no_of_funds") is not None) for d in docs))
print("headOfficeLei filled by office", Counter((d["ae_officeType"], bool(d.get("ae_headOfficeLei")), bool(d.get("ae_lei"))) for d in docs))
print("lei repeated detail", [(k, [(d["ae_officeType"], d["ae_entityTypeCode"]) for d in docs if d.get("ae_lei") == k]) for k, c in Counter(d.get("ae_lei") for d in docs if d.get("ae_lei")).items() if c > 1])
FORMS = (r"S\.?\s?A\.?|SP\.?\s?Z\s?O\.?\s?O|SPÓŁKA|SPOLKA|LIMITED|LTD|GMBH|AG\b|PLC|ODDZIAŁ|BRANCH|SUCURSAL|SUCCURSALE|"
         r"ORGANIZAČNÍ|POBOČKA|ZWEIGNIEDERLASSUNG|NIEDERLASSUNG|SIÓKTELEP|SUCURSALA|ASI\b|TFI|S\.K\.A|SP\.\s?K|SE\b|N\.V|"
         r"B\.V|S\.R\.L|SARL|KFT")
WORDS = (r"FUND|CAPITAL|INVEST|MANAGEMENT|PARTNERS|GROUP|SECURITIES|BROKER|MARKET|BANK|ASSET|EQUITY|VENTURE|HOLDING|"
         r"FINANC|TRADING|EXCHANGE|GIEŁDA|DOM MAKLERSKI|TOWARZYSTWO|FUNDUSZ")
for d in docs:
    n = (d.get("ae_entityName") or "").upper()
    if not re.search(FORMS, n):
        shown = n if re.search(WORDS, n) else f"[withheld: {len(n.split())} words]"
        print("NO LEGAL FORM", d["id"], d["ae_entityTypeCode"], d["ae_officeType"], d["ae_status"], shown)
print("names uppercase-only", sum(1 for d in docs if d["ae_entityName"] == d["ae_entityName"].upper()))
