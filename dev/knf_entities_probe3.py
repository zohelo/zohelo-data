"""Read-only Dev probe 3 for PLG-016 (#2994): why two equal fl requests gave different bytes. Writes nothing; prints
only field names and ids near the first difference. Deleted before merge."""

from __future__ import annotations

import json
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

FULL = URL.replace("&fl=" + URL.split("&fl=")[1].split("&")[0], "")
for name, url in (("fl", URL), ("full", FULL), ("fl", URL)):
    bodies = []
    for _ in range(3):
        bodies.append(requests.get(url, headers={"User-Agent": UA}, timeout=120).content)
        time.sleep(3)
    parsed = [json.loads(b) for b in bodies]
    print(name, "sizes", [len(b) for b in bodies], "bytes equal", [bodies[0] == b for b in bodies],
          "parsed equal", [parsed[0] == p for p in parsed])
    for b in bodies[1:]:
        if b != bodies[0]:
            i = next(k for k in range(min(len(b), len(bodies[0]))) if b[k] != bodies[0][k])
            ctx0 = bodies[0][max(0, i - 200): i + 60].decode("utf-8", "replace")
            ctx1 = b[max(0, i - 200): i + 60].decode("utf-8", "replace")
            mask = lambda s: re.sub(r'("ae_entityName":")[^"]*', r"\1…", s)  # noqa: E731
            print("  first diff at", i, "\n  A:", mask(ctx0).replace("\n", " "), "\n  B:", mask(ctx1).replace("\n", " "))
            d0 = {d["id"]: d for d in parsed[0]["response"]["docs"]}
            d1 = {d["id"]: d for d in json.loads(b)["response"]["docs"]}
            changed = [k for k in d0 if d0[k] != d1.get(k)]
            print("  docs changed", len(changed), [(k, sorted(f for f in d0[k] if d0[k].get(f) != d1[k].get(f))) for k in changed[:5]])
            print("  order same", list(d0) == list(d1), "key order same", all(list(d0[k]) == list(d1[k]) for k in d0))
