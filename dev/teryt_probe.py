"""Dev probe (#843): read-only eTERYT page and one download, no writes. Deleted before merge."""

import re

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.gus_teryt.extract import CONTROLS, PAGE, USER_AGENT, catalogue_date

facts = {}
session = requests.Session()
session.headers["User-Agent"] = USER_AGENT
try:
    page = session.get(PAGE, timeout=120)
    html = page.text
    facts["get"] = {"status": page.status_code, "bytes": len(page.content), "final_url": page.url[:200]}
    fields = dict(re.findall(r'<input[^>]+name="(__[A-Z]+)"[^>]+value="([^"]*)"', html))
    facts["form_fields"] = sorted(fields)
    facts["controls_missing"] = [e for e, c in CONTROLS.items() if c.split("$")[-1] not in html]
    facts["control_like_names"] = sorted(set(re.findall(r"ctl00\$body\$[A-Za-z0-9_]+", html)))[:30]
    facts["title"] = (re.search(r"<title>(.*?)</title>", html, re.S) or [None, ""])[1].strip()[:120]
    if "__VIEWSTATE" in fields:
        post = session.post(
            PAGE, data={**fields, "__EVENTTARGET": CONTROLS["TERC_Urzedowy"], "__EVENTARGUMENT": ""}, timeout=180
        )
        facts["post"] = {
            "status": post.status_code,
            "bytes": len(post.content),
            "zip": post.content[:2] == b"PK",
            "content_type": post.headers.get("Content-Type"),
            "head": post.content[:200].decode("utf-8", "replace") if post.content[:2] != b"PK" else "",
        }
        if post.content[:2] == b"PK":
            facts["post"]["catalogue_date"] = catalogue_date(post.content)
except Exception as error:  # probe: report whatever happens
    facts["error"] = f"{type(error).__name__}: {error}"[:500]
annotate("notice", "dev teryt probe", facts)
