"""Dev probe 2 for #3318/#3319 (test branch only): is the UOKiK decisions index anywhere else, does the host answer.

Read-only. 1. TCP to decyzje.uokik.gov.pl 443 and 80 (one try each, 45 s). 2. dane.gov.pl: institutions whose name
holds "Konkurencji" or "UOKiK" and the titles of their datasets (official open-data portal; one request at a time,
2 s apart). 3. robots.txt only of dziennikurzedowy.uokik.gov.pl (UOKiK's official journal host): status and whether it
names Claude agents; nothing else is read from it. Never uokik.gov.pl.
"""

from __future__ import annotations

import json
import socket
import time

import requests

from ingestion.common.report import annotate

S = requests.Session()
S.headers["User-Agent"] = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"


def tcp(host: str) -> dict:
    out = {}
    for port in (443, 80):
        try:
            with socket.create_connection((host, port), timeout=45):
                out[str(port)] = "open"
        except OSError as exc:
            out[str(port)] = f"{type(exc).__name__}: {exc}"[:90]
    return out


def dane() -> dict:
    out: dict = {"institutions": [], "datasets": []}
    for q in ("Konkurencji", "UOKiK"):
        r = S.get("https://api.dane.gov.pl/1.4/institutions", params={"q": q, "per_page": 20}, timeout=(20, 60))
        for d in r.json().get("data", []):
            a = d.get("attributes", {})
            row = [d.get("id"), str(a.get("title"))[:90], a.get("datasets_count")]
            if row not in out["institutions"]:
                out["institutions"].append(row)
        time.sleep(2)
    for inst in out["institutions"]:
        if "konkurencji" not in str(inst[1]).lower():
            continue
        r = S.get(
            f"https://api.dane.gov.pl/1.4/institutions/{inst[0]}/datasets", params={"per_page": 100}, timeout=(20, 60)
        )
        for d in r.json().get("data", []):
            a = d.get("attributes", {})
            out["datasets"].append([d.get("id"), str(a.get("title"))[:80], str(a.get("modified"))[:10]])
        time.sleep(2)
    return out


def main() -> int:
    res: dict = {"utc": time.strftime("%Y-%m-%d %H:%M", time.gmtime()), "decyzje": tcp("decyzje.uokik.gov.pl")}
    try:
        res["dane_gov_pl"] = dane()
    except Exception as exc:
        res["dane_gov_pl"] = f"{type(exc).__name__}: {str(exc)[:150]}"
    try:
        r = S.get("https://dziennikurzedowy.uokik.gov.pl/robots.txt", timeout=(30, 60))
        low = r.text.lower()
        res["journal_robots"] = [r.status_code, "claudebot" in low, "anthropic" in low, len(r.text)]
    except requests.RequestException as exc:
        res["journal_robots"] = f"{type(exc).__name__}: {str(exc)[:120]}"
    print("PROBE2", json.dumps(res, ensure_ascii=False), flush=True)
    annotate("notice", "uokik probe2", res)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
