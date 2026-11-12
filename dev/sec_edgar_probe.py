"""Second read-only look at SEC EDGAR (CD-012): is the refusal of run 43321810312 the same from another runner?

Two requests, 10 s apart, with the repository's identified user agent; nothing stored, nothing retried another way.
"""
import time

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns.bulk_file import USER_AGENT


def main():
    out = []
    for url in ("https://www.sec.gov/robots.txt", "https://data.sec.gov/robots.txt"):
        try:
            r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=60)
            text = " ".join(r.text.split())
            out.append({"url": url, "status": r.status_code, "server": r.headers.get("Server"),
                        "type": r.headers.get("Content-Type"), "text": text[:1200]})
        except requests.RequestException as exc:
            out.append({"url": url, "error": type(exc).__name__})
        time.sleep(10)
    annotate("notice", "sec_edgar probe 2", {"user_agent": USER_AGENT, "answers": out})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
