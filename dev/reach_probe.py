"""Read-only reachability probe from a runner (reliability lane, 25 Nov): one HEAD/GET per host, no writes."""

from __future__ import annotations

import time

import requests

from ingestion.common.report import annotate

URLS = [
    "https://ckan.multimediagdansk.pl/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/30e783e4-2bec-4a7d-bb22-ee3e3b26ca96/download/gtfsgoogle.zip",
    "https://sdi.eea.europa.eu/catalogue/srv/api/records/3b7fe76c-524a-439a-bfd2-a6e4046302a2/related?type=siblings",
    "https://www.eea.europa.eu/",
]


def main() -> int:
    out = {}
    for url in URLS:
        started = time.monotonic()
        try:
            r = requests.head(url, timeout=(20, 60), allow_redirects=True, headers={"User-Agent": "zohelo-data/1.0"})
            out[url[:60]] = {"status": r.status_code, "s": round(time.monotonic() - started, 1),
                             "last_modified": r.headers.get("last-modified"), "length": r.headers.get("content-length")}
        except Exception as exc:
            out[url[:60]] = f"{type(exc).__name__} after {round(time.monotonic() - started, 1)} s: {str(exc)[:160]}"
        time.sleep(1)
    annotate("notice", "reachability from the runner", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
