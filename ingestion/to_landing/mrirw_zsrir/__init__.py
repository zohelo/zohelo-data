"""MRiRW ZSRIR market bulletins: Landing activities (register PLS-032, zsrir.minrol.gov.pl).

The ministry's ZSRIR portal publishes each market's weekly bulletin ("Biuletyn informacyjny - rynek ...") as an XLSX
and a PDF file; the portal's own public endpoints list them (``/api/ZsrirData/GetReportFileList?id=<report>``: id,
period, publication time and file name of every file since August 2024) and serve them
(``/api/ZsrirData/DownloadReportFile?id=<file>``). No sign-in. Entity = one market's bulletin (MARKETS), version =
download time (the server sends no Last-Modified); only the newest XLSX issue is landed, because its monthly sheets
carry the whole history Silver reads (milk 2004-, cereals about 20 years, pigs the last five years). A copy lands only
when the newest file id changed and its SHA-256 differs from the landed copy.

TLS: the server sends its leaf certificate without the issuing intermediate (Certum DV TLS G2 R39 CA), so a client
that does not fetch missing intermediates (Python, unlike browsers) cannot build the chain. INTERMEDIATE is that
certificate as Certum publishes it at the address named in the leaf's Authority Information Access field
(http://certumdvtlsg2r39ca.repository.certum.pl/certumdvtlsg2r39ca.cer, SHA-256 fingerprint 83:C0:A5:A7:...:44:83,
valid to 2039-06-05), issued by Certum Trusted Root CA of certifi's bundle. Requests to the portal verify the chain
and host name as usual against certifi's roots plus this one certificate; verification is never switched off.

landing.inspect  newest XLSX issue per market vs Landing (current / stale / missing); no writes
landing.extract  the newest issue of each market whose file id is not landed yet, one download at a time
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

from collections.abc import Iterator
import contextlib
import os
import tempfile
import time
from typing import Any

SOURCE = "mrirw_zsrir"
BASE = "https://zsrir.minrol.gov.pl"
PAUSE = 2.0
# market entity: report id of /api/ZsrirData/GetReportList (probe 43838495001)
MARKETS = {"cereals": 2, "milk": 1, "pork": 14}
INTERMEDIATE = """-----BEGIN CERTIFICATE-----
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
8FpPkganuCwy++7umTkM7+Q=
-----END CERTIFICATE-----
"""


@contextlib.contextmanager
def portal_tls() -> Iterator[str]:
    """certifi's roots plus INTERMEDIATE as the CA bundle of requests (REQUESTS_CA_BUNDLE) for the block's calls."""
    import certifi

    with tempfile.TemporaryDirectory(prefix="zohelo-zsrir-") as folder:
        bundle = os.path.join(folder, "ca.pem")
        with open(certifi.where(), encoding="ascii") as roots, open(bundle, "w", encoding="ascii") as out:
            out.write(roots.read() + "\n" + INTERMEDIATE)
        before = os.environ.get("REQUESTS_CA_BUNDLE")
        os.environ["REQUESTS_CA_BUNDLE"] = bundle
        try:
            yield bundle
        finally:
            if before is None:
                os.environ.pop("REQUESTS_CA_BUNDLE", None)
            else:
                os.environ["REQUESTS_CA_BUNDLE"] = before


def _json(path: str) -> Any:
    """GET a portal endpoint (inside portal_tls) with bounded retries; 4xx other than 408/429 fails at once."""
    import requests

    from ingestion.to_landing.patterns import bulk_file

    for attempt in range(bulk_file.RETRIES):
        try:
            response = requests.get(
                BASE + path,
                headers={"User-Agent": bulk_file.USER_AGENT, "Accept": "application/json"},
                timeout=(30, 120),
            )
            if 400 <= response.status_code < 500 and response.status_code not in bulk_file.RETRY_STATUSES:
                raise bulk_file.PermanentError(f"{response.status_code}: {path}")
            response.raise_for_status()
            return response.json()
        except bulk_file.PermanentError:
            raise
        except (requests.RequestException, ValueError):
            if attempt == bulk_file.RETRIES - 1:
                raise
            time.sleep(30 * (attempt + 1))
    raise RuntimeError("unreachable")


def newest_issues() -> dict[str, dict[str, Any]]:
    """{market: newest XLSX file of its bulletin} (latest period end, then latest publication), one list per market."""
    out = {}
    for n, (market, report) in enumerate(MARKETS.items()):
        if n:
            time.sleep(PAUSE)
        files = [
            f
            for f in _json(f"/api/ZsrirData/GetReportFileList?id={report}")["reportFiles"]
            if str(f.get("filename", "")).lower().endswith(".xlsx")
        ]
        if not files:
            raise RuntimeError(f"{SOURCE}: report {report} ({market}) lists no XLSX file")
        f = max(files, key=lambda x: (x["dateTo"], x["publishedDateTime"], x["id"]))
        out[market] = {
            "entity": market,
            "url": f"{BASE}/api/ZsrirData/DownloadReportFile?id={f['id']}",
            "issue": f"{f['dateFrom']}..{f['dateTo']}",
            "published": f["publishedDateTime"][:19],
            "filename": f["filename"],
            "files_listed": len(files),
        }
    return out


def held(landing_root: str = "01_landing") -> dict[str, dict[str, Any]]:
    """{market: newest Landing receipt}."""
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    out = {}
    for market in MARKETS:
        receipts = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, market, landing_root))
        if receipts:
            out[market] = receipts[-1]
    return out


def _inspect(p) -> int:
    from ingestion.common.report import annotate

    landed = held()
    with portal_tls():
        issues = newest_issues()
    rows = {
        m: {
            **{k: i[k] for k in ("issue", "published", "filename", "files_listed")},
            "state": "missing" if m not in landed else ("current" if landed[m]["source_url"] == i["url"] else "stale"),
        }
        for m, i in issues.items()
    }
    annotate("notice", f"{SOURCE} landing inspect", rows)
    return 0


def extract(budget_minutes: float, landing_root: str = "01_landing") -> int:
    """budget_minutes is unused: three files of 1-3 MB a run."""
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    landed = held(landing_root)
    results = {}
    with portal_tls():
        issues = newest_issues()
        for market, issue in issues.items():
            if market in landed and landed[market]["source_url"] == issue["url"]:
                results[market] = {"outcome": "unchanged", "issue": issue["issue"]}
                continue
            time.sleep(PAUSE)
            spec = {"entity": market, "url": issue["url"], "format": "raw", "version": "last_modified"}
            r = bulk_file.load_file(client, bucket, SOURCE, spec, landing_root)
            results[market] = {**r, "issue": issue["issue"], "filename": issue["filename"]}
    annotate("notice", f"{SOURCE} landing extract", results)
    return 0


def _extract(p) -> int:
    return extract(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=[{"entity": m} for m in MARKETS])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
