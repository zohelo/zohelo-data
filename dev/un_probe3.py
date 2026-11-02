"""Dev probe 3 for #2394 (not for main, writes nothing): full-dataflow size and CSV options on UNdata legacy SDMX REST."""

from __future__ import annotations

import time

import requests

from ingestion.common.report import annotate

UA = {"User-Agent": "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"}
BASE = "https://data.un.org/legacy/ws/rest/data/"
CASES = [
    ("NA_MAIN/all?format=csv", {}),
    ("NA_MAIN/all?format=csvfile", {}),
    ("NA_MAIN/all", {"Accept": "text/csv"}),
    ("DF_UNDATA_ENERGY/all", {"Accept": "text/csv"}),
    ("DF_UNData_EnergyBalance/all", {"Accept": "text/csv"}),
    ("DF_SEEA_AEA/all", {"Accept": "text/csv"}),
    ("DF_SEEA_ENERGY/all", {"Accept": "text/csv"}),
    ("NASEC_IDCNFSA_A/all", {"Accept": "text/csv"}),
]
BUDGET = 150  # seconds per case


def main() -> int:
    """Stream each full dataflow for at most BUDGET seconds; annotate status, type, bytes, rows, completeness."""
    out = []
    for path, hdr in CASES:
        start = time.monotonic()
        try:
            with requests.get(BASE + path, headers={**UA, **hdr}, timeout=(30, 120), stream=True) as r:
                size, lines, done, head = 0, 0, True, b""
                for chunk in r.iter_content(1 << 16):
                    if not head:
                        head = chunk[:160]
                    size += len(chunk)
                    lines += chunk.count(b"\n")
                    if time.monotonic() - start > BUDGET:
                        done = False
                        break
                out.append({"path": path, "accept": hdr.get("Accept"), "status": r.status_code,
                            "type": r.headers.get("Content-Type"), "lm": r.headers.get("Last-Modified"),
                            "bytes": size, "lines": lines, "complete": done,
                            "seconds": round(time.monotonic() - start), "head": head.decode("utf-8", "replace")})
        except requests.RequestException as exc:
            out.append({"path": path, "error": f"{type(exc).__name__}: {str(exc)[:150]}",
                        "seconds": round(time.monotonic() - start)})
    for i in range(0, len(out), 2):
        annotate("notice", f"undata full {i // 2 + 1}", out[i : i + 2])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
