"""QE (read-only): Eurostat table of contents rows for HICP datasets (code, last update, data start/end)."""

import urllib.request

from ingestion.common.report import annotate

TOC = "https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt?lang=en"


def main() -> int:
    req_ = urllib.request.Request(TOC, headers={"User-Agent": "zohelo-qe"})
    with urllib.request.urlopen(req_, timeout=120) as r:
        lines = r.read().decode("utf-8", "replace").splitlines()
    header = lines[0]
    rows = [" | ".join(c.strip().strip('"') for c in ln.split("\t")[1:8]) for ln in lines
            if "prc_hicp" in ln.lower() and ("rate" in ln.lower() or "annual" in ln.lower() or "monthly" in ln.lower())]
    annotate("notice", "qe toc header", header[:500])
    for i in range(0, len(rows), 12):
        annotate("notice", f"qe toc hicp {i}", "\n".join(rows[i:i + 12])[:3800])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
