"""Dev probe for #2397 (not for main, writes nothing): UNCTAD Data Hub catalogue and bulk files."""

from __future__ import annotations

import re
import subprocess
import tempfile

import requests

from ingestion.common.report import annotate

UA = {"User-Agent": "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"}
API = "https://unctadstat-api.unctad.org"
CATALOGUE = [f"{API}/datamart-api", f"{API}/datamart-api/reports", f"{API}/datamart-api/api/reports",
             f"{API}/datamart-api/Reports", f"{API}/bulkdownload", f"{API}/swagger/index.html",
             f"{API}/datamart-api/swagger/index.html", "https://unctadstat.unctad.org/datacentre/sitemap.xml",
             "https://unctadstat.unctad.org/sitemap.xml"]
CODES = ["US.GDPTotal", "US.FdiFlowsStock", "US.TradeMerchTotal", "US.PLSCI", "US.TradeServCatTotal",
         "US.PopTotal", "US.Cpi_A", "US.ExchangeRateCrosstab", "US.CommodityPrice_M", "US.TradeMerchGR",
         "US.DigitallyDeliverableServices", "US.ICTGoodsValue", "US.CreativeGoodsValue", "US.MerchantFleet",
         "US.PortCalls", "US.SeaborneTrade", "US.TradeServCatByPartner", "US.BalanceOfPayments"]


def main() -> int:
    """Annotate catalogue endpoints (status, head, report codes found) and each candidate bulk file."""
    cat = []
    found: set[str] = set()
    for url in CATALOGUE:
        try:
            r = requests.get(url, headers=UA, timeout=60)
            codes = set(re.findall(r"US\.[A-Za-z_0-9]{3,40}", r.text))
            found |= codes
            cat.append({"url": url[-45:], "status": r.status_code, "bytes": len(r.content),
                        "type": r.headers.get("Content-Type"), "codes": len(codes), "head": r.text[:200]})
        except requests.RequestException as exc:
            cat.append({"url": url[-45:], "error": str(exc)[:120]})
    for i in range(0, len(cat), 3):
        annotate("notice", f"unctad catalogue {i // 3 + 1}", cat[i : i + 3])
    annotate("notice", "unctad codes found", sorted(found)[:300])
    files = []
    for code in CODES:
        url = f"{API}/bulkdownload/{code}/{code.replace('.', '_')}"
        try:
            r = requests.get(url, headers=UA, timeout=180)
            item = {"code": code, "status": r.status_code, "bytes": len(r.content),
                    "cd": r.headers.get("Content-Disposition"), "etag": r.headers.get("ETag")}
            if r.content[:2] == b"7z":
                with tempfile.NamedTemporaryFile(suffix=".7z") as f:
                    f.write(r.content)
                    f.flush()
                    listing = subprocess.run(["7z", "l", "-slt", f.name], capture_output=True, text=True, timeout=60)
                    item["members"] = re.findall(r"^(?:Path|Size|Modified) = (.+)$", listing.stdout, re.M)[2:8]
            files.append(item)
        except requests.RequestException as exc:
            files.append({"code": code, "error": str(exc)[:120]})
    for i in range(0, len(files), 3):
        annotate("notice", f"unctad files {i // 3 + 1}", files[i : i + 3])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
