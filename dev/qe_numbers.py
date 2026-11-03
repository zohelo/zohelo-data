"""QE sprint check #2507 (read-only): published latest.json values for PL vs the providers' own APIs."""

import json
import urllib.request

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate

PREFIX = "06_published/v1/"
KPIS = ["gdp_per_capita", "gdp_volume_growth", "unemployment_rate"]
EUROSTAT = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
PROVIDER = {
    ("gdp_per_capita", "eurostat"): EUROSTAT + "nama_10_pc?geo=PL&unit=CP_EUR_HAB&na_item=B1GQ&sinceTimePeriod=2021",
    ("gdp_volume_growth", "eurostat"): EUROSTAT + "nama_10_gdp?geo=PL&unit=CLV_PCH_PRE&na_item=B1GQ&sinceTimePeriod=2021",
    ("unemployment_rate", "eurostat"): EUROSTAT + "une_rt_a?geo=PL&unit=PC_ACT&age=Y15-74&sex=T&sinceTimePeriod=2021",
    ("gdp_per_capita", "world_bank_wdi"): "https://api.worldbank.org/v2/country/PL/indicator/NY.GDP.PCAP.CD?format=json&date=2021:2025",
    ("gdp_volume_growth", "world_bank_wdi"): "https://api.worldbank.org/v2/country/PL/indicator/NY.GDP.MKTP.KD.ZG?format=json&date=2021:2025",
    ("unemployment_rate", "world_bank_wdi"): "https://api.worldbank.org/v2/country/PL/indicator/SL.UEM.TOTL.ZS?format=json&date=2021:2025",
}


def fetch(url: str):
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "zohelo-qe"}), timeout=60) as r:
        return json.loads(r.read())


def provider(url: str) -> dict:
    data = fetch(url)
    if isinstance(data, list):
        return {row["date"]: row["value"] for row in data[1] or []}
    index = data["dimension"]["time"]["category"]["index"]
    pos = {v: k for k, v in index.items()}
    return {pos[int(i)]: v for i, v in data["value"].items()} | {"_updated": data.get("updated")}


def main() -> int:
    client, bucket = s3(), req("STORAGE_LAKEHOUSE_BUCKET_NAME")
    for kpi in KPIS:
        body = client.get_object(Bucket=bucket, Key=f"{PREFIX}kpi/{kpi}/latest.json")["Body"].read()
        latest = json.loads(body)
        rows = [v for v in latest["values"] if v["geography_key"] == "PL" and v["geography_level"] == "country"]
        out = {"generated_at": latest.get("generated_at"), "unit_key": latest.get("unit_key"), "rows": rows}
        for row in rows:
            url = PROVIDER.get((kpi, row["source_key"]))
            if url:
                try:
                    out.setdefault("provider", {})[row["source_key"]] = provider(url)
                except Exception as e:  # report, do not fail the check
                    out.setdefault("provider", {})[row["source_key"]] = f"error: {e}"
        annotate("notice", f"qe {kpi}", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
