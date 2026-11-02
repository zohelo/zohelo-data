"""QE read-only check: published latest values, the live zohelo.com home, and the providers' own values.

Reads 06_published/v1 (no writes anywhere), fetches zohelo.com pages and public provider APIs, and
publishes the results as annotations. Branch script; deleted with the branch.
"""

from __future__ import annotations

import html
import json
import re

import requests

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate

PREFIX = "06_published/v1/"
UA = {"User-Agent": "zohelo-qe-check/1.0 (+https://zohelo.com)"}


def published() -> dict:
    client = s3()
    bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME")

    def read(key: str):
        return json.loads(client.get_object(Bucket=bucket, Key=PREFIX + key)["Body"].read())

    catalog = read("catalog/catalog.json")
    out = {"generated_at": catalog.get("generated_at"), "kpis": []}
    for kpi in catalog["kpis"]:
        row = {"k": kpi["kpi_key"], "st": kpi["status"], "u": kpi["unit_key"], "last": kpi["last_period"]}
        if kpi["status"] == "published":
            try:
                latest = read(f"kpi/{kpi['kpi_key']}/latest.json")
                row["v"] = [
                    [v["source_key"], v["period"], v["value"], v.get("previous_period"), v.get("previous_value")]
                    for v in latest["values"]
                    if v["geography_key"] == "PL"
                ]
            except Exception as exc:  # noqa: BLE001 - report and go on
                row["err"] = str(exc)[:120]
        out["kpis"].append(row)
    try:
        feed = read("feed/changes.json")
        out["feed"] = json.dumps(feed, default=str)[:400]
    except Exception as exc:  # noqa: BLE001
        out["feed"] = f"missing: {str(exc)[:80]}"
    return out


def page_text(url: str) -> str:
    r = requests.get(url, headers=UA, timeout=30)
    t = re.sub(r"<script.*?</script>|<style.*?</style>", " ", r.text, flags=re.S)
    t = html.unescape(re.sub(r"<[^>]+>", " ", t))
    return f"HTTP {r.status_code} " + re.sub(r"\s+", " ", t)


def snippet(text: str, start: str, length: int) -> str:
    i = text.find(start)
    return text[i : i + length] if i >= 0 else text[:length]


def eurostat(dataset: str, params: str) -> list:
    url = (
        f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset}"
        f"?format=JSON&lang=EN&geo=PL&lastTimePeriod=3&{params}"
    )
    d = requests.get(url, headers=UA, timeout=60).json()
    idx = d["dimension"]["time"]["category"]["index"]
    by = {pos: t for t, pos in idx.items()}
    return [[by[int(k)], v] for k, v in sorted(d["value"].items(), key=lambda kv: int(kv[0]))] + [d.get("updated")]


def providers() -> dict:
    out: dict = {}
    checks = {
        "hicp_manr_CP00": ("prc_hicp_manr", "coicop=CP00&unit=RCH_A"),
        "une_rt_m_SA_T": ("une_rt_m", "s_adj=SA&age=TOTAL&sex=T&unit=PC_ACT"),
        "une_rt_a_Y15-74": ("une_rt_a", "age=Y15-74&sex=T&unit=PC_ACT"),
        "gdp_CP_MEUR": ("nama_10_gdp", "na_item=B1GQ&unit=CP_MEUR"),
        "gdp_CLV_PCH_PRE": ("nama_10_gdp", "na_item=B1GQ&unit=CLV_PCH_PRE"),
        "gdp_pc_CP_EUR_HAB": ("nama_10_pc", "na_item=B1GQ&unit=CP_EUR_HAB"),
    }
    for name, (ds, p) in checks.items():
        try:
            out[name] = eurostat(ds, p)
        except Exception as exc:  # noqa: BLE001
            out[name] = f"error {str(exc)[:80]}"
    try:
        r = requests.get("https://api.nbp.pl/api/exchangerates/rates/a/eur/last/2/?format=json", headers=UA, timeout=30)
        out["nbp_eur"] = [[x["effectiveDate"], x["mid"]] for x in r.json()["rates"]]
    except Exception as exc:  # noqa: BLE001
        out["nbp_eur"] = f"error {str(exc)[:80]}"
    for ind in ("SP.POP.TOTL", "FP.CPI.TOTL.ZG", "NY.GDP.PCAP.CD"):
        try:
            r = requests.get(f"https://api.worldbank.org/v2/country/PL/indicator/{ind}?format=json&mrnev=2", headers=UA, timeout=30)
            out["wb_" + ind] = [[x["date"], x["value"]] for x in r.json()[1]]
        except Exception as exc:  # noqa: BLE001
            out["wb_" + ind] = f"error {str(exc)[:80]}"
    return out


def main() -> None:
    try:
        pub = published()
        annotate("notice", "qe published", pub)
    except Exception as exc:  # noqa: BLE001
        annotate("error", "qe published", str(exc)[:300])
    for name, url, start in (
        ("qe home pl", "https://zohelo.com/", "Dziś w danych"),
        ("qe home en", "https://zohelo.com/en/", "Today in data"),
        ("qe inflation pl", "https://zohelo.com/indicators/inflation", "Inflacja"),
        ("qe gdp pl", "https://zohelo.com/indicators/gdp_current_prices", "PKB"),
        ("qe changes pl", "https://zohelo.com/changes", "Co się zmieniło"),
    ):
        try:
            annotate("notice", name, snippet(page_text(url), start, 1500))
        except Exception as exc:  # noqa: BLE001
            annotate("warning", name, str(exc)[:300])
    annotate("notice", "qe providers", providers())


if __name__ == "__main__":
    main()
