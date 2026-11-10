"""Read-only probe (PLG-006, PLG-002/003, GL-003): what NBP and OECD serve to GitHub Actions.

Downloads public files only; writes nothing to R2. Results are annotations.
"""

from __future__ import annotations

from collections import Counter
import csv
from hashlib import sha256
import io
import re
import sys
import xml.etree.ElementTree as ET
import zipfile

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"


def get(url: str, **kw) -> requests.Response:
    return requests.get(url, headers={"User-Agent": UA, "Accept-Encoding": "identity", **kw}, timeout=(30, 300))


def head_info(r: requests.Response) -> dict:
    return {
        "status": r.status_code,
        "final_url": r.url,
        "bytes": len(r.content),
        "sha256": sha256(r.content).hexdigest()[:16],
        "content_type": r.headers.get("Content-Type"),
        "last_modified": r.headers.get("Last-Modified"),
        "etag": r.headers.get("ETag"),
        "history": [h.status_code for h in r.history],
    }


def run(title: str, fn) -> None:
    try:
        annotate("notice", title, fn())
    except Exception as exc:  # report and continue
        annotate("warning", title, {"error": f"{type(exc).__name__}: {str(exc)[:800]}"})


def nbp_xml(url: str) -> dict:
    r = get(url)
    out = head_info(r)
    out["head_text"] = r.content[:400].decode("utf-8", "replace")
    root = ET.fromstring(r.content)
    out["root"] = {"tag": root.tag, "attrib": root.attrib}
    children = list(root)
    out["children"] = len(children)
    out["child_tags"] = dict(Counter(c.tag for c in children))
    out["first_child"] = (
        {"attrib": children[0].attrib, "items": [i.attrib for i in children[0]][:8]} if children else None
    )
    out["last_child"] = (
        {"attrib": children[-1].attrib, "items": [i.attrib for i in children[-1]][:8]} if children else None
    )
    ids: Counter = Counter()
    names: dict = {}
    attrs: Counter = Counter()
    for c in children:
        attrs.update(c.attrib.keys())
        for i in c:
            ids[i.attrib.get("id")] += 1
            names.setdefault(i.attrib.get("id"), set()).add(i.attrib.get("nazwa"))
            attrs.update(f"item:{k}" for k in i.attrib)
    out["item_ids"] = dict(ids)
    out["item_names"] = {k: sorted(n for n in v if n)[:4] for k, v in names.items()}
    out["attributes"] = dict(attrs)
    dates = sorted(c.attrib.get("obowiazuje_od", "") for c in children)
    out["date_range"] = [dates[0], dates[-1]] if dates else None
    return out


def xlsx(url: str) -> dict:
    r = get(url)
    out = head_info(r)
    if r.status_code != 200:
        return out
    z = zipfile.ZipFile(io.BytesIO(r.content))
    wb = z.read("xl/workbook.xml").decode()
    out["sheets"] = re.findall(r'<sheet [^>]*name="([^"]+)"', wb)[:20]
    shared = z.read("xl/sharedStrings.xml").decode() if "xl/sharedStrings.xml" in z.namelist() else ""
    strings = re.findall(r"<t[^>]*>([^<]*)</t>", shared)
    out["shared_strings_sample"] = strings[:60]
    return out


def links(url: str, pattern: str) -> dict:
    r = get(url)
    found = sorted(set(re.findall(r'href="([^"]+)"', r.text)))
    return {"status": r.status_code, "final_url": r.url, "links": [h for h in found if re.search(pattern, h)][:60]}


def oecd_csv(url: str) -> dict:
    r = get(url, Accept="application/vnd.sdmx.data+csv; charset=utf-8")
    out = head_info(r)
    out["headers_all"] = {
        k: v
        for k, v in r.headers.items()
        if k.lower().startswith(("x-", "ratelimit", "retry", "cache", "last", "etag"))
    }
    if r.status_code != 200:
        out["text"] = r.text[:600]
        return out
    rows = list(csv.DictReader(io.StringIO(r.content.decode("utf-8-sig"))))
    out["columns"] = list(rows[0].keys()) if rows else []
    out["rows"] = len(rows)
    for col in (
        "REF_AREA",
        "FREQ",
        "MEASURE",
        "UNIT_MEASURE",
        "ACTIVITY",
        "ADJUSTMENT",
        "TRANSFORMATION",
        "TIME_HORIZ",
        "METHODOLOGY",
        "OBS_STATUS",
    ):
        if rows and col in rows[0]:
            values = Counter(x[col] for x in rows)
            out[col] = dict(values.most_common(70)) if col != "REF_AREA" else sorted(values)
    pol = [x for x in rows if x.get("REF_AREA") == "POL"]
    if pol:
        series = Counter(
            (
                x.get("MEASURE"),
                x.get("UNIT_MEASURE"),
                x.get("ADJUSTMENT"),
                x.get("TRANSFORMATION"),
                x.get("METHODOLOGY"),
            )
            for x in pol
        )
        out["pol_series"] = [list(k) + [v] for k, v in series.items()]
        periods = sorted(x["TIME_PERIOD"] for x in pol)
        out["pol_range"] = [periods[0], periods[-1]]
        out["pol_last"] = sorted(pol, key=lambda x: x["TIME_PERIOD"])[-3:]
    out["sample"] = rows[:2]
    return out


def oecd_structure(url: str) -> dict:
    r = get(url, Accept="application/xml")
    out = head_info(r)
    text = r.text
    out["dims"] = re.findall(r'<structure:Dimension [^>]*id="([^"]+)"', text)[:20]
    out["names"] = re.findall(r'<common:Name xml:lang="en">([^<]+)</common:Name>', text)[:15]
    out["version"] = re.findall(r'<structure:Dataflow [^>]*version="([^"]+)"', text)[:3]
    return out


def main() -> int:
    part = sys.argv[1] if len(sys.argv) > 1 else "all"
    if part in ("all", "nbp"):
        run("nbp rates archive xml", lambda: nbp_xml("https://static.nbp.pl/dane/stopy/stopy_procentowe_archiwum.xml"))
        run("nbp rates current xml", lambda: nbp_xml("https://static.nbp.pl/dane/stopy/stopy_procentowe.xml"))
        run("nbp rates old url", lambda: head_info(get("http://www.nbp.pl/xml/stopy_procentowe_archiwum.xml")))
        run(
            "nbp rates page",
            lambda: links(
                "https://nbp.pl/en/monetary-policy/mpc-decisions/basic-interest-rates/", r"stopy|\.xml|\.xls"
            ),
        )
    if part in ("all", "bop"):
        run("nbp bop_m", lambda: xlsx("https://static.nbp.pl/dane/bilans-platniczy/bop_m.xlsx"))
        run("nbp bop_q", lambda: xlsx("https://static.nbp.pl/dane/bilans-platniczy/bop_q.xlsx"))
        run(
            "nbp bop page",
            lambda: links(
                "https://nbp.pl/en/statistic-and-financial-reporting/balance-of-payments-statistics/",
                r"static\.nbp|\.xls|investment",
            ),
        )
        run(
            "nbp iip page",
            lambda: links(
                "https://nbp.pl/en/statistic-and-financial-reporting/balance-of-payments-statistics/international-investment-position/",
                r"static\.nbp|\.xls",
            ),
        )
    if part in ("all", "oecd"):
        base = "https://sdmx.oecd.org/public/rest"
        run(
            "oecd cli structure",
            lambda: oecd_structure(f"{base}/dataflow/OECD.SDD.STES/DSD_STES@DF_CLI/latest?references=datastructure"),
        )
        run("oecd cli all csv", lambda: oecd_csv(f"{base}/data/OECD.SDD.STES,DSD_STES@DF_CLI,/all?format=csvfile"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
