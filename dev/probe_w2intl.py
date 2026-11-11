"""Read-only probe (GL-015 BIS, GL-023 ILOSTAT, GL-017 UNdata, GL-028 UNDP HDR, GL-038 UNECE, GL-006 WB GEM).

Downloads public files only; writes nothing to R2. Findings are printed to the job log.
Usage: python -m dev.probe_w2intl <group> [<group> ...]   (groups: bis ilo undata hdr unece gem, or a URL)
"""

from __future__ import annotations

import gzip
import io
import json
import sys
import zipfile

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"

GROUPS = {
    "bis": [
        "https://stats.bis.org/api/v1/dataflow/BIS/all/latest",
        "https://stats.bis.org/api/v1/data/WS_CBPOL/M.PL/all?format=csv",
        "https://stats.bis.org/api/v1/data/WS_CBPOL/M.PL/all?format=sdmx-csv",
        "https://stats.bis.org/api/v2/data/dataflow/BIS/WS_CBPOL/1.0/M.PL?format=csv",
        "https://stats.bis.org/api/v2/data/dataflow/BIS/WS_CBPOL/*/M.PL",
        "https://data.bis.org/bulkdownload",
        "https://data.bis.org/static/bulk/WS_CBPOL_csv_flat.zip",
        "https://data.bis.org/static/bulk/WS_SPP_csv_flat.zip",
        "https://data.bis.org/static/bulk/WS_TC_csv_flat.zip",
        "https://data.bis.org/static/bulk/WS_EER_csv_flat.zip",
    ],
    "ilo": [
        "https://rplumber.ilo.org/metadata/toc/indicator/?lang=en&format=.csv",
        "https://rplumber.ilo.org/files/website/bulk/indicator/UNE_DEAP_SEX_AGE_RT_A.csv.gz",
        "https://rplumber.ilo.org/files/website/bulk/indicator/UNE_DEAP_SEX_AGE_RT_Q.csv.gz",
        "https://rplumber.ilo.org/files/website/bulk/indicator/EMP_DWAP_SEX_AGE_RT_A.csv.gz",
        "https://sdmx.ilo.org/rest/dataflow/ILO/DF_UNE_DEAP_SEX_AGE_RT/latest",
        "https://sdmx.ilo.org/rest/data/ILO,DF_UNE_DEAP_SEX_AGE_RT,1.0/POL..?format=csv",
        "https://sdmx.ilo.org/rest/data/ILO,DF_UNE_DEAP_SEX_AGE_RT/POL?format=csvfile",
    ],
    "undata": [
        "https://data.un.org/ws/rest/dataflow/all/all/latest",
        "https://data.un.org/Handlers/ExplorerHandler.ashx?t=sources",
        "https://data.un.org/Handlers/DownloadHandler.ashx?DataFilter=group_code:101&DataMartId=SNAAMA&Format=csv",
        "https://data.un.org/Handlers/DownloadHandler.ashx?DataFilter=countryCode:616&DataMartId=POP&Format=csv",
        "https://data.un.org/_Docs/SYB/CSV/SYB67_1_202411_Population,%20Surface%20Area%20and%20Density.csv",
        "https://data.un.org/Default.aspx",
    ],
    "hdr": [
        "https://hdr.undp.org/data-center/documentation-and-downloads",
        "https://hdr.undp.org/sites/default/files/2025_HDR/HDR25_Composite_indices_complete_time_series.csv",
        "https://hdr.undp.org/sites/default/files/2023-24_HDR/HDR23-24_Composite_indices_complete_time_series.csv",
    ],
    "unece": [
        "https://w3.unece.org/PXWeb2015/api/v1/en/STAT",
        "https://w3.unece.org/PXWeb/api/v1/en/STAT",
        "https://w3.unece.org/PXWeb/en",
        "https://data.unece.org/",
        "https://unece.org/data",
    ],
    "ilo2": [
        "https://rplumber.ilo.org/data/indicator/?id=UNE_DEAP_SEX_AGE_RT_A&format=.csv.gz",
        "https://rplumber.ilo.org/data/indicator/?id=UNE_DEAP_SEX_AGE_RT_Q&ref_area=POL&format=.csv",
        "https://rplumber.ilo.org/files/website/bulk/indicator/UNE_DEAP_SEX_AGE_RT_Q.csv.gz",
        "https://rplumber.ilo.org/files/website/bulk/indicator.html",
        "https://sdmx.ilo.org/rest/data/ILO,DF_UNE_DEAP_SEX_AGE_RT,1.0/POL.Q....?format=csv",
        "https://sdmx.ilo.org/rest/data/ILO,DF_UNE_DEAP_SEX_AGE_RT,1.0/all?format=csv&startPeriod=2024",
        "https://sdmx.ilo.org/rest/datastructure/ILO/DSD_UNE_DEAP_SEX_AGE_RT/latest",
    ],
    "undata2": [
        "https://data.un.org/ws/rest/dataflow/all/all/latest",
    ],
    "unece2": [
        "https://w3.unece.org/PXWeb2015/api/v1/en/STAT/20-ME",
        "https://w3.unece.org/PXWeb2015/api/v1/en/STAT/30-GE",
        "https://w3.unece.org/PXWeb2015/api/v1/en/STAT/10-CountryOverviews",
    ],
    "gem": [
        "https://datacatalogapi.worldbank.org/ddhxext/DatasetView?dataset_unique_id=0037798",
        "https://databankfiles.worldbank.org/public/ddpext_download/GemDataEXTR.zip",
        "https://databank.worldbank.org/data/download/GemDataEXTR.zip",
        "https://api.worldbank.org/v2/sources/15?format=json",
        "https://api.worldbank.org/v2/sources/15/indicators?format=json&per_page=400",
    ],
}


def out(title: str, value) -> None:
    print(f"### {title}: {value}", flush=True)


def head_text(data: bytes, lines: int = 6, chars: int = 1800) -> str:
    text = data[:200000].decode("utf-8-sig", errors="replace")
    return "\n".join(text.splitlines()[:lines])[:chars]


def probe(url: str) -> None:
    try:
        r = requests.get(url, headers={"User-Agent": UA, "Accept-Encoding": "identity"}, timeout=(30, 300))
    except requests.RequestException as exc:
        out(url, f"ERROR {type(exc).__name__}: {exc}"[:400])
        return
    data = r.content
    keep = {k: r.headers.get(k) for k in ("Content-Type", "Last-Modified", "ETag", "Content-Disposition")}
    out(url, [r.status_code, len(data), keep])
    if data[:2] == b"PK":
        z = zipfile.ZipFile(io.BytesIO(data))
        out("  zip members", [(i.filename, i.file_size) for i in z.infolist()][:30])
        for info in z.infolist()[:3]:
            with z.open(info) as f:
                out(f"  head {info.filename}", "\n" + head_text(f.read(300000)))
    elif data[:2] == b"\x1f\x8b":
        raw = gzip.decompress(data)
        out("  gunzip bytes", len(raw))
        out("  head", "\n" + head_text(raw))
        lines = raw.decode("utf-8", errors="replace").splitlines()
        out("  lines", len(lines))
        out("  POL lines", [ln for ln in lines if ln.startswith('"POL"') or ln.startswith("POL")][:5])
        areas = sorted({ln.split(",")[0].strip('"') for ln in lines[1:]})
        out("  areas", areas)
    elif "xml" in (keep["Content-Type"] or ""):
        import re

        text = data.decode("utf-8", errors="replace")
        out("  xml ids", re.findall(r'(?:Dataflow|Dimension|TimeDimension)[^>]*? id="([^"]+)"', text)[:120])
        out("  xml names", re.findall(r'<common:Name xml:lang="en">([^<]+)<', text)[:120])
    elif "json" in (keep["Content-Type"] or ""):
        try:
            out("  json", json.dumps(r.json())[:6000])
        except ValueError:
            out("  text", head_text(data))
    elif "html" in (keep["Content-Type"] or ""):
        text = data.decode("utf-8", errors="replace")
        import re

        links = sorted(set(re.findall(r'href="([^"]+\.(?:csv|zip|xlsx|px|gz)[^"]*)"', text)))
        out("  links", links[:80])
        out("  title", re.findall(r"<title>(.*?)</title>", text, re.S)[:1])
    else:
        out("  head", "\n" + head_text(data, 12, 3000))


def main(argv: list[str]) -> int:
    for arg in argv:
        for url in GROUPS.get(arg, [arg]):
            probe(url)
    return 0



BIS_FULL = {
    "cbpol": "WS_CBPOL/M./all",
    "eer": "WS_EER/M.../all",
    "spp": "WS_SPP/all/all",
    "tc": "WS_TC/all/all",
    "credit_gap": "WS_CREDIT_GAP/all/all",
    "dsr": "WS_DSR/all/all",
}


def bis_full() -> None:
    import csv
    from collections import Counter

    for name, path in BIS_FULL.items():
        url = f"https://stats.bis.org/api/v1/data/{path}?format=sdmx-csv"
        r = requests.get(url, headers={"User-Agent": UA}, timeout=(30, 900))
        out(f"bis {name}", [url, r.status_code, len(r.content)])
        if r.status_code != 200:
            out(f"bis {name} text", r.text[:300])
            continue
        rows = list(csv.reader(io.StringIO(r.content.decode("utf-8-sig"), newline="")))
        h = rows[0]
        t = h.index("TIME_PERIOD")
        out(f"bis {name} header", h)
        out(f"bis {name} rows", len(rows) - 1)
        out(f"bis {name} empty time/value", sum(1 for x in rows[1:] if not x[t] or not x[t + 1]))
        out(f"bis {name} bad width", sum(1 for x in rows[1:] if len(x) != len(h)))
        out(f"bis {name} dot in dims", sum(1 for x in rows[1:] for v in x[3:t] if "." in v))
        out(f"bis {name} pipe in attrs", sum(1 for x in rows[1:] for v in x[t + 2:] if "|" in v))
        out(f"bis {name} non-numeric", Counter(x[t + 1] for x in rows[1:] if x[t + 1] and not x[t + 1].replace(".", "", 1).replace("-", "", 1).replace("E", "").isdigit()).most_common(5))
        keys = Counter(tuple(x[3:t + 1]) for x in rows[1:])
        out(f"bis {name} dup keys", sum(1 for c in keys.values() if c > 1))
        for d in h[3:t]:
            i = h.index(d)
            out(f"bis {name} {d}", dict(Counter(x[i] for x in rows[1:]).most_common(70)))
        for a in ("UNIT_MEASURE", "UNIT_MULT", "OBS_STATUS", "TIME_FORMAT"):
            if a in h:
                i = h.index(a)
                out(f"bis {name} attr {a}", dict(Counter(x[i] for x in rows[1:]).most_common(10)))
        pl = [x for x in rows[1:] if "PL" in x[3:t]]
        out(f"bis {name} PL rows", len(pl))
        ser: dict = {}
        for x in pl:
            ser.setdefault(".".join(x[3:t]), []).append(x[t])
        for k, v in list(ser.items())[:30]:
            out(f"bis {name} PL {k}", [len(v), min(v), max(v)])
        out(f"bis {name} periods", [min(x[t] for x in rows[1:] if x[t]), max(x[t] for x in rows[1:] if x[t])])


def unece_tree() -> None:
    base = "https://w3.unece.org/PXWeb2015/api/v1/en/STAT/"
    for path in ("20-ME/3-MELF", "20-ME/2-MENA", "20-ME/4-MEPI", "20-ME/1-MEOV", "30-GE/03-WorkAndeconomy", "30-GE/00-GenderOverView"):
        r = requests.get(base + path, headers={"User-Agent": UA}, timeout=(30, 120))
        out(f"unece {path}", [r.status_code, [(x.get("id"), x.get("type"), x.get("text", "").strip(), x.get("updated")) for x in (r.json() if r.ok else [])]])
        if r.ok:
            for x in r.json()[:2]:
                if x.get("type") == "t":
                    m = requests.get(base + path + "/" + x["id"], headers={"User-Agent": UA}, timeout=(30, 120))
                    out(f"unece meta {x['id']}", json.dumps(m.json())[:2500] if m.ok else m.status_code)
                    q = {"query": [{"code": v["code"], "selection": {"filter": "all", "values": ["*"]}} for v in m.json()["variables"]], "response": {"format": "csv"}}
                    d = requests.post(base + path + "/" + x["id"], json=q, headers={"User-Agent": UA}, timeout=(30, 300))
                    out(f"unece data {x['id']}", [d.status_code, len(d.content), d.headers.get("Content-Type"), d.content[:800].decode("utf-8", "replace")])


def undata_sdmx() -> None:
    base = "https://data.un.org/ws/rest/"
    for flow in ("NA_MAIN", "DF_UNDATA_COUNTRYDATA"):
        r = requests.get(base + f"dataflow/all/{flow}/latest?references=datastructure", headers={"User-Agent": UA}, timeout=(30, 120))
        import re
        out(f"undata {flow} dsd", re.findall(r'(?:Dimension|TimeDimension|Attribute|PrimaryMeasure) [^>]*?id="([^"]+)"', r.text)[:60])
        out(f"undata {flow} agency", re.findall(r'<structure:Dataflow [^>]*>', r.text)[:2])
        for q in (f"data/{flow}/all?format=csv", f"data/{flow}/all?format=csvfile"):
            d = requests.get(base + q, headers={"User-Agent": UA}, timeout=(30, 900))
            out(f"undata {q}", [d.status_code, len(d.content), d.headers.get("Content-Type"), d.content[:700].decode("utf-8", "replace")])
            if d.ok and d.content[:8] == b"DATAFLOW":
                import csv as _csv
                rows = list(_csv.reader(io.StringIO(d.content.decode("utf-8-sig"), newline="")))
                h = rows[0]
                out(f"undata {flow} rows", len(rows) - 1)
                t = h.index("TIME_PERIOD")
                out(f"undata {flow} dot/pipe", [sum(1 for x in rows[1:] for v in x[1:t] if "." in v), sum(1 for x in rows[1:] for v in x[t + 2:] if "|" in v)])
                from collections import Counter
                for dname in h[1:t]:
                    i = h.index(dname)
                    out(f"undata {flow} {dname}", dict(Counter(x[i] for x in rows[1:]).most_common(40)))
                pl = [x for x in rows[1:] if "616" in x[1:t] or "POL" in x[1:t] or "PL" in x[1:t]]
                out(f"undata {flow} PL rows", [len(pl), pl[:3]])
                break


def main2(argv: list[str]) -> int:
    main([a for a in argv if a not in ("bisfull", "unece3", "undata3")])
    if "unece3" in argv:
        unece_tree()
    if "undata3" in argv:
        undata_sdmx()
    if "bisfull" in argv:
        bis_full()
    return 0


if __name__ == "__main__":
    raise SystemExit(main2(sys.argv[1:]))
