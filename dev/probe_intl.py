"""Read-only probe (GL-016 ECB, GL-039 UN WPP, GL-021 UNCTAD): file structure for Bronze/Silver design.

Downloads public files only; writes nothing to R2. Findings are printed to the job log.
"""

from __future__ import annotations

from collections import Counter
import csv
import gzip
import io
from pathlib import Path
import subprocess
import sys
import tempfile

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
ECB = "https://data-api.ecb.europa.eu/service/data/"
EU = {"AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI", "FR", "GR", "HR", "HU", "IE", "IT", "LT",
      "LU", "LV", "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK"}


def get(url: str) -> requests.Response:
    return requests.get(url, headers={"User-Agent": UA, "Accept-Encoding": "identity"}, timeout=(30, 600))


def out(title: str, value) -> None:
    print(f"### {title}: {value}", flush=True)


def ecb(name: str, query: str, dims_of_interest: list[str]) -> None:
    r = get(ECB + query)
    out(f"ecb {name} status", [r.status_code, len(r.content), r.headers.get("Last-Modified")])
    if r.status_code != 200:
        out(f"ecb {name} text", r.text[:500])
        return
    text = r.content.decode("utf-8-sig")
    rows = list(csv.reader(io.StringIO(text, newline="")))
    header = rows[0]
    out(f"ecb {name} header", header)
    out(f"ecb {name} rows", len(rows) - 1)
    out(f"ecb {name} first rows", rows[1:3])
    out(f"ecb {name} pipe in any field", sum(1 for row in rows[1:] for v in row if "|" in v))
    out(f"ecb {name} dot in dim codes", sum(1 for row in rows[1:] for v in row[1:header.index("TIME_PERIOD")] if "." in v))
    for d in dims_of_interest:
        if d in header:
            i = header.index(d)
            out(f"ecb {name} {d}", dict(Counter(row[i] for row in rows[1:]).most_common(80)))
    k, t, v = header.index("KEY"), header.index("TIME_PERIOD"), header.index("OBS_VALUE")
    series: dict[str, list] = {}
    for row in rows[1:]:
        series.setdefault(row[k], []).append((row[t], row[v]))
    out(f"ecb {name} series", len(series))
    for key, obs in list(series.items())[:400]:
        if any(f".{c}." in key for c in ("PL", "U2", "I9", "B0", "D0", "PLN", "EU", "I8", "4F", "DE")):
            obs.sort()
            out(f"ecb {name} series {key}", [len(obs), obs[0], obs[-1]])
    for col in ("UNIT", "UNIT_MULT", "DECIMALS", "TITLE", "OBS_STATUS", "TIME_FORMAT"):
        if col in header:
            i = header.index(col)
            out(f"ecb {name} attr {col}", dict(Counter(row[i] for row in rows[1:]).most_common(8)))


def wpp(name: str, url: str) -> None:
    r = get(url)
    out(f"wpp {name} status", [r.status_code, len(r.content), r.headers.get("Last-Modified")])
    if r.status_code != 200:
        return
    text = gzip.decompress(r.content).decode("utf-8-sig")
    out(f"wpp {name} uncompressed bytes", len(text))
    reader = csv.reader(io.StringIO(text, newline=""))
    header = next(reader)
    rows = list(reader)
    out(f"wpp {name} header", header)
    out(f"wpp {name} rows", len(rows))
    out(f"wpp {name} pipe in any field", sum(1 for row in rows for v in row if "|" in v))
    out(f"wpp {name} widths", dict(Counter(len(r_) for r_ in rows)))
    h = {c: i for i, c in enumerate(header)}
    for c in ("LocTypeName", "Variant", "VarID", "AgeGrp", "AgeGrpStart"):
        if c in h:
            out(f"wpp {name} {c}", dict(Counter(row[h[c]] for row in rows).most_common(40)))
    if "Time" in h:
        times = sorted({int(row[h["Time"]]) for row in rows})
        out(f"wpp {name} years", [times[0], times[-1], len(times)])
    locs = Counter((row[h["LocID"]], row[h["Location"]], row[h["ISO2_code"]], row[h["ISO3_code"]],
                    row[h.get("SDMX_code", 0)], row[h["LocTypeName"]]) for row in rows)
    out(f"wpp {name} locations", len(locs))
    out(f"wpp {name} europe-ish", [k for k in locs if "Europe" in k[1] or "EU" in k[1] or k[2] in EU][:60])
    out(f"wpp {name} non-country locations", sorted({k[1] for k in locs if not k[2]})[:120])
    pl = [row for row in rows if row[h["ISO2_code"]] == "PL"]
    out(f"wpp {name} PL rows", len(pl))
    for row in pl:
        if row[h["Time"]] in ("1950", "2023", "2024", "2025", "2100"):
            out(f"wpp {name} PL {row[h['Time']]}", dict(zip(header, row)))
            if name == "age5":
                break
    empties = Counter(header[i] for row in rows for i, v in enumerate(row) if v == "")
    out(f"wpp {name} empty cells per column", dict(empties.most_common(70)))


def unctad(code: str) -> None:
    url = f"https://unctadstat-api.unctad.org/bulkdownload/{code.replace('_', '.', 1)}/{code}"
    r = get(url)
    out(f"unctad {code} status", [r.status_code, len(r.content), r.headers.get("Content-Type")])
    if r.status_code != 200:
        return
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "a.7z"
        path.write_bytes(r.content)
        listing = subprocess.run(["7z", "l", "-slt", str(path)], capture_output=True, text=True).stdout
        out(f"unctad {code} members", [ln for ln in listing.splitlines() if ln.startswith(("Path", "Size"))][:12])
        body = subprocess.run(["7z", "x", "-so", str(path)], capture_output=True).stdout
    out(f"unctad {code} uncompressed", len(body))
    out(f"unctad {code} head bytes", body[:300])
    text = body.decode("utf-8-sig", "replace")
    reader = csv.reader(io.StringIO(text, newline=""))
    header = next(reader)
    rows = list(reader)
    out(f"unctad {code} header", header)
    out(f"unctad {code} rows", len(rows))
    out(f"unctad {code} widths", dict(Counter(len(r_) for r_ in rows)))
    out(f"unctad {code} pipe in any field", sum(1 for row in rows for v in row if "|" in v))
    out(f"unctad {code} first rows", rows[:3])
    h = {c: i for i, c in enumerate(header)}
    for c in header:
        values = Counter(row[h[c]] for row in rows if len(row) == len(header))
        if len(values) <= 60:
            out(f"unctad {code} col {c}", dict(values.most_common(60)))
        else:
            out(f"unctad {code} col {c} distinct", [len(values), list(values)[:8]])
    econ = next((c for c in header if c.lower().startswith("economy") and "label" in c.lower()), None)
    econ_code = next((c for c in header if c.lower() == "economy"), None)
    if econ and econ_code:
        econs = Counter((row[h[econ_code]], row[h[econ]]) for row in rows if len(row) == len(header))
        out(f"unctad {code} economies", len(econs))
        out(f"unctad {code} europe-ish", [k for k in econs if any(s in k[1] for s in ("Europ", "Poland", "Germany", "Greece", "Czech"))][:40])
        out(f"unctad {code} poland rows", [row for row in rows if len(row) == len(header) and row[h[econ]] == "Poland"][-6:])


def main() -> int:
    part = sys.argv[1] if len(sys.argv) > 1 else "all"
    if part in ("all", "ecb"):
        for name, query, dims in (
            ("icp_annual_rate", "ICP/M..N.000000.4.ANR?format=csvdata", ["REF_AREA"]),
            ("fm_key_rates", "FM/B.U2.EUR.4F.KR..LEV?format=csvdata", ["PROVIDER_FM_ID"]),
            ("est_estr", "EST/B.EU000A2X2A25.WT?format=csvdata", []),
            ("irs_long_term", "IRS/M..L.L40.CI.0000..N.Z?format=csvdata", ["REF_AREA", "CURRENCY"]),
            ("bsi_monetary_aggregates", "BSI/M.U2.Y.V.M10+M20+M30.X.1+I.U2.2300.Z01.E+A?format=csvdata",
             ["BS_ITEM", "DATA_TYPE", "BS_SUFFIX"]),
            ("exr_daily", "EXR/D..EUR.SP00.A?format=csvdata", ["CURRENCY"]),
        ):
            try:
                ecb(name, query, dims)
            except Exception as exc:
                out(f"ecb {name} error", repr(exc)[:600])
    if part in ("all", "wpp"):
        base = "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/"
        for name, file in (("demographic", "WPP2024_Demographic_Indicators_Medium.csv.gz"),
                           ("age5", "WPP2024_PopulationByAge5GroupSex_Medium.csv.gz")):
            try:
                wpp(name, base + file)
            except Exception as exc:
                out(f"wpp {name} error", repr(exc)[:600])
    if part == "more":
        for name, query, dims in (
            ("icp_items_u2", "ICP/M.U2.N..4.ANR?lastNObservations=2&format=csvdata", ["ICP_ITEM"]),
            ("icp_items_pl", "ICP/M.PL.N..4.ANR?lastNObservations=2&format=csvdata", ["ICP_ITEM"]),
            ("icp_all_xtotal", "ICP/M..N.TOTAL.4.ANR?format=csvdata", ["REF_AREA"]),
        ):
            try:
                ecb(name, query, dims)
            except Exception as exc:
                out(f"ecb {name} error", repr(exc)[:600])
        for code in ("US_DigitallyDeliverableServices", "US_CommodityPrice_M", "US_PLSCI", "US_MerchantFleet",
                     "US_SeaborneTrade", "US_PortCalls"):
            try:
                unctad(code)
            except Exception as exc:
                out(f"unctad {code} error", repr(exc)[:600])
    if part in ("all", "unctad"):
        for code in ("US_FdiFlowsStock", "US_TradeMerchTotal", "US_TradeServCatTotal"):
            try:
                unctad(code)
            except Exception as exc:
                out(f"unctad {code} error", repr(exc)[:600])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
