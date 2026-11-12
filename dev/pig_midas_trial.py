"""Dev trial (PL-ENV-015 PIG-PIB MIDAS, #2870).

Mode ``probe`` (read-only, no R2 writes): robots.txt, WFS capabilities (versions, output formats, paging
constraints, CRS), DescribeFeatureType and hits per layer, byte stability of two identical small GetFeature
requests per version (an embedded timeStamp changes the bytes every request), one full GetFeature per layer with
an attribute profile. One request at a time with pauses. At most 10 annotations.
"""

from __future__ import annotations

from collections import Counter
import json
from pathlib import Path
import re
import sys
import tempfile
import time
from urllib.parse import quote
import xml.etree.ElementTree as ET

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

HOST = "https://cbdgmapa.pgi.gov.pl"
WFS = f"{HOST}/arcgis/services/midas/MapServer/WFSServer"
UA = {"User-Agent": bulk_file.USER_AGENT}
PAUSE = 4


def get(url: str, timeout: int = 120) -> requests.Response:
    time.sleep(PAUSE)
    return requests.get(url, headers={**UA, "Accept-Encoding": "identity"}, timeout=(30, timeout))


def local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def capabilities() -> tuple[dict, list[str]]:
    out: dict = {}
    r = get(f"{HOST}/robots.txt", 30)
    out["robots"] = {"status": r.status_code, "ct": r.headers.get("Content-Type"), "body": r.text[:400]}
    types: list[str] = []
    for version in ("2.0.0", "1.1.0"):
        r = get(f"{WFS}?service=WFS&request=GetCapabilities&version={version}")
        info: dict = {"status": r.status_code, "bytes": len(r.content), "server": r.headers.get("Server")}
        if r.status_code == 200:
            root = ET.fromstring(r.content)
            info["root_version"] = root.attrib.get("version")
            ops = {}
            for op in root.iter():
                if local(op.tag) == "Operation":
                    params = {}
                    for p in op:
                        if local(p.tag) in ("Parameter", "Constraint"):
                            vals = [v.text for v in p.iter() if local(v.tag) in ("Value", "DefaultValue") and v.text]
                            params[p.attrib.get("name")] = vals[:12]
                    ops[op.attrib.get("name")] = params
            info["getfeature"] = ops.get("GetFeature")
            info["global_constraints"] = {
                c.attrib.get("name"): [v.text for v in c.iter() if local(v.tag) in ("Value", "DefaultValue")][:3]
                for c in root.iter()
                if local(c.tag) == "Constraint"
                and c.attrib.get("name")
                in ("ImplementsResultPaging", "CountDefault", "ImplementsStandardJoins", "KVPEncoding")
            }
            fts = []
            for ft in root.iter():
                if local(ft.tag) == "FeatureType":
                    d = {
                        local(c.tag): (c.text or "").strip()[:120]
                        for c in ft
                        if local(c.tag) in ("Name", "Title", "DefaultCRS", "DefaultSRS", "OtherCRS", "OtherSRS")
                    }
                    fmts = [f.text for f in ft.iter() if local(f.tag) == "Format"]
                    if fmts:
                        d["formats"] = fmts[:6]
                    fts.append(d)
            info["feature_types"] = fts
            if version == "2.0.0":
                types = [f.get("Name") for f in fts]
                info["fees_access"] = [
                    (local(e.tag), (e.text or "")[:300])
                    for e in root.iter()
                    if local(e.tag) in ("Fees", "AccessConstraints")
                ]
        out[version] = info
    return out, types


def feature_rows(path: Path) -> tuple[dict, list[dict], list[str]]:
    """Root attributes, per feature {property: text or geometry tag}, member element names."""
    root_attrs: dict = {}
    rows: list[dict] = []
    members: Counter = Counter()
    depth = 0
    for event, el in ET.iterparse(str(path), events=("start", "end")):
        if event == "start":
            depth += 1
            if depth == 1:
                root_attrs = {local(k): v for k, v in el.attrib.items()}
            continue
        depth -= 1
        if depth == 1 and local(el.tag) in ("member", "featureMember", "featureMembers"):
            members[local(el.tag)] += 1
            for feat in el:
                row = {
                    "@id": feat.attrib.get("{http://www.opengis.net/gml/3.2}id")
                    or feat.attrib.get("{http://www.opengis.net/gml}id")
                }
                for prop in feat:
                    kids = list(prop)
                    row[local(prop.tag)] = f"<{local(kids[0].tag)}>" if kids else prop.text
                rows.append(row)
            el.clear()
    return root_attrs, rows, list(members)


def profile(rows: list[dict]) -> list:
    cols: dict[str, list] = {}
    for row in rows:
        for k, v in row.items():
            cols.setdefault(k, []).append(v)
    out = []
    for k, vals in cols.items():
        nonnull = [v for v in vals if v not in (None, "")]
        distinct = Counter(nonnull)
        shape = Counter(
            "int"
            if re.fullmatch(r"-?\d+", v)
            else "num"
            if re.fullmatch(r"-?\d+[.,]\d+", v)
            else "date"
            if re.match(r"\d{4}-\d\d-\d\d", v)
            else "geom"
            if v.startswith("<")
            else "text"
            for v in nonnull[:5000]
        )
        out.append(
            [k, len(vals) - len(nonnull), len(distinct), dict(shape), [str(s)[:24] for s, _ in distinct.most_common(3)]]
        )
    return out


def probe() -> None:
    caps, types = capabilities()
    print(json.dumps(caps, ensure_ascii=False), flush=True)
    annotate("notice", "midas probe: capabilities 2.0.0", caps.get("2.0.0"))
    annotate(
        "notice", "midas probe: robots, capabilities 1.1.0", {"robots": caps["robots"], "1.1.0": caps.get("1.1.0")}
    )
    stability = {}
    per_layer = {}
    for n, name in enumerate(types):
        tn = quote(name, safe=":")
        info: dict = {}
        r = get(f"{WFS}?service=WFS&version=2.0.0&request=DescribeFeatureType&typeNames={tn}")
        if r.status_code == 200:
            try:
                root = ET.fromstring(r.content)
                info["describe"] = [
                    [e.attrib.get("name"), e.attrib.get("type"), e.attrib.get("minOccurs"), e.attrib.get("nillable")]
                    for e in root.iter()
                    if local(e.tag) == "element" and e.attrib.get("type")
                ][:60]
            except ET.ParseError as exc:
                info["describe_error"] = str(exc)[:200]
        else:
            info["describe_status"] = r.status_code
        r = get(f"{WFS}?service=WFS&version=2.0.0&request=GetFeature&typeNames={tn}&resultType=hits")
        info["hits"] = {"status": r.status_code, "head": r.text[:300]}
        if n < 2:  # byte stability of identical requests, both versions, small counts
            for version, cnt in (("2.0.0", "count=3"), ("1.1.0", "maxFeatures=3")):
                key = "typeNames" if version == "2.0.0" else "typeName"
                url = f"{WFS}?service=WFS&version={version}&request=GetFeature&{key}={tn}&{cnt}"
                a, b = get(url), get(url)
                ts = re.findall(rb'timeStamp="[^"]+"', a.content[:2000])
                stability[f"{n}:{version}"] = {
                    "status": [a.status_code, b.status_code],
                    "same_bytes": a.content == b.content,
                    "bytes": len(a.content),
                    "timestamp_attr": [t.decode() for t in ts],
                    "ct": a.headers.get("Content-Type"),
                    "last_modified": a.headers.get("Last-Modified"),
                    "etag": a.headers.get("ETag"),
                    "head": a.text[:350],
                }
            url = f"{WFS}?service=WFS&version=2.0.0&request=GetFeature&typeNames={tn}&count=3&outputFormat=GEOJSON"
            g = get(url)
            stability[f"{n}:geojson"] = {
                "status": g.status_code,
                "ct": g.headers.get("Content-Type"),
                "head": g.text[:200],
            }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "layer.xml"
            url = f"{WFS}?service=WFS&version=2.0.0&request=GetFeature&typeNames={tn}"
            started = time.monotonic()
            time.sleep(PAUSE)
            try:
                digest, size, headers, _final, _d = bulk_file.download(url, path, "raw", retries=2, read_timeout=600)
            except Exception as exc:
                info["full_error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
                per_layer[name] = info
                continue
            info["full"] = {
                "bytes": size,
                "seconds": round(time.monotonic() - started),
                "sha": digest[:12],
                "ct": headers.get("Content-Type"),
                "head": path.read_bytes()[:300].decode("utf-8", "replace"),
            }
            try:
                root_attrs, rows, members = feature_rows(path)
            except ET.ParseError as exc:
                info["parse_error"] = str(exc)[:300]
                per_layer[name] = info
                continue
            info["root"] = root_attrs
            info["members"] = members
            info["features"] = len(rows)
            ids = Counter(r.get("@id") for r in rows)
            info["dup_gml_ids"] = sum(1 for c in ids.values() if c > 1)
            info["profile"] = profile(rows)
        per_layer[name] = info
        print(json.dumps({"layer": name, **info}, ensure_ascii=False), flush=True)
        brief = {k: v for k, v in info.items() if k != "describe" or "profile" not in info}
        if "full" in brief:
            brief["full"] = {k: v for k, v in brief["full"].items() if k != "head"}
        annotate("notice", f"midas probe layer {n}", {"type": name, **brief})
    annotate("notice", "midas probe: byte stability", stability)
    print(json.dumps({"layers": list(per_layer)}, ensure_ascii=False))


def probe2() -> None:
    """Attribute profile of every layer from one GML32+GZIP GetFeature each; key uniqueness and number formats."""
    import gzip

    _caps, types = capabilities()
    for n, name in enumerate(types):
        tn = quote(name, safe=":")
        info: dict = {}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "layer.gz"
            url = f"{WFS}?service=WFS&version=2.0.0&request=GetFeature&typeNames={tn}&outputFormat=GML32%2BGZIP"
            started = time.monotonic()
            time.sleep(PAUSE)
            try:
                _sha, size, headers, _f, _d = bulk_file.download(url, path, "gzip", retries=2, read_timeout=600)
            except Exception as exc:
                annotate("warning", f"midas probe2 layer {n}", {"type": name, "gzip_error": str(exc)[:400]})
                continue
            raw = Path(tmp) / "layer.xml"
            with gzip.open(path) as src, raw.open("wb") as dst:
                while block := src.read(8 << 20):
                    dst.write(block)
            info["gzip"] = {
                "bytes": size,
                "xml_bytes": raw.stat().st_size,
                "seconds": round(time.monotonic() - started),
                "ct": headers.get("Content-Type"),
                "cd": headers.get("Content-Disposition"),
                "head": raw.read_bytes()[:120].decode("utf-8", "replace"),
            }
            root_attrs, rows, members = feature_rows(raw)
        info["root"] = {k: v for k, v in root_attrs.items() if k != "schemaLocation"}
        info["members"], info["features"] = members, len(rows)
        cols = sorted({k for r in rows for k in r})
        info["columns"] = cols
        keys = {}
        for key in ("OBJECTID", "Numer_złoża", "ID_konturu", "@id"):
            vals = [r.get(key) for r in rows]
            c = Counter(v for v in vals if v not in (None, ""))
            keys[key] = {
                "null": sum(1 for v in vals if v in (None, "")),
                "distinct": len(c),
                "dup_values": sum(1 for v in c.values() if v > 1),
                "max_rep": max(c.values(), default=0),
                "sample_dup": [k for k, v in c.most_common(3) if v > 1],
            }
        info["keys"] = keys
        prof = []
        for k in cols:
            vals = [r.get(k) for r in rows]
            nonnull = [v for v in vals if v not in (None, "")]
            shapes = Counter(
                "int"
                if re.fullmatch(r"-?\d+", v)
                else "dec"
                if re.fullmatch(r"-?\d+\.\d+", v)
                else "exp"
                if re.fullmatch(r"-?\d+(\.\d+)?[eE][-+]?\d+", v)
                else "geom"
                if v.startswith("<")
                else "text"
                for v in nonnull
            )
            nums = [float(v) for v in nonnull if re.fullmatch(r"-?\d+(\.\d+)?([eE][-+]?\d+)?", v)]
            c = Counter(nonnull)
            row = [k, len(vals) - len(nonnull), len(c), dict(shapes)]
            if nums:
                row += [min(nums), max(nums)]
            row.append([str(s)[:20] for s, _ in c.most_common(4)])
            prof.append(row)
        info["profile"] = prof
        if any(r.get("Numer_złoża") for r in rows):
            nz = [r.get("Numer_złoża") or "" for r in rows]
            info["numer_zloza_nonint"] = [v for v in Counter(nz) if v and not re.fullmatch(r"\d+(\.0+)?", v)][:8]
        print(json.dumps({"layer": name, **info}, ensure_ascii=False), flush=True)
        annotate("notice", f"midas probe2 layer {n}", {"type": name[6:40], **info})


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else "trial"
    try:
        if mode == "probe":
            probe()
        elif mode == "probe2":
            probe2()
        else:
            raise SystemExit(f"unknown mode {mode}")
    except Exception as exc:
        annotate("error", "midas trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
