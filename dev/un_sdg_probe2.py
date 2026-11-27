"""Second read-only probe for GL-018 (#3316): the database archive page in full (bulk CSV files per release), HEAD of
the newest files, and the newest CSV ZIP downloaded to the runner's disk (not R2) and profiled with DuckDB. Also the
swagger paths (BOM-safe). GET/HEAD to unstats.un.org only, one request at a time, pauses, identifying User-Agent.
Deleted before merge (never on the PR branch).
"""

from __future__ import annotations

import json
import os
import re
import time
import zipfile

import duckdb
import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
HOST = "https://unstats.un.org"
S = requests.Session()
S.headers.update({"User-Agent": UA})
EU = ("('40','56','100','191','196','203','208','233','246','250','276','300','348','372','380','428','440','442',"
      "'470','528','616','620','642','703','705','724','752')")


def note(title: str, data) -> None:
    text = json.dumps(data, default=str)
    print(f"### {title}: {text}", flush=True)
    annotate("notice", title, data if len(text) < 3800 else {"truncated": text[:3800]})


def main() -> int:
    sw = S.get(f"{HOST}/SDGAPI/swagger/v1/swagger.json", timeout=(20, 120))
    paths = {}
    try:
        for path, ops in json.loads(sw.content.decode("utf-8-sig")).get("paths", {}).items():
            for method, op in ops.items():
                params = [p.get("name") for p in op.get("parameters", [])]
                body = op.get("requestBody", {}).get("content", {})
                if body:
                    params.append("body:" + ",".join(next(iter(body.values())).get("schema", {}).get("properties", {})))
                paths[f"{method.upper()} {path}"] = params
    except ValueError as exc:
        paths = {"error": str(exc)}
    note("sdg probe2 swagger", paths)
    time.sleep(2)
    page = S.get(f"{HOST}/sdgs/indicators/database/archive", timeout=(20, 120), headers={"Accept": "text/html"})
    links = sorted(set(re.findall(r'href="([^"]+\.zip)"', page.text)))
    csvs = [x for x in links if x.endswith("_CSV.zip")]
    text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", page.text))
    i = text.find("2026")
    note("sdg probe2 archive", {"links": len(links), "csv": len(csvs), "newest_csv": csvs[-8:],
                                "other": [x for x in links if not x.endswith(("_CSV.zip", "_R.zip"))][-6:],
                                "text_2026": text[max(0, i - 300): i + 900], "text_head": text[:600]})
    heads = {}
    for url in csvs[-3:]:
        time.sleep(2)
        u = url if url.startswith("http") else HOST + url
        h = S.head(u, timeout=(20, 120), allow_redirects=True)
        heads[u.rsplit("/", 1)[-1]] = {"st": h.status_code, "len": h.headers.get("content-length"),
                                        "lm": h.headers.get("last-modified"), "ct": h.headers.get("content-type")}
    note("sdg probe2 heads", heads)
    newest = csvs[-1] if csvs[-1].startswith("http") else HOST + csvs[-1]
    time.sleep(2)
    start = time.monotonic()
    path = "/tmp/sdg_newest.zip"
    with S.get(newest, timeout=(20, 900), stream=True) as r, open(path, "wb") as f:
        for chunk in r.iter_content(1 << 20):
            f.write(chunk)
    info = {"url": newest, "bytes": os.path.getsize(path), "minutes": round((time.monotonic() - start) / 60, 1)}
    with zipfile.ZipFile(path) as z:
        info["members"] = [(m.filename, m.file_size) for m in z.infolist()][:10]
        member = max(z.infolist(), key=lambda m: m.file_size)
        z.extract(member, "/tmp/sdg")
        csv = f"/tmp/sdg/{member.filename}"
    with open(csv, "rb") as f:
        info["head"] = f.read(1200).decode("utf-8", "replace")
    con = duckdb.connect()
    con.execute(f"create table t as select * from read_csv('{csv}', all_varchar=true, header=true)")
    cols = [r[0] for r in con.execute("describe t").fetchall()]
    info["columns"] = cols

    def q(s):
        return con.execute(s).fetchall()

    area = next(c for c in cols if c.lower() == "geoareacode")
    series = next(c for c in cols if c.lower() == "seriescode")
    info["rows"] = q("select count(*) from t")[0][0]
    info["areas"] = q(f'select count(distinct "{area}") from t')[0][0]
    info["series"] = q(f'select count(distinct "{series}") from t')[0][0]
    info["rows_eu27"] = q(f'select count(*) from t where "{area}" in {EU}')[0][0]
    note("sdg probe2 newest csv", info)
    prof = {c: q(f'select count(distinct "{c}"), count("{c}") from t')[0] for c in cols}
    note("sdg probe2 columns", prof)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
