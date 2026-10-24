"""Read-only: what the DBW bulk ZIPs contain (member names, headers, first rows). Deleted before merge."""

import io
import zipfile

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.gus_dbw import extract as x

s = requests.Session()
s.headers["User-Agent"] = x.USER_AGENT
tree = x.get(s, f"{x.WEB}/api_app/wsk/getIndicatorsTree").json()
inds = x.indicators(tree)
out = {"indicators_total": len(inds)}
for name in ("222_1116_318_2010_282_pl.zip", "1285_29_95_2_pl.zip", "567_156_94_4_pl.zip"):
    body = x.get(s, f"{x.WEB}/bulk_new/{name}").content
    z = zipfile.ZipFile(io.BytesIO(body))
    members = []
    for m in z.infolist():
        with z.open(m) as h:
            head = h.read(700).decode("utf-8", "replace")
        members.append({"name": m.filename, "size": m.file_size, "head": head})
    out[name] = members
for k, v in out.items():
    annotate("notice", f"dbw zip {k}", v)
