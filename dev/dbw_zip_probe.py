"""Read-only: why some DBW dictionary CSVs do not parse. Deleted before merge."""

import io
from pathlib import Path
import tempfile
import zipfile

import duckdb
import requests

from ingestion.common.report import annotate
from ingestion.to_landing.gus_dbw import extract as x

s = requests.Session()
s.headers["User-Agent"] = x.USER_AGENT
for name in ("222_1116_6_2018_282_pl.zip", "183_1116_180_2014_282_pl.zip"):
    z = zipfile.ZipFile(io.BytesIO(x.get(s, f"{x.WEB}/bulk_new/{name}").content))
    member = next(m for m in z.infolist() if m.filename.endswith("_Slowniki.csv"))
    raw = z.read(member)
    text = raw.decode("utf-8", "replace")
    lines = text.split("\r\n")
    odd = [(i, line[:200]) for i, line in enumerate(lines) if line.count('"') % 2 or line.count('"') > 2][:8]
    no_crlf = text.count("\n") - text.count("\r\n")
    out = {
        "size": len(raw),
        "lines": len(lines),
        "odd_quote_lines": odd,
        "bare_lf": no_crlf,
        "bom": raw[:3] == b"\xef\xbb\xbf",
        "non_utf8": raw.decode("utf-8", "replace").count("�"),
    }
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "d.csv"
        p.write_bytes(raw)
        cols = {f"c{i}": "VARCHAR" for i in range(5)}
        try:
            n = duckdb.sql(
                f"select count(*) from read_csv('{p}', delim=';', header=false, skip=1, columns={cols}, quote='\"', "
                "escape='\"', null_padding=true, all_varchar=true, strict_mode=true, auto_detect=false)"
            ).fetchone()
            out["auto_detect_false"] = n
        except Exception as exc:
            out["auto_detect_false_error"] = str(exc)[:600]
    annotate("notice", f"dbw dict {name}", out)
