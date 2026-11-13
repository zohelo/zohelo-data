"""Read-only probe 2 of the GPR spreadsheets (register PL-ENV-022): why xlrd cannot open the 2020/21 national SDRR
workbook (traceback and read variants), repeated section numbers per SDRR file, and the attribute table (DBF) of the
two SHP ZIPs as a fallback route. One request at a time, 2 s apart; writes nothing to R2. Deleted before merge."""

from __future__ import annotations

from collections import Counter
import io
import re
import struct
import time
import traceback
import zipfile

import requests

from ingestion.common import xlsx_cells
from ingestion.to_landing.patterns import bulk_file

FILES = {
    "dk20_sdrr": "https://www.gov.pl/attachment/edfc2cc9-e35e-4702-ba86-43f920229bd2",
    "dk20_errata": "https://www.gov.pl/attachment/f25450b4-5f16-4911-9605-7f08d9ab914c",
    "dw20_sdrr": "https://www.gov.pl/attachment/bc113506-2cb6-45fb-9e83-7c8e60aa11fd",
    "dk25_sdrr": "https://www.gov.pl/attachment/28194e66-2fb2-4dd7-becf-fedcf2b26908",
    "dw25_sdrr": "https://www.gov.pl/attachment/17ff216a-615f-4593-afa9-e62d5bffc738",
}
SHP = {
    "shp20": "https://www.gov.pl/attachment/540a9afe-df60-4610-bc93-808a925e0ed0",
    "shp25": "https://www.gov.pl/attachment/7f8ca805-e384-4802-bf10-d36538e6ef84",
}


def get(url: str) -> bytes:
    time.sleep(2)
    r = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=120)
    print(f"GET {url} {r.status_code} {len(r.content)} cd={r.headers.get('Content-Disposition')}", flush=True)
    return r.content


def col1(body: bytes) -> list[str]:
    if body[:2] == b"PK":
        return [c["value_text"] for c in xlsx_cells.cells(body) if c["column_number"] == 1 and c["row_number"] > 10]
    import xlrd

    sh = xlrd.open_workbook(file_contents=body).sheet_by_index(0)
    out = []
    for r in range(10, sh.nrows):
        v = sh.cell_value(r, 0)
        out.append(str(int(v)) if isinstance(v, float) else str(v))
    return out


def dbf(data: bytes, name: str) -> None:
    n, header, size = struct.unpack("<IHH", data[4:12])
    fields, pos = [], 32
    while data[pos] != 0x0D:
        fname = data[pos : pos + 11].split(b"\0")[0].decode("latin-1")
        fields.append((fname, chr(data[pos + 11]), data[pos + 16]))
        pos += 32
    print(f"  DBF {name}: records={n} fields={fields}")
    for i in range(min(n, 3)):
        rec = data[header + i * size : header + (i + 1) * size]
        off, vals = 1, []
        for fname, _, length in fields:
            vals.append(f"{fname}={rec[off:off + length].decode('cp1250', 'replace').strip()}")
            off += length
        print("   ", " | ".join(vals))


def main() -> int:
    import xlrd

    for name, url in FILES.items():
        body = get(url)
        if name == "dk20_sdrr":
            print(f"  head={body[:8].hex()} size={len(body)}")
            for kwargs in ({}, {"on_demand": True}, {"ignore_workbook_corruption": True}, {"formatting_info": True}):
                try:
                    book = xlrd.open_workbook(file_contents=body, **kwargs)
                    sh = book.sheet_by_index(0)
                    print(f"  xlrd {kwargs}: ok sheets={book.sheet_names()} rows={sh.nrows} cols={sh.ncols}")
                    for r in list(range(0, 13)) + [sh.nrows - 2, sh.nrows - 1]:
                        print(f"   r{r + 1}: {[sh.cell_value(r, c) for c in range(sh.ncols)][:20]}")
                    break
                except Exception:
                    print(f"  xlrd {kwargs}: failed\n" + "".join(traceback.format_exc().splitlines(True)[-8:]))
            try:
                from xlrd import compdoc

                cd = compdoc.CompDoc(body, logfile=io.StringIO())
                print("  OLE dirlist:", [(d.name, d.tot_size) for d in cd.dirlist if d.name])
            except Exception as exc:
                print("  OLE read failed:", repr(exc))
        try:
            ids = [v.strip() for v in col1(body)]
        except Exception as exc:
            print(f"  {name}: col1 failed {exc!r}")
            continue
        data = [v for v in ids if re.fullmatch(r"\d+", v)]
        dup = [k for k, c in Counter(data).items() if c > 1]
        print(f"  {name}: data_rows={len(data)} distinct={len(set(data))} repeated={dup[:30]} other={[v[:40] for v in ids if v and not re.fullmatch(r'[0-9]+', v)][:5]}")
        if name == "dk20_errata":
            print(f"  errata ids: {data}")
    for name, url in SHP.items():
        body = get(url)
        z = zipfile.ZipFile(io.BytesIO(body))
        print(f"  {name} members: {[(i.filename, i.file_size) for i in z.infolist()]}")
        for i in z.infolist():
            if i.filename.lower().endswith(".dbf"):
                dbf(z.read(i.filename), i.filename)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
