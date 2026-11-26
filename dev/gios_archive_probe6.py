"""Read-only probe 6 (#3119): the 2013 year file only, on the runner's disk: members whose header says a daily
averaging time but whose periods hold hours, the rows with a time of day, and the columns labelled '1t'."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile

import duckdb

from ingestion.to_bronze.gios_air_quality import archive
from ingestion.to_landing.gios_air_quality import archive as landing


def main() -> int:
    years = sys.argv[1:] or ["2013"]
    s = landing.session()
    listed = landing.listing(s)
    con = duckdb.connect()
    q = lambda sql: con.execute(sql).fetchall()  # noqa: E731
    for y in years:
        body = landing.get(s, listed[f"archive_{y}"]["url"])
        out = Path(tempfile.mkdtemp()) / "b.parquet"
        archive.decode(body, {"entity": f"archive_{y}", "sha256": "p", "version_token": "p", "ingested_at": "20261126T000000Z"}, out)
        B = f"(select *, cast(period as double) as p from read_parquet('{out}'))"
        print("MIXED", y, json.dumps(q(f"""select member, averaging_time, count(*) filter (where abs(p - round(p)) > 1.0/48), count(*),
            min(p) filter (where abs(p - round(p)) > 1.0/48), max(p) filter (where abs(p - round(p)) > 1.0/48)
            from {B} group by 1, 2 having count(*) filter (where abs(p - round(p)) > 1.0/48) > 0
            and count(*) filter (where abs(p - round(p)) <= 1.0/48) > 0 order by 1"""), default=str))
        print("MIXED_ROWS", y, json.dumps(q(f"""select member, station_code, averaging_time, period, value, sheet_row from {B}
            where member like '%Ni(PM10)%' and abs(p - round(p)) > 1.0/48 order by sheet_row limit 12"""), default=str))
        print("MIXED_SHEETROWS", y, json.dumps(q(f"""select member, min(sheet_row), max(sheet_row), count(distinct sheet_row), count(distinct station_code)
            from {B} where abs(p - round(p)) > 1.0/48 and member like '%24g%' group by 1"""), default=str))
        print("T1", y, json.dumps(q(f"""select member, station_code, indicator, averaging_time, min(period), max(period), count(*), list(value order by sheet_row)[1:8]
            from {B} where averaging_time = '1t' group by all"""), default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
