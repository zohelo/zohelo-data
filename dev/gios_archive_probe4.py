"""Read-only probe 4 (#3119): the decoder and the Silver SQL of this branch over every yearly ZIP and the station
workbook, on the runner's disk only (no R2): rows per year, layouts, header quirks, Silver checks, station codes
resolved through the station workbook. One download at a time, 5 s apart (landing archive.get)."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import time

import duckdb

from ingestion.common.report import annotate
from ingestion.to_bronze.gios_air_quality import archive, archive_load
from ingestion.to_landing.gios_air_quality import archive as landing
from warehouse import entity

FIVE = ("PM10", "PM2.5", "NO2", "O3", "SO2")


def main() -> int:
    s = landing.session()
    listed = landing.listing(s)
    print("LISTED", json.dumps({e: v["id"] for e, v in sorted(listed.items())}))
    con = duckdb.connect()
    work = Path(tempfile.mkdtemp())
    receipt = {"sha256": "probe", "version_token": "probe", "ingested_at": "20261126T000000Z"}
    time.sleep(5)
    st = work / "stations.parquet"
    rows = archive_load.stations(landing.get(s, listed["archive_stations"]["url"]), {**receipt, "entity": "archive_stations"}, st)
    q = lambda sql: con.execute(sql).fetchall()  # noqa: E731
    S = f"read_parquet('{st}')"
    print("STATIONS", rows, q(f"select count(distinct station_code), count(*) filter (where old_station_codes is not null) from {S}"))
    print("STATIONS_VOIV", q(f"select voivodeship, count(*) from {S} group by 1 order by 1"))
    print("STATIONS_OLD_SAMPLE", q(f"select station_code, old_station_codes from {S} where old_station_codes like '%,%' or old_station_codes like '%;%' or old_station_codes like '% %' limit 8"))
    print("STATIONS_DATES", q(f"select min(opened_on), max(opened_on), min(closed_on), max(closed_on), min(latitude), max(longitude) from {S}"))
    years = sorted((e for e in listed if e != "archive_stations"), reverse=True)
    if len(sys.argv) > 1:
        years = [f"archive_{y}" for y in sys.argv[1:]]
    allcodes = work / "codes.parquet"
    codes: list[str] = []
    summary = {}
    for name in years:
        time.sleep(5)
        t0 = time.monotonic()
        try:
            body = landing.get(s, listed[name]["url"])
        except Exception as exc:
            print("YEAR", name, "download failed", exc)
            continue
        info = {"bytes": len(body), "download_s": round(time.monotonic() - t0, 1)}
        b, sv = work / f"{name}.b.parquet", work / f"{name}.s.parquet"
        try:
            rep = archive.decode(body, {**receipt, "entity": name}, b)
            info.update({"members": rep["members"], "rows": rep["rows"], "skipped": rep["skipped"], "other_sheets": rep["other_sheets"],
                         "layouts": {k: len(v) for k, v in rep["layouts"].items()}})
        except archive.ArchiveDecodeError as exc:
            info["decode_error"] = str(exc)[:600]
            print("YEAR", name, json.dumps(info, ensure_ascii=False))
            summary[name] = info
            continue
        try:
            res = entity.apply("gios_air_archive_values", str(b), sv, not_null=archive_load.NOT_NULL, unique_key=archive_load.KEY,
                               same_rows=True, reject_if=archive_load.REJECT_IF, memory_limit="8GB")
            info.update({"silver_rows": res["rows"], "silver_bytes": res["bytes"], "bronze_bytes": b.stat().st_size})
        except Exception as exc:
            info["silver_error"] = f"{type(exc).__name__}: {str(exc)[:600]}"
            res = entity.apply("gios_air_archive_values", str(b), sv, same_rows=True, memory_limit="8GB")
        V = f"read_parquet('{sv}')"
        info["fallback_indicator"] = q(f"select member, count(distinct sheet_row) from {V} where indicator_published is null group by 1 limit 8")
        info["avg_published"] = q(f"select averaging_time, coalesce(averaging_time_published, '<null>'), count(*) from {V} group by 1, 2 order by 3 desc limit 12")
        info["avg_conflict"] = q(f"select member, averaging_time, averaging_time_published, count(*) from {V} where averaging_time_published in ('1g', '24g') and averaging_time_published <> averaging_time group by all limit 5")
        info["odd_station_codes"] = q(f"select member, station_code, indicator_published, averaging_time_published, count(*) from {V} where not regexp_matches(station_code, '^[A-Z][a-z][A-Za-z0-9._-]+$') group by all limit 8")
        info["dup"] = q(f"select count(*) from (select 1 from {V} group by station_code, indicator, averaging_time, measured_at having count(*) > 1)")[0][0]
        info["dup_sample"] = q(f"select station_code, indicator, averaging_time, list(distinct member), count(*) from {V} group by station_code, indicator, averaging_time, measured_at having count(*) > 1 limit 4")
        info["non_numeric"] = q(f"select value_text, count(*) from {V} where value is null group by 1 order by 2 desc limit 10")
        info["off_hour"] = q(f"select count(*) from {V} where averaging_time = '1g' and minute(measured_at) <> 0")[0][0]
        info["indicators"] = q(f"select indicator, averaging_time, count(distinct station_code), count(*) from {V} group by 1, 2 order by 1, 2")
        info["five_units"] = q(f"select indicator, coalesce(unit, '<none>'), count(*) from {V} where indicator in {FIVE} group by 1, 2")
        info["five_range"] = q(f"select indicator, averaging_time, min(value), max(value), count(*) filter (where value < 0), count(*) filter (where value = 0) from {V} where indicator in {FIVE} group by 1, 2 order by 1, 2")
        info["days"] = q(f"select min(measured_on), max(measured_on) from {V}")[0]
        con.execute(f"create table if not exists c (y varchar, code varchar)")
        con.execute(f"insert into c select distinct '{name}', station_code from {V} where indicator in {FIVE} and averaging_time = '1g'")
        print("YEAR", name, json.dumps(info, ensure_ascii=False, default=str))
        summary[name] = {k: info.get(k) for k in ("bytes", "rows", "silver_rows", "dup", "silver_error")}
        b.unlink(missing_ok=True)
        sv.unlink(missing_ok=True)
    # station codes of the five pollutants (hourly) resolved through the station sheet: current code or an old code
    con.execute(f"""create table m as
        select station_code as code, station_code, voivodeship from {S}
        union all
        select trim(o.code), station_code, voivodeship from (select station_code, voivodeship, unnest(string_split(regexp_replace(old_station_codes, '[;\\s]+', ',', 'g'), ',')) as code from {S} where old_station_codes is not null) as o where trim(o.code) <> ''""")
    print("CODE_AMBIGUOUS", q("select code, list(distinct station_code) from m group by 1 having count(distinct station_code) > 1 limit 10"))
    print("CODE_UNRESOLVED", q("select y, count(*), list(code)[1:8] from c where code not in (select code from m) group by 1 order by 1"))
    print("CODE_RESOLVED", q("select count(distinct code), count(distinct code) filter (where code in (select code from m)) from c"))
    annotate("notice", "gios archive probe 4", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
