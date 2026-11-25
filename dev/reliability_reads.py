"""Read-only checks of the reliability lane, 25 Nov (#2873). Writes nothing to R2 or the catalog.

1. gleif rr (#2610): the newest Landing rr file, decoded locally; raw dates whose UTC instant is outside years
   1..9999; the Parquet statistics of the old Silver expressions (fail?) and of the branch's model (readable?),
   the latter also through PyIceberg's parquet_file_to_data_file as the loader commits it.
2. ms_krs (#3178): selection vs held records around 0140330387; four polite API requests.
3. #3196: the rows behind ms_krs_entities_complete and entsog_gas_days (dbt show, prod target, queries only).
4. eea (#2942): the catalogue request that failed on 25 Nov.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import traceback

from ingestion.common.report import annotate
from warehouse.build import PROJECT, TOPICS

DATES = [
    "relationship_period_1_startdate",
    "relationship_period_1_enddate",
    "registration_initialregistrationdate",
    "registration_lastupdatedate",
    "registration_nextrenewaldate",
]
LOW, HIGH = "'0001-01-01 00:00:00+00'", "'9999-12-31 23:59:59+00'"


def _stats(path: Path) -> list[str]:
    import pyarrow.parquet as pq

    md, bad = pq.ParquetFile(path).metadata, []
    for g in range(md.num_row_groups):
        for c in range(md.num_columns):
            s = md.row_group(g).column(c).statistics
            try:
                if s is not None and s.has_min_max:
                    s.min, s.max  # noqa: B018
            except Exception as exc:  # what parquet_file_to_data_file meets
                bad.append(f"{md.schema.column(c).name}: {type(exc).__name__}: {exc}")
    return sorted(set(bad))


def gleif() -> None:
    import duckdb

    from ingestion.common import bronze_files as bf
    from ingestion.common import entity_tables as et
    from ingestion.common.r2 import catalog, req, s3
    from ingestion.to_bronze.gleif import entities as g
    from ingestion.to_landing.patterns import bulk_file
    from warehouse import entity

    client, landing = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    receipt = bulk_file.newest(client, landing, "gleif", "rr")
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        g.fetch(client, landing, receipt, work / "raw.zip")
        rows, _ = g.decode(work / "raw.zip", receipt, work / "bronze.parquet", None)
        con = duckdb.connect()
        con.execute("SET TimeZone = 'UTC'")
        out = {"version": receipt["version_token"], "bronze_rows": rows, "out_of_range": {}}
        old = []
        for col in DATES:
            cast = f"try_cast(nullif({col}, '') as timestamptz)"
            old.append(f"{cast} as {col}")
            n, sample = con.execute(
                f"select count(*), list(relationship_startnode_nodeid || ' ' || {col})[1:5] "
                f"from '{work}/bronze.parquet' where {cast} not between {LOW} and {HIGH}"
            ).fetchone()
            if n:
                out["out_of_range"][col] = {"rows": n, "sample": sample}
        con.execute(
            f"COPY (select {', '.join(old)} from '{work}/bronze.parquet') TO '{work}/old.parquet' (FORMAT parquet)"
        )
        out["old_expressions_stats_errors"] = _stats(work / "old.parquet")
        report = entity.apply(
            "gleif_relationships",
            str(work / "bronze.parquet"),
            work / "silver.parquet",
            not_null=g.FILES["rr"][2],
            memory_limit="8GB",
            temp_directory=str(work),
        )
        out["branch_model"] = {"rows": report["rows"], "stats_errors": _stats(work / "silver.parquet")}
        try:
            table = bf.retry(lambda: catalog().load_table(("silver", "gleif_relationships")), "load_table")
            files = et._data_files(table, [str(work / "silver.parquet")], ["entity", "raw_sha256"])
            out["branch_model"]["data_file"] = {"records": files[0].record_count}
        except Exception as exc:
            out["branch_model"]["data_file"] = f"{type(exc).__name__}: {str(exc)[:300]}"
    annotate("notice", "1 gleif rr dates", out)


def krs() -> None:
    import requests

    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.ms_krs import extract as x

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    numbers = x.selection()
    parts = x.held_parts(client, bucket, "01_landing")
    held = {k: r for p in parts.values() for k, r in p["records"].items()}
    missing = [n for n in numbers if n not in held]
    with_extract = sorted(k for k, r in held.items() if r.get("http_status") == 200)
    out = {
        "selection": len(numbers),
        "held": len(held),
        "held_with_extract": len(with_extract),
        "highest_with_extract": with_extract[-3:],
        "missing": len(missing),
        "missing_first": missing[:5],
        "missing_last": missing[-3:],
        "missing_below_0001300000": sum(1 for n in missing if n < "0001300000"),
        "held_from_0001300000": sum(1 for n in held if n >= "0001300000"),
        "held_from_0001300000_with_extract": sum(1 for n in with_extract if n >= "0001300000"),
    }
    probe = {}
    session = requests.Session()
    session.headers.update({"User-Agent": "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data)"})
    for number in [*missing[:1], *with_extract[-1:]]:
        for register in ("P", "S"):
            r = session.get(x.EXTRACT_URL.format(krs=number, register=register), timeout=(20, 60))
            probe[f"{number} {register}"] = r.status_code
            time.sleep(1.0)
    out["probe"] = probe
    annotate("notice", "2 ms_krs selection and probe", out)


def _dbt(args: list[str]) -> subprocess.CompletedProcess:
    base = [
        "dbt",
        "--quiet",
        args[0],
        "--project-dir",
        str(PROJECT),
        "--profiles-dir",
        str(PROJECT),
        "--target",
        "prod",
    ]
    return subprocess.run(
        [*base, *args[1:], "--vars", json.dumps({"topics": TOPICS})],
        cwd=PROJECT,
        env={**os.environ},
        text=True,
        capture_output=True,
    )


def show(title: str, sql: str, limit: int = 40) -> None:
    done = _dbt(["show", "--inline", sql, "--limit", str(limit), "--output", "json"])
    out = done.stdout.strip()
    try:
        rows = json.loads(out[out.index("{") :])["show"]
    except (ValueError, KeyError):
        annotate("error", f"{title}: show failed", {"rc": done.returncode, "out": (out + done.stderr)[-1500:]})
        return
    annotate("notice", title, {"rows": len(rows), "first": rows})


KRS_PARTS = """
with b as (
    select entity, source_version, ingested_at, count(*) as rows, count(*) filter (where http_status = '200') as ext,
        count(*) filter (where http_status = '200' and register is null) as ext_no_register,
        count(*) filter (where http_status = '200' and name is null) as ext_no_name,
        count(distinct krs_number) as numbers
    from {{ source('ms_krs', 'ms_krs_extracts') }}
    group by entity, source_version, ingested_at
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),
s as (
    select entity, min(source_version) as s_version, max(source_version) as s_version_max, count(*) as s_rows,
        count(*) filter (where has_current_extract) as s_ext,
        count(*) filter (where has_current_extract and (register is null or name is null)) as s_ext_incomplete
    from {{ ref('ms_krs_entities') }}
    group by all
)
select b.*, s.* exclude (entity) from b left join s using (entity) order by entity
"""

KRS_ROWS = """
select krs_number, http_status, register, extract_kind, name is null as name_null, source_version
from {{ source('ms_krs', 'ms_krs_extracts') }}
where http_status = '200' and (register is null or name is null)
"""

KRS_VERSIONS = """
select entity, source_version, ingested_at, count(*) as rows
from {{ source('ms_krs', 'ms_krs_extracts') }}
group by all order by entity, source_version, ingested_at
"""

ENTSOG = """
select point_key, direction_key, substr(period_from, 12) as from_time, substr(period_to, 12) as to_time,
    date_diff('day', cast(left(period_from, 10) as date), cast(left(period_to, 10) as date)) as days,
    count(*) as rows, min(period_from) as first_from, max(period_from) as last_from, min(entity) as first_month,
    max(entity) as last_month
from {{ ref('entsog_physical_flow') }}
where substr(period_from, 12, 2) not in ('03', '04', '05') or cast(left(period_to, 10) as date) <> gas_day + 1
group by all order by rows desc
"""

ENTSOG_POINT_HOURS = """
with bad as (
    select distinct point_key, direction_key
    from {{ ref('entsog_physical_flow') }}
    where substr(period_from, 12, 2) not in ('03', '04', '05') or cast(left(period_to, 10) as date) <> gas_day + 1
)
select f.point_key, f.direction_key, substr(f.period_from, 12, 5) as from_hour, count(*) as rows,
    min(f.period_from) as first_from, max(f.period_from) as last_from
from {{ ref('entsog_physical_flow') }} as f join bad using (point_key, direction_key)
group by all order by 1, 2, 5
"""


def eea() -> None:
    import requests

    from ingestion.to_landing import eea as e

    url = f"{e.CATALOGUE}/{e.SERIES}/related?type=siblings"
    out = {}
    for accept in ("application/json", None):
        started = time.monotonic()
        try:
            r = requests.get(
                url, headers={"User-Agent": e.USER_AGENT, **({"Accept": accept} if accept else {})}, timeout=120
            )
            body = r.text
            out[str(accept)] = {
                "status": r.status_code,
                "seconds": round(time.monotonic() - started, 1),
                "type": r.headers.get("content-type"),
                "head": body[:300],
            }
        except Exception as exc:
            out[str(accept)] = f"{type(exc).__name__}: {str(exc)[:300]}"
        time.sleep(2)
    try:
        specs = e.resolve()
        out["resolve"] = specs
    except Exception as exc:
        out["resolve"] = f"{type(exc).__name__}: {str(exc)[:300]}"
    annotate("notice", "4 eea catalogue", out)


ENTSOG_ROWS = """
with f as (
    select * from {{ ref('entsog_physical_flow') }} where point_key = 'ITP-00543' and direction_key = 'exit'
)
select a.period_from, a.period_to, a.value_kwh_d as v, a.flow_status as st, a.last_update_utc as upd,
    a.tso_item_identifier as item, a.point_label, a.operator_label, a.id_point_type,
    b.period_from as from_05, b.value_kwh_d as v_05, b.flow_status as st_05, b.tso_item_identifier as item_05,
    b.last_update_utc as upd_05
from f as a
left join f as b on b.gas_day = a.gas_day and substr(b.period_from, 12, 2) = '05'
where substr(a.period_from, 12, 2) = '07'
order by a.period_from
"""


ENTSOG_SUMMARY = """
with f as (
    select * from {{ ref('entsog_physical_flow') }} where point_key = 'ITP-00543' and direction_key = 'exit'
),
odd as (select * from f where substr(period_from, 12, 2) not in ('03', '04', '05'))
select count(*) as rows, count(value_kwh_d) as with_value, count(*) filter (where is_na) as is_na,
    count(*) filter (where exists (select 1 from f as b where b.gas_day = odd.gas_day and b.period_from <> odd.period_from))
        as day_also_other_record,
    min(period_from) as first, max(period_from) as last, list(distinct strftime(gas_day, '%Y-%m') order by 1) as months,
    list(distinct substr(period_from, 12)) as starts, list(distinct last_update_utc::varchar) as updates,
    (select count(*) from f where substr(period_from, 12, 2) in ('03', '04', '05') and value_kwh_d is not null)
        as normal_with_value,
    (select max(period_from) from f where value_kwh_d is not null) as last_value_day
from odd
"""

ENTSOG_ALL = """
select count(*) as rows, count(value_kwh_d) as with_value
from {{ ref('entsog_physical_flow') }}
where substr(period_from, 12, 2) not in ('03', '04', '05') and value_kwh_d is not null
"""


def krs_layout() -> None:
    import requests

    from ingestion.to_landing.ms_krs import extract as x

    def shape(node, depth=0):
        if isinstance(node, dict):
            return {k: shape(v, depth + 1) for k, v in node.items()} if depth < 2 else f"dict[{len(node)}]"
        if isinstance(node, list):
            return [shape(node[0], depth + 1), len(node)] if node else "list[0]"
        return type(node).__name__

    out = {}
    session = requests.Session()
    session.headers.update({"User-Agent": "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data)"})
    for number in ("0000694417", "0001268305", "0000517824"):
        r = session.get(x.EXTRACT_URL.format(krs=number, register="P"), timeout=(20, 60))
        time.sleep(1.0)
        doc = r.json() if r.status_code == 200 else {}
        d1 = x._at(doc.get("odpis") or {}, "dane.dzial1") or {}
        out[number] = {
            "status": r.status_code,
            "odpis_keys": sorted((doc.get("odpis") or {}).keys()),
            "dane_keys": sorted((x._at(doc.get("odpis") or {}, "dane") or {}).keys()),
            "dzial1": shape(d1),
        }
    annotate("notice", "3g ms_krs answer layout without a name (structure only)", out)


def main() -> int:
    if os.environ.get("ONLY") or True:
        steps = [
            ("3f entsog summary", lambda: show("3f entsog ITP-00543 exit, records not starting 03-05", ENTSOG_SUMMARY)),
            ("3h entsog all", lambda: show("3h entsog records with a value not starting 03-05", ENTSOG_ALL)),
            ("3g ms_krs layout", krs_layout),
        ]
        return max((_run(n, s) for n, s in steps), default=0)
    steps = [
        ("1 gleif", gleif),
        ("2 ms_krs", krs),
        ("3a ms_krs parts", lambda: show("3a ms_krs Bronze newest vs Silver per part", KRS_PARTS)),
        ("3b ms_krs rows", lambda: show("3b ms_krs extracts without register or name", KRS_ROWS)),
        ("3c ms_krs versions", lambda: show("3c ms_krs Bronze versions", KRS_VERSIONS, 80)),
        ("3d entsog", lambda: show("3d entsog_gas_days rows", ENTSOG)),
        ("3e entsog hours", lambda: show("3e entsog hours of the points concerned", ENTSOG_POINT_HOURS, 80)),
        ("4 eea", eea),
    ]
    failed = 0
    for name, step in steps:
        try:
            step()
        except Exception:
            failed += 1
            annotate("error", f"{name} crashed", traceback.format_exc()[-1500:])
    return 1 if failed else 0


def _run(name, step) -> int:
    try:
        step()
        return 0
    except Exception:
        annotate("error", f"{name} crashed", traceback.format_exc()[-1500:])
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
