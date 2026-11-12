"""Read-only: how big would Gold be if every loaded Silver block of Eurostat, GUS BDL and GUS DBW
were in it (design of generic Gold coverage, O36). Reads Iceberg metadata and the Silver files'
key columns; writes nothing to R2. Deleted before merge.

python -m dev.gold_coverage_size [minutes per source, default 60]
One compact annotation; per-block detail as CSV in DEV_OUTPUT_DIR.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import csv
import os
from pathlib import Path
import random
import re
import sys
import time

import duckdb

from ingestion.common.r2 import catalog, req
from ingestion.common.report import annotate

THREADS = 8
EU27 = "AT BE BG CY CZ DE DK EE EL ES FI FR HR HU IE IT LT LU LV MT NL PL PT RO SE SI SK".split()
TOTALS = "('TOTAL','T','_T','TOT','NSA')"  # NSA is not a total; counted apart below


def connect():
    con = duckdb.connect()
    con.execute("SET memory_limit = '9GB'; SET preserve_insertion_order = false; SET threads = 4")
    con.execute("INSTALL httpfs; LOAD httpfs")
    endpoint = req("STORAGE_ENDPOINT_URL").replace("https://", "").rstrip("/")
    con.execute(
        "CREATE SECRET r2 (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION 'auto', URL_STYLE 'path')",
        [req("STORAGE_WRITE_ACCESS_KEY"), req("STORAGE_WRITE_SECRET_KEY"), endpoint],
    )
    return con


def files_of(cat, name):
    """[(entity, path, rows, bytes)] of a Silver entity table, from metadata only."""
    table = cat.load_table(("silver", name))
    files = table.inspect.files()
    out = []
    for path, rows, size, partition in zip(  # noqa: B905
        files.column("file_path").to_pylist(),
        files.column("record_count").to_pylist(),
        files.column("file_size_in_bytes").to_pylist(),
        files.column("partition").to_pylist(),
    ):
        entity = next(iter((partition or {}).values()), None)
        out.append((str(entity), path, rows, size))
    return out


def table_size(cat, namespace, name):
    try:
        files = cat.load_table((namespace, name)).inspect.files()
    except Exception as exc:  # a missing table is a finding, not a failure
        return {"error": type(exc).__name__}
    return {
        "files": files.num_rows,
        "rows": sum(files.column("record_count").to_pylist()),
        "mb": round(sum(files.column("file_size_in_bytes").to_pylist()) / 1e6, 1),
    }


def scan(con, files, sql, budget_s, seed=7):
    """Run sql (with {path}) over each file on THREADS cursors, in random order within a time
    budget, so a partial result is an unbiased sample. Returns (rows per file, done files, errors)."""
    order = list(files)
    random.Random(seed).shuffle(order)
    deadline = time.monotonic() + budget_s
    done, errors = [], []

    def one(item):
        if time.monotonic() > deadline:
            return None
        entity, path = item[0], item[1]
        cur = con.cursor()
        for attempt in range(3):
            try:
                return item, cur.execute(sql.replace("{path}", path.replace("'", "''"))).fetchall()
            except Exception as exc:
                if attempt == 2:
                    errors.append(f"{entity}: {type(exc).__name__}: {str(exc)[:120]}")
                    return None
                time.sleep(5 * (attempt + 1))
        return None

    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        for result in pool.map(one, order):
            if result:
                done.append(result)
    return done, errors


EUROSTAT_SQL = f"""
select
    freq_code,
    count(*) as rows_all,
    count(*) filter (where value is not null) as n,
    count(*) filter (where value is not null and geo_code = 'PL') as pl,
    count(*) filter (where value is not null and geo_code like 'PL_%') as pl_region,
    count(*) filter (where value is not null and regexp_matches(geo_code, '^(EU|EA|EEA|EFTA)')) as agg,
    count(*) filter (where value is not null and geo_code <> 'PL' and geo_code in ({", ".join(repr(c) for c in EU27)})) as eu27,
    count(*) filter (where value is not null and length(geo_code) = 2
        and geo_code not in ({", ".join(repr(c) for c in EU27)}) and not regexp_matches(geo_code, '^(EU|EA)')) as country_other,
    count(*) filter (where value is not null and length(geo_code) between 3 and 5 and geo_code not like 'PL%'
        and regexp_matches(geo_code, '^[A-Z]{{2}}[0-9A-Z]{{1,3}}$') and not regexp_matches(geo_code, '^(EU|EA|EEA|EFTA)')) as nuts_other,
    approx_count_distinct(hash(unit_code, geo_code, breakdown_codes)) filter (where value is not null) as series,
    approx_count_distinct(hash(unit_code, breakdown_codes)) filter (where value is not null) as series_no_geo,
    count(distinct unit_code) as units,
    min(time_period) filter (where value is not null) as first_period,
    max(time_period) filter (where value is not null) as last_period,
    any_value(breakdown_dimensions) as dims,
    count(distinct breakdown_dimensions) as dim_sets,
    count(*) filter (where value is not null and geo_code is null) as geo_null
from read_parquet('{{path}}')
group by freq_code
"""

# Per dataset: for each breakdown dimension, how many codes and whether a total-like code exists.
EUROSTAT_DIMS_SQL = """
with s as (
    select distinct breakdown_dimensions as d, breakdown_codes as c
    from read_parquet('{path}') where value is not null
), u as (
    select unnest(string_split(d, ',')) as dim, unnest(string_split(c, ',')) as code from s where d <> ''
)
select dim, count(distinct code) as codes,
    bool_or(code in ('TOTAL', 'T', '_T')) as has_total,
    count(distinct code) = 1 as single
from u group by dim
"""


def eurostat(cat, con, budget_s, out_dir):
    files = files_of(cat, "eurostat_observations")
    meta = {
        "datasets": len({f[0].upper() for f in files}),
        "files": len(files),
        "rows": sum(f[2] for f in files),
        "mb": round(sum(f[3] for f in files) / 1e6),
    }
    done, errors = scan(con, files, EUROSTAT_SQL, budget_s * 0.7)
    cols = ["rows_all", "n", "pl", "pl_region", "agg", "eu27", "country_other", "nuts_other", "series", "series_no_geo"]
    total, by_freq, by_prefix, big, detail = Counter(), defaultdict(Counter), defaultdict(Counter), [], []
    dim_sets_gt1 = geo_null = 0
    for (entity, _path, rows, size), result in done:
        ds_n = 0
        for row in result:
            freq = row[0]
            values = dict(zip(cols, row[1:11], strict=True))
            for key, value in values.items():
                total[key] += value or 0
                by_freq[freq][key] += value or 0
                by_prefix[entity.upper().split("_")[0]][key] += value or 0
            by_freq[freq]["datasets"] += 1
            ds_n += values["n"] or 0
            dim_sets_gt1 += row[15] > 1
            geo_null += row[16] or 0
            detail.append([entity, freq, rows, size, *row[1:15]])
        by_prefix[entity.upper().split("_")[0]]["datasets"] += 1
        big.append((ds_n, entity.upper()))
    multi_freq = sum(1 for _e, c in Counter(e for (e, *_r), res in done for _ in res).items() if c > 1)
    scope = lambda c: c["pl"] + c["pl_region"] + c["agg"] + c["eu27"] + c["country_other"]  # noqa: E731
    top_prefix = sorted(by_prefix.items(), key=lambda kv: -kv[1]["n"])[:14]
    # total detection on a sample of the scanned datasets
    sample = [item for item, _res in done][:600]
    dims_done, dim_errors = scan(con, sample, EUROSTAT_DIMS_SQL, budget_s * 0.3, seed=11)
    dim_stats = Counter()
    dim_names = Counter()
    for _item, result in dims_done:
        dim_stats["datasets"] += 1
        open_dims = [r for r in result if not r[3]]  # more than one code
        dim_stats["all_open_dims_have_total"] += all(r[2] for r in open_dims)
        dim_stats["no_breakdown"] += not result
        dim_stats["dims"] += len(result)
        dim_stats["dims_single_code"] += sum(1 for r in result if r[3])
        dim_stats["open_dims"] += len(open_dims)
        dim_stats["open_dims_with_total"] += sum(1 for r in open_dims if r[2])
        for r in open_dims:
            if not r[2]:
                dim_names[r[0]] += 1
    with open(Path(out_dir) / "eurostat.csv", "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["dataset", "freq", "file_rows", "file_bytes", *cols, "units", "first", "last", "dims"])
        writer.writerows(detail)
    return {
        "meta": meta,
        "scanned": {
            "files": len(done),
            "file_rows": sum(i[2] for i, _ in done),
            "errors": len(errors) + len(dim_errors),
            "err": (errors + dim_errors)[:2],
        },
        "total": dict(total),
        "scope_countries_agg_plregions": scope(total),
        "by_freq": {
            f: {
                "ds": c["datasets"],
                "n": c["n"],
                "scope": scope(c),
                "pl": c["pl"] + c["pl_region"],
                "series": c["series"],
            }
            for f, c in sorted(by_freq.items(), key=lambda kv: -kv[1]["n"])
        },
        "top_prefix": {p: [c["datasets"], c["n"], scope(c)] for p, c in top_prefix},
        "largest": [[e, n] for n, e in sorted(big, reverse=True)[:8]],
        "ds_over_10M": sum(1 for n, _ in big if n > 10_000_000),
        "ds_over_1M": sum(1 for n, _ in big if n > 1_000_000),
        "multi_freq_datasets": multi_freq,
        "dim_sets_gt1": dim_sets_gt1,
        "geo_null": geo_null,
        "totals": dict(dim_stats),
        "open_dims_without_total": dim_names.most_common(12),
    }


def bdl(cat, con, budget_s, out_dir):
    files = files_of(cat, "bdl_observations")
    meta = {
        "subgroups": len({f[0] for f in files}),
        "files": len(files),
        "rows": sum(f[2] for f in files),
        "mb": round(sum(f[3] for f in files) / 1e6),
    }
    variables = (
        cat.load_table(("silver", "bdl_variables"))
        .scan(selected_fields=("subgroup_id", "dimension_key", "n1"))
        .to_arrow()
    )  # noqa: F841
    units = cat.load_table(("silver", "bdl_units")).scan(selected_fields=("unit_id", "level")).to_arrow()  # noqa: F841
    con.execute(
        """create or replace table bdl_var as
        select subgroup_id, dimension_key,
            case
                when lower(trim(cast(n1 as varchar))) in ('styczeń','luty','marzec','kwiecień','maj','czerwiec','lipiec',
                    'sierpień','wrzesień','październik','listopad','grudzień') then 'M'
                when regexp_matches(lower(trim(cast(n1 as varchar))), '^[1-4] kwartał$') then 'Q'
                when regexp_matches(lower(trim(cast(n1 as varchar))), '^([1-4]-[1-4] kwartał|.*półrocz|styczeń-|rok$)') then 'X'
                else 'A' end as freq
        from variables"""
    )
    con.execute("create or replace table bdl_unit as select unit_id, level from units")
    sql = """
    select
        coalesce(v.freq, '?') as freq,
        case
            when o.unit_code_scheme = 'TERC' then
                case when o.unit_code = '0000000' then 0 when o.unit_code like '__00000' then 2
                     when o.unit_code like '____000' then 5 else 6 end
            else coalesce(u.level, -1) end as level,
        count(*) as rows_all,
        count(*) filter (where o.value is not null) as n,
        approx_count_distinct(hash(o.unit_code, o.unit_name, o.dimension_values, o.measure_unit))
            filter (where o.value is not null) as series,
        min(o.year) as y0, max(o.year) as y1,
        count(*) filter (where o.value is not null and o.year >= 2010) as n_2010,
        count(distinct o.measure_unit) as units
    from read_parquet('{path}') as o
    left join bdl_var as v on o.subgroup_id = v.subgroup_id and o.dimension_values = v.dimension_key
    left join bdl_unit as u on o.unit_code = u.unit_id
    group by 1, 2
    """
    done, errors = scan(con, files, sql, budget_s)
    by_level, by_freq, total, big, detail = defaultdict(Counter), defaultdict(Counter), Counter(), Counter(), []
    for (entity, _path, rows, size), result in done:
        for freq, level, rows_all, n, series, y0, y1, n_2010, _units in result:
            for bucket in (by_level[level], by_freq[freq], total):
                bucket["rows_all"] += rows_all
                bucket["n"] += n
                bucket["series"] += series or 0
                bucket["n_2010"] += n_2010
            big[entity] += n
            detail.append([entity, freq, level, rows, size, rows_all, n, series, y0, y1, n_2010])
    with open(Path(out_dir) / "bdl.csv", "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            ["subgroup", "freq", "level", "file_rows", "file_bytes", "rows_all", "n", "series", "y0", "y1", "n_2010"]
        )
        writer.writerows(detail)
    upto = lambda lv: sum(c["n"] for level, c in by_level.items() if 0 <= level <= lv)  # noqa: E731
    return {
        "meta": meta,
        "scanned": {
            "files": len(done),
            "file_rows": sum(i[2] for i, _ in done),
            "errors": len(errors),
            "err": errors[:2],
        },
        "total": dict(total),
        "by_level": {str(k): [c["n"], c["series"]] for k, c in sorted(by_level.items())},
        "n_to_woj": upto(2),
        "n_to_podregion": upto(4),
        "n_to_powiat": upto(5),
        "by_freq": {k: [c["n"], c["series"]] for k, c in by_freq.items()},
        "largest": big.most_common(6),
        "sg_over_1M": sum(1 for n in big.values() if n > 1_000_000),
    }


def dbw(cat, con, budget_s, out_dir):
    files = files_of(cat, "dbw_observations")
    block = lambda e: re.sub(r"(_[0-9]{4})?_pl$", "", e)  # noqa: E731
    meta = {
        "blocks": len({block(f[0]) for f in files}),
        "files": len(files),
        "rows": sum(f[2] for f in files),
        "mb": round(sum(f[3] for f in files) / 1e6),
    }
    sql = """
    select
        count(*) as rows_all,
        count(*) filter (where value is not null) as n,
        approx_count_distinct(hash(presentation_id, position_1_id, position_2_id, position_3_id, position_4_id,
            position_5_id, position_6_id, position_7_id, position_8_id, position_9_id))
            filter (where value is not null) as series,
        count(distinct period_id) as periods, min(year) as y0, max(year) as y1,
        count(distinct presentation_id) as presentations
    from read_parquet('{path}')
    """
    done, errors = scan(con, files, sql, budget_s)
    by_freq, total, big, detail = defaultdict(Counter), Counter(), Counter(), []
    blocks_seen = defaultdict(set)
    for (entity, _path, rows, size), result in done:
        rows_all, n, series, periods, y0, y1, presentations = result[0]
        name = block(entity)
        freq = name.rsplit("_", 1)[-1]
        for bucket in (by_freq[freq], total):
            bucket["rows_all"] += rows_all
            bucket["n"] += n
            bucket["series_file_sum"] += series or 0
        blocks_seen[freq].add(name)
        big[name] += n
        detail.append([entity, name, freq, rows, size, rows_all, n, series, periods, y0, y1, presentations])
    with open(Path(out_dir) / "dbw.csv", "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "entity",
                "block",
                "periodicity",
                "file_rows",
                "file_bytes",
                "rows_all",
                "n",
                "series",
                "periods",
                "y0",
                "y1",
                "presentations",
            ]
        )
        writer.writerows(detail)
    return {
        "meta": meta,
        "scanned": {
            "files": len(done),
            "file_rows": sum(i[2] for i, _ in done),
            "errors": len(errors),
            "err": errors[:2],
        },
        "total": dict(total),
        "by_periodicity": {
            k: [len(blocks_seen[k]), c["n"]] for k, c in sorted(by_freq.items(), key=lambda kv: -kv[1]["n"])
        },
        "largest": big.most_common(6),
        "blocks_over_1M": sum(1 for n in big.values() if n > 1_000_000),
    }


def main() -> None:
    minutes = float(sys.argv[1]) if len(sys.argv) > 1 else 60
    only = sys.argv[2].split(",") if len(sys.argv) > 2 else ["eurostat", "bdl", "dbw"]
    out_dir = os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output")
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    cat = catalog()
    con = connect()
    report = {
        "gold_now": {
            t: table_size(cat, "gold", t)
            for t in ("fact_annual", "fact_quarterly", "fact_monthly", "fact_daily", "dim_series", "dim_measure")
        }
    }
    for name, run in (("eurostat", eurostat), ("bdl", bdl), ("dbw", dbw)):
        if name not in only:
            continue
        started = time.monotonic()
        try:
            report[name] = run(cat, con, minutes * 60, out_dir)
        except Exception as exc:
            report[name] = {"error": f"{type(exc).__name__}: {str(exc)[:300]}"}
        report[name]["seconds"] = round(time.monotonic() - started)
        # one annotation per source: each stays under the 4 KB cap
        annotate("notice", f"gold coverage size {name}", report[name])
    annotate("notice", "gold coverage size gold_now", report["gold_now"])


if __name__ == "__main__":
    main()
