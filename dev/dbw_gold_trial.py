"""Dev trial (scratch only): Gold observations of a sample of GUS DBW blocks, built by
warehouse/observations.py into the namespace and prefix zz_dev_dbw_gold, checked, then removed. Reads
production Silver, the blocks' dictionary files, gold.map_geography and the curated facts; writes nothing
outside the scratch. Deleted before merge.

python -m dev.dbw_gold_trial [random sample size, default 34] [minutes, default 60] [keep]
"""

from __future__ import annotations

from collections import Counter
import contextlib
import json
import os
from pathlib import Path
import random
import subprocess
import sys
import time

import duckdb

from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog, req, s3
from warehouse import observations as obs

NS = "zz_dev_dbw_gold"
SOURCE = "gus_dbw"
MODELS = Path(obs.__file__).parent / "entity"
# Curated blocks (poc_dbw_blocks): KPI blocks, NUTS twins (BAEL), powiaty (875), regional blocks.
ALWAYS = [
    "1038_16_105_4", "1038_16_92_4", "1038_16_389_3", "875_143_95_1", "1514_1360_95_3", "1512_1360_95_3",
    "305_1722_5_1", "305_1721_5_3", "376_2_243_1", "398_16_243_3", "469_16_117_1", "581_57_93_4", "581_16_2_1",
    "1603_1339_116_4", "507_870_93_1",
]
# curated measure (gold.fact_*) -> its fact table; the rule key is the same key with @<frequency>
SPOT = {
    "gus_dbw:1038_16_105_4:105": ("fact_annual", "A"),
    "gus_dbw:1038_16_92_4:92": ("fact_annual", "A"),
    "gus_dbw:581_57_93_4:93": ("fact_annual", "A"),
    "gus_dbw:1514_1360_95_3:95": ("fact_quarterly", "Q"),
    "gus_dbw:398_16_243_3:243": ("fact_quarterly", "Q"),
    "gus_dbw:875_143_95_1:95": ("fact_monthly", "M"),
    "gus_dbw:305_1722_5_1:5": ("fact_monthly", "M"),
    "gus_dbw:376_2_243_1:243": ("fact_monthly", "M"),
    "gus_dbw:469_16_117_1:117": ("fact_monthly", "M"),
}


def annotate(level: str, title: str, data) -> None:
    """As ingestion.common.report.annotate, with Polish letters kept as they are."""
    message = json.dumps(data, default=str, separators=(",", ":"), ensure_ascii=False)
    if len(message) > 3900:
        message = message[:3888] + "…[truncated]"
    if os.environ.get("GITHUB_ACTIONS") == "true":
        message = message.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        print(f"::{level} title={title}::{message}", flush=True)
    else:
        print(f"[{level}] {title}: {message}", flush=True)


def connect(tmp: str):
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '9GB'; SET preserve_insertion_order = false; SET temp_directory = '{tmp}'")
    if os.environ.get("STORAGE_ENDPOINT_URL", "").startswith("https://") and not os.environ.get("HARNESS_ROOT"):
        con.execute("INSTALL httpfs; LOAD httpfs")
        endpoint = req("STORAGE_ENDPOINT_URL").replace("https://", "").rstrip("/")
        con.execute(
            "CREATE SECRET r2 (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION 'auto', URL_STYLE 'path')",
            [req("STORAGE_WRITE_ACCESS_KEY"), req("STORAGE_WRITE_SECRET_KEY"), endpoint],
        )
    return con


def files(paths: list[str]) -> str:
    return "read_parquet([" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "])"


def state_of(cat, name: str) -> dict:
    return et.state(cat.load_table((NS, f"{SOURCE}_{name}")), "block_id")


def cleanup(cat, client) -> dict:
    gone = {"tables": 0, "objects": 0}
    for name in [*(f"{SOURCE}_{t}" for t in obs.TABLES), obs.REJECTED[1]]:
        with contextlib.suppress(Exception):
            cat.drop_table((NS, name))
            gone["tables"] += 1
    with contextlib.suppress(Exception):
        cat.drop_namespace(NS)
    bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME")
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=NS + "/"):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            client.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
            gone["objects"] += len(keys)
    left = sum(
        len(p.get("Contents", []))
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=NS + "/")
    )
    return {**gone, "objects_left": left, "namespace_left": (NS,) in cat.list_namespaces()}


def account(tmp: str, item: dict, geo_map_path: str) -> dict:
    """Silver rows of one block by what the build stage does with them, counted with the build stage's own
    preparation steps (labels, periods, areas, shape, twins): empty, a 0 that is a no-value sign, a period
    the calendar cannot place (by period name), a NUTS twin that repeats its voivodeship, loaded."""
    con = connect(tmp)
    try:
        con.execute(f"create view silver as select * from {files(item['paths'])}")
        con.execute(f"create view dictionary as select * from {files(item['with']['dictionary'])}")
        con.execute(f"create view geo_map as select * from read_parquet('{geo_map_path}')")
        for name, model in obs.SOURCES[SOURCE]["prepare"].items():
            con.execute(f"create temp table {name} as {(MODELS / (model + '.sql')).read_text(encoding='utf-8')}")
        positions = "[" + ", ".join(f"s.position_{k}_id" for k in range(1, 10)) + "]"
        rows = con.execute(
            f"""
            with marked as (
                select
                    case
                        when s.value is null then 'empty'
                        when s.value = 0 and coalesce(s.missing_reason_id, 253) <> 253 then 'zero_sign'
                        when pe.frequency is null or s.year is null then 'period'
                        when t.entity_id is not null then 'twin'
                        else 'loaded'
                    end as outcome,
                    case when pe.frequency is null then coalesce(pe.period_name, 'id ' || s.period_id) end as period,
                    s.missing_reason_id as sign, s.value
                from silver as s
                left join periods as pe on s.period_id = pe.period_id
                left join twins as t on s.entity_id = t.entity_id and s.presentation_id = t.presentation_id
                    and s.period_id = t.period_id and s.year = t.year and {positions} = t.positions
            )
            select outcome, case when outcome = 'period' then period end, case when outcome = 'zero_sign' then sign end,
                count(*), count(*) filter (where value is not null and value <> 0 and coalesce(sign, 253) <> 253)
            from marked group by 1, 2, 3"""
        ).fetchall()
        shape = con.execute(
            "select territory_slot, (select count(*) from areas), (select count(*) from areas where is_twin), "
            "(select count(*) from areas where geography_key = '_X'), (select max(wordings) from labels) from shape"
        ).fetchone()
        return {"rows": rows, "shape": shape}
    finally:
        con.close()


def checks(cat, con, sample_items: dict) -> dict:
    states = {n: state_of(cat, n) for n in obs.TABLES}
    paths = {n: [p for held in states[n].values() for p in held["paths"]] for n in obs.TABLES}
    o, s, m, g = (files(paths[n]) for n in ("observations", "series", "measures", "geography"))
    one = lambda q: con.execute(q).fetchone()  # noqa: E731
    out: dict = {}
    out["rows"], out["series_keys"], out["blocks"] = one(
        f"select count(*), count(distinct series_key), count(distinct block_id) from {o}"
    )
    # the grain exactly as tests/fact_grain.sql groups it: series_key × date_key over the whole table
    out["duplicate_keys"] = one(
        f"select count(*) from (select 1 from {o} group by series_key, date_key having count(*) > 1)"
    )[0]
    out["null_keys"] = one(
        f"""select count(*) from {o} where series_key is null or measure_key is null or geography_key is null
            or date_key is null or frequency is null or value is null or observation_status_key is null"""
    )[0]
    out["series_rows"], out["series_points"] = one(f"select count(*), sum(points) from {s}")
    out["measures"], out["measure_points"] = one(f"select count(*), sum(points) from {m}")
    out["by_frequency"] = con.execute(
        f"select frequency, count(*), min(date_key)::varchar, max(date_key)::varchar from {o} group by 1 order by 2 desc"
    ).fetchall()
    out["status"] = con.execute(
        f"select observation_status_key, count(*), count(*) filter (where value = 0) from {o} group by 1 order by 2 desc"
    ).fetchall()
    out["geo"] = con.execute(
        f"""select case when geography_key in ('_X', '_Z', 'PL') then geography_key
                else regexp_extract(geography_key, '^[A-Z]+', 0) end, count(*)
            from {o} group by 1 order by 2 desc"""
    ).fetchall()
    out["geography_codes"] = con.execute(
        f"""select case when geography_key in ('_X', '_Z', 'PL') then geography_key
                else regexp_extract(geography_key, '^[A-Z]+', 0) end, count(*), sum(points)
            from {g} group by 1 order by 3 desc"""
    ).fetchall()
    out["unmatched_top"] = con.execute(
        f"""select regexp_replace(geography_source_code, ' #[0-9]+$', ''), sum(points) from {g}
            where geography_key = '_X' group by 1 order by 2 desc limit 12"""
    ).fetchall()
    out["unmatched_kinds"] = con.execute(
        f"""select case when geography_source_code like 'POWIAT %' then 'powiat'
                when regexp_matches(geography_source_code, '^(REGION|MAKROREGION|PODREGION) ') then 'nuts'
                else 'other' end, count(distinct regexp_replace(geography_source_code, ' #[0-9]+$', '')), sum(points)
            from {g} where geography_key = '_X' group by 1 order by 3 desc"""
    ).fetchall()
    out["total_series"], out["blocks_with_total"], out["sex_series"] = one(
        f"""select count(*) filter (where is_total), count(distinct block_id) filter (where is_total),
            count(*) filter (where sex_key <> '_Z') from {s}"""
    )
    out["measures_per_block"] = one(f"select min(n), median(n), max(n) from (select block_id, count(*) as n from {m} group by 1)")
    out["cycle_measures"] = con.execute(
        f"select measure_key, points from {m} where regexp_matches(measure_key, ':[AQSMX]@') order by 2 desc limit 6"
    ).fetchall()
    out["units"] = one(f"select count(distinct unit_key) from {m}")[0]
    out["series_key_sample"] = [r[0] for r in con.execute(f"select series_key from {s} order by hash(series_key) limit 2").fetchall()]
    # the block id of the files equals the block the driver planned (a second run would rebuild otherwise)
    out["block_ids_differ"] = one(
        f"select count(*) from (select distinct block_id from {o}) where block_id not in ({', '.join(repr(b) for b in sample_items)})"
    )[0]
    out["mb"] = {n: round(sum(h["bytes"] for h in states[n].values()) / 1e6, 2) for n in obs.TABLES}
    out["bytes_per_million_rows"] = {
        n: round(sum(h["bytes"] for h in states[n].values()) / max(out["rows"], 1) * 1e6) for n in obs.TABLES
    }
    out["rows_by_block"] = dict(con.execute(f"select block_id, count(*) from {o} group by 1").fetchall())
    return out


def spot(cat, con) -> dict:
    """Newest Polish values: the curated facts (production Gold, read only) against the rule-based rows of
    the same measure, area and period. Equal when the sorted values are the same."""
    from pyiceberg.expressions import And, EqualTo

    o = files([p for held in state_of(cat, "observations").values() for p in held["paths"]])
    out = {}
    for measure, (table, frequency) in SPOT.items():
        try:
            rows = (
                cat.load_table(("gold", table))
                .scan(row_filter=And(EqualTo("measure_key", measure), EqualTo("geography_key", "PL")),
                      selected_fields=("series_key", "period_label", "value", "date_key"))
                .to_arrow().to_pylist()
            )
            if not rows:
                out[measure] = "no curated PL rows"
                continue
            newest = max(r["date_key"] for r in rows)
            curated = sorted(round(r["value"], 6) for r in rows if r["date_key"] == newest)
            mine = con.execute(
                f"""select value, date_key = (select max(date_key) from {o}
                        where measure_key = '{measure}@{frequency}' and geography_key = 'PL')
                    from {o} where measure_key = '{measure}@{frequency}' and geography_key = 'PL'
                    and date_key = cast('{newest}' as date)"""
            ).fetchall()
            ruled = sorted(round(r[0], 6) for r in mine)
            out[measure] = {
                "period": str(newest), "curated": curated[:4], "by_rule": ruled[:4], "values": [len(curated), len(ruled)],
                "equal": curated == ruled, "newest_in_both": bool(mine) and all(r[1] for r in mine),
            }
        except Exception as exc:
            out[measure] = f"{type(exc).__name__}: {str(exc)[:100]}"
    return out


def main() -> None:
    size = int(sys.argv[1]) if len(sys.argv) > 1 else 34
    minutes = float(sys.argv[2]) if len(sys.argv) > 2 else 60
    tmp = "/mnt/duckdb_tmp"
    try:
        subprocess.run(["sudo", "mkdir", "-p", tmp], check=True)
        subprocess.run(["sudo", "chown", str(os.getuid()), tmp], check=True)
    except Exception:
        tmp = "/tmp/duckdb_tmp"
        Path(tmp).mkdir(parents=True, exist_ok=True)
    os.environ["DUCKDB_TEMP_DIRECTORY"] = tmp
    cat, client = catalog(), s3()
    before = cleanup(cat, client)  # leftovers of a killed earlier run
    try:
        silver_table = cat.load_table(obs.SOURCES[SOURCE]["silver"])
        silver = et.state(silver_table, "entity_id")
        dictionary = et.state(cat.load_table(("bronze", "dbw_dictionary_items")), "entity_id")
        blocks = obs.blocks_of(SOURCE, silver, {"dictionary": dictionary})
        # the block count: file stems by the old rule (the year only at the end) against blocks
        old_stems = {obs.DBW_FILE_END.sub("", e) for e in silver if e is not None}
        inside = [e for e in silver if e is not None and obs.DBW_YEAR_INSIDE.match(e)]
        inside_blocks = {obs.dbw_block(e) for e in inside}
        triples = {b.rsplit("_", 1)[0] for b in inside_blocks}
        siblings = sum(1 for b in blocks if b not in inside_blocks and b.rsplit("_", 1)[0] in triples)
        by_size = sorted(blocks, key=lambda b: -blocks[b]["rows"])
        rng = random.Random(20261112)
        by_last: dict[str, list[str]] = {}
        for b in sorted(blocks):
            by_last.setdefault(b.rsplit("_", 1)[-1], []).append(b)
        share = {"4": 9, "1": 6, "3": 5, "282": 4, "5": 4, "2": 3, "7": 3}
        picked = {b for b in ALWAYS if b in blocks}
        for last, names in by_last.items():
            picked |= set(rng.sample(names, min(len(names), max(1, round(share.get(last, 2) * size / 34)))))
        picked |= set(by_size[:1])  # the largest block
        picked |= {by_size[len(by_size) // 100], by_size[len(by_size) // 20]}  # 99th and 95th percentile
        sample = sorted(picked)
        sizes = sorted(blocks[b]["rows"] for b in sample)
        plan = {
            "silver_files": len(silver), "file_stems_old_rule": len(old_stems), "blocks": len(blocks),
            "files_with_year_inside": len(inside), "their_blocks": len(inside_blocks),
            "their_blocks_files_min_max": [
                min(len(blocks[b]["paths"]) for b in inside_blocks), max(len(blocks[b]["paths"]) for b in inside_blocks),
            ] if inside_blocks else [],
            "their_blocks_sample": sorted(inside_blocks)[:3],
            "four_part_blocks_sharing_their_indicator_section_presentation": siblings,
            "blocks_by_last_part": dict(Counter(b.rsplit("_", 1)[-1] for b in blocks).most_common()),
            "blocks_missing_a_dictionary_file": sum(1 for v in blocks.values() if v.get("missing")),
            "files_per_block_max": max(len(v["paths"]) for v in blocks.values()),
            "all_silver_rows": sum(v["rows"] for v in blocks.values()),
            "all_silver_mb": round(sum(v["bytes"] for v in blocks.values()) / 1e6),
            "largest": [[b, blocks[b]["rows"], round(blocks[b]["bytes"] / 1e6)] for b in by_size[:3]],
            "sample": len(sample), "sample_files": sum(len(blocks[b]["paths"]) for b in sample),
            "sample_silver_rows": sum(blocks[b]["rows"] for b in sample),
            "sample_silver_mb": round(sum(blocks[b]["bytes"] for b in sample) / 1e6, 1),
            "sample_rows_min_median_max": [sizes[0], sizes[len(sizes) // 2], sizes[-1]],
            "sample_by_last_part": dict(Counter(b.rsplit("_", 1)[-1] for b in sample).most_common()),
            "cleanup_before": before,
        }
        annotate("notice", "dbw gold trial plan", plan)
        args = ["--source", SOURCE, "--trial", NS, "--minutes", str(minutes), "--only", *sample]
        started = time.monotonic()
        first = obs.main(args)
        build_seconds = time.monotonic() - started
        started = time.monotonic()
        second = obs.main([*args[:5], "10", *args[6:]])  # the same again: nothing to build
        second_seconds = round(time.monotonic() - started)
        con = connect(tmp)
        report = checks(cat, con, {b: blocks[b] for b in sample})
        rows_by_block = report.pop("rows_by_block")
        # rows against Silver, block by block, with the build stage's own preparation steps
        lookup_dir = Path(tmp) / "trial_lookups"
        lookup_dir.mkdir(parents=True, exist_ok=True)
        lookups, _geo = obs.export_lookups(cat, SOURCE, lookup_dir)
        totals, periods, signs, wrong, shapes, kept_signs = Counter(), Counter(), Counter(), [], Counter(), 0
        not_built = Counter()  # Silver rows of the sample's blocks that Gold did not take (rejected whole)
        for b in sample:
            result = account(tmp, blocks[b], lookups["geo_map"])
            built = b in rows_by_block
            loaded = 0
            for outcome, period, sign, n, kept in result["rows"]:
                (totals if built else not_built)[outcome] += n
                if period:
                    periods[period] += n
                if not built:
                    continue
                kept_signs += kept if outcome == "loaded" else 0
                if sign is not None:
                    signs[str(sign)] += n
                loaded += n if outcome == "loaded" else 0
            if built and loaded != rows_by_block[b]:
                wrong.append([b, loaded, rows_by_block[b]])
            slot, areas, twins, unmatched, wordings = result["shape"]
            shapes["no_territorial_slot" if slot is None else "territorial_slot"] += 1
            shapes["blocks_with_twins"] += bool(twins)
            shapes["blocks_with_unmatched_areas"] += bool(unmatched)
            shapes["blocks_with_reworded_labels"] += bool(wordings and wordings > 1)
        valued = sum(n for k, n in totals.items() if k != "empty")
        rejected = (
            cat.load_table((NS, obs.REJECTED[1])).scan().to_arrow().to_pylist()
            if (NS, obs.REJECTED[1]) in cat.list_tables(NS) else []
        )
        silver_mb = sum(blocks[b]["bytes"] for b in sample if b in rows_by_block) / 1e6
        annotate("notice", "dbw gold trial rows against silver", {
            "exit": [first, second], "build_seconds": round(build_seconds), "second_run_seconds": second_seconds,
            "blocks_built": len(rows_by_block), "blocks_not_built": len(sample) - len(rows_by_block),
            "silver_rows_of_blocks_not_built": dict(not_built),
            "silver_rows": sum(totals.values()), "silver_rows_with_a_value": valued, "gold_rows": report["rows"],
            "not_loaded_by_rule": {k: n for k, n in totals.items() if k not in ("loaded", "empty")},
            "silver_empty": totals["empty"], "expected_loaded": totals["loaded"],
            "difference": totals["loaded"] - report["rows"], "blocks_where_rows_differ": wrong[:8],
            "zero_signs_by_id": dict(signs), "numbers_kept_with_a_no_value_sign": kept_signs,
            "periods_not_placed": dict(periods.most_common(14)), "shapes": dict(shapes),
            "seconds_per_million_gold_rows": round(build_seconds / max(report["rows"], 1) * 1e6, 1),
            "gold_rows_per_minute": round(report["rows"] / build_seconds * 60),
            "silver_mb_of_built_blocks": round(silver_mb, 1),
        })
        annotate("notice", "dbw gold trial checks", report)
        annotate("notice", "dbw gold trial newest PL values", spot(cat, con))
        annotate("notice", "dbw gold trial rejected", {
            "count": len(rejected), "reasons": dict(Counter(r["error"][:90] for r in rejected).most_common(6)),
            "blocks": [r["entity"].split(":")[-1] for r in rejected][:30],
        })
    finally:
        if "keep" not in sys.argv:
            annotate("notice", "dbw gold trial cleanup", cleanup(cat, client))


if __name__ == "__main__":
    main()
