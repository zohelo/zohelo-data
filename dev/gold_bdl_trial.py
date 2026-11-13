"""Dev trial (scratch only): Gold observations of a sample of GUS BDL subgroups, built by
warehouse/observations.py into the namespace and prefix zz_dev_gold_bdl, checked, then removed.
Reads production Silver, gold.map_geography, gold.coverage and the curated facts; writes nothing outside
the scratch. Deleted before merge.

python -m dev.gold_bdl_trial [minutes, default 150] [keep]
"""

from __future__ import annotations

import contextlib
import io
import json
import os
from pathlib import Path
import random
import subprocess
import sys
import tempfile
import time

import duckdb

from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from warehouse import observations as obs

NS = "zz_dev_gold_bdl"
SOURCE = "gus_bdl"
# curated KPI subgroups (poc_bdl_blocks) and subgroups loaded through the API (ingestion/sources/gus_bdl.yaml)
ALWAYS = ["P2137", "P1873", "P2425", "P2392", "P2497", "P3824", "P1410", "P2961", "P2955", "P1869", "P3502",
          "P2346", "P3788", "P2730", "P2359", "P2496", "P2692", "P3794", "P2837"]
API = ["P4624", "P3804", "P4058", "P3433", "P3475", "P2324"]
NO_VALUE = ("value = 0 and ((channel = 'web_export' and coalesce(lower(attribute), '') in ('x', 'n')) "
            "or (channel = 'api' and coalesce(attribute, '') in ('4', '14', '50', '91')))")
CUMULATIVE = r"^([1-4]-[1-4] kwartał|.*półrocz|styczeń-)"


def connect(tmp: str):
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '9GB'; SET preserve_insertion_order = false; SET temp_directory = '{tmp}'")
    con.execute("INSTALL httpfs; LOAD httpfs")
    endpoint = req("STORAGE_ENDPOINT_URL").replace("https://", "").rstrip("/")
    con.execute(
        "CREATE SECRET r2 (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION 'auto', URL_STYLE 'path')",
        [req("STORAGE_WRITE_ACCESS_KEY"), req("STORAGE_WRITE_SECRET_KEY"), endpoint],
    )
    return con


def paths_of(cat, name: str) -> list[str]:
    table = cat.load_table((NS, f"{SOURCE}_{name}"))
    return [p for held in et.state(table, "block_id").values() for p in held["paths"]]


def files(paths: list[str]) -> str:
    return "read_parquet([" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "])"


def cleanup(cat, client) -> dict:
    gone = {"tables": 0, "objects": 0}
    for name in [*(f"{SOURCE}_{t}" for t in obs.TABLES), obs.REJECTED[1]]:
        with contextlib.suppress(Exception):
            cat.drop_table((NS, name))
            gone["tables"] += 1
    with contextlib.suppress(Exception):
        cat.drop_namespace(NS)
    bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME")
    pages = client.get_paginator("list_objects_v2")
    for page in pages.paginate(Bucket=bucket, Prefix=NS + "/"):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            client.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
            gone["objects"] += len(keys)
    left = sum(len(p.get("Contents", [])) for p in pages.paginate(Bucket=bucket, Prefix=NS + "/"))
    return {**gone, "objects_left": left, "namespace_left": (NS,) in cat.list_namespaces()}


def build(args: list[str]) -> tuple[int, dict]:
    """One builder run; its exit code and the summary line it prints."""
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = obs.main(args)
    lines = [line for line in out.getvalue().splitlines() if line.startswith("{") and '"operation"' in line]
    kept = [line for line in out.getvalue().splitlines() if not line.startswith("::")]
    print("\n".join(kept)[-3000:], flush=True)
    return code, json.loads(lines[-1]) if lines else {}


def pick(silver: dict, ranks: dict[str, int], projections: list[str]) -> list[str]:
    """At least 40 subgroups of different sizes and levels: per level group (to voivodeship, to powiat,
    to gmina) four from each third by Silver rows, the curated and API subgroups, and the two largest."""
    rng = random.Random(20261112)
    picked = {b for b in [*ALWAYS, *API, *projections] if b in silver}
    for group in (0, 1, 2):
        blocks = sorted((b for b in silver if ranks.get(b) == group), key=lambda b: silver[b]["rows"])
        third = max(1, len(blocks) // 3)
        for part in (blocks[:third], blocks[third : 2 * third], blocks[2 * third :]):
            picked |= set(rng.sample(part, min(4, len(part))))
    picked |= set(sorted(silver, key=lambda b: -silver[b]["rows"])[:2])
    return sorted(picked)


def checks(cat, con, silver_paths: list[str]) -> dict:
    o, s, m, g = (files(paths_of(cat, n)) for n in ("observations", "series", "measures", "geography"))
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
            or date_key is null or frequency is null or value is null"""
    )[0]
    out["series_rows"], out["series_points"] = one(f"select count(*), sum(points) from {s}")
    out["measures"], out["measure_points"] = one(f"select count(*), sum(points) from {m}")
    out["by_frequency"] = con.execute(
        f"select frequency, count(*), min(date_key)::varchar, max(date_key)::varchar from {o} group by 1 order by 2 desc"
    ).fetchall()
    out["status"] = con.execute(
        f"select observation_status_key, count(*), count(*) filter (where value = 0) from {o} group by 1 order by 2 desc"
    ).fetchall()
    out["forecast_blocks"] = con.execute(
        f"""select block_id, count(*), min(date_key)::varchar, max(date_key)::varchar from {o}
            where observation_status_key = 'forecast' group by 1 order by 2 desc limit 8"""
    ).fetchall()
    out["geo"] = con.execute(
        f"""select case when geography_key = 'PL' then 'PL'
                when geography_key like 'TERC:%' then 'TERC' || cast(length(geography_key) - 5 as varchar)
                when geography_key like 'NUTS:%' then 'NUTS' when geography_key like 'BDL:%' then 'BDL podregion'
                else geography_key end, count(*)
            from {o} group by 1 order by 2 desc"""
    ).fetchall()
    kind = """case when geography_key <> '_X' then 'mapped'
                when geography_source_code like '%#%#%' then 'another name under the code'
                when geography_source_code like '%#%' and length(split_part(geography_source_code, '#', 1)) = 7
                    then 'TERYT code beside the unit id'
                when geography_source_code like '%#%' and (substr(geography_source_code, 6, 7) = '0000000'
                    or substr(geography_source_code, 3, 10) = '0000000000') then 'region or macroregion on a voivodeship'
                when geography_source_code like '%#%' then 'unit id beside another unit id of the area'
                when length(geography_source_code) = 7 then 'TERYT code not in the map'
                else 'unit id not in the map' end"""
    out["geography_codes (kind, block x code, codes, points)"] = con.execute(
        f"select {kind}, count(*), count(distinct geography_source_code), sum(points) from {g} group by 1 order by 4 desc"
    ).fetchall()
    out["unmatched_share_of_rows"] = round(one(f"select count(*) filter (where geography_key = '_X') / count(*) from {o}")[0], 4)
    out["other_names_under_a_code"] = con.execute(
        f"select block_id, geography_source_code, points from {g} where geography_source_code like '%#%#%' order by 3 desc limit 6"
    ).fetchall()
    # rows of a unit that does not hold its area, against the rows of the unit that does (same series, same period)
    out["twins (kind, rows, holder has the cell, same value, other value)"] = con.execute(
        f"""with d as (
                select series_key, {kind} as kind,
                    measure_key || '|' || split_part(geography_source_code, '#', 2) || '|' || breakdown_json as holder_key
                from {s} where geography_source_code like '%#%'
            )
            select d.kind, count(*), count(h.value), count(*) filter (where h.value = t.value),
                count(*) filter (where h.value <> t.value)
            from {o} as t inner join d on t.series_key = d.series_key
            left join {o} as h on h.series_key = d.holder_key and h.date_key = t.date_key
            group by 1 order by 2 desc"""
    ).fetchall()
    out["not_in_map_top"] = con.execute(
        f"""select geography_source_code, count(*), sum(points) from {g}
            where geography_key = '_X' and geography_source_code not like '%#%' group by 1 order by 3 desc limit 12"""
    ).fetchall()
    out["stand_in_sample"] = con.execute(
        f"""select geography_source_code, count(*) from {g} where geography_source_code like '%#%'
            group by 1 order by 2 desc limit 4"""
    ).fetchall()
    out["measures_per_block"] = con.execute(
        f"select min(n), median(n), max(n) from (select block_id, count(*) as n from {m} group by 1)"
    ).fetchone()
    out["units_top"] = con.execute(f"select unit_key, sum(points) from {m} group by 1 order by 2 desc limit 8").fetchall()
    out["total_series"], out["blocks_with_total"] = one(
        f"select count(*) filter (where is_total), count(distinct block_id) filter (where is_total) from {s}"
    )
    # the Silver side, counted here from the sampled subgroups' files (not by the builder)
    sv = files(silver_paths)
    (out["silver_rows"], out["silver_valued"], out["silver_no_value_sign"], out["silver_cumulative"]) = one(
        f"""select count(*), count(*) filter (where value is not null),
                count(*) filter (where {NO_VALUE}),
                count(*) filter (where value is not null and not ({NO_VALUE})
                    and regexp_matches(lower(trim(json_extract_string(dimension_values, '$[0]'))), '{CUMULATIVE}'))
            from {sv}"""
    )
    out["silver_expected_gold"] = out["silver_valued"] - out["silver_no_value_sign"] - out["silver_cumulative"]
    out["rows_minus_expected"] = out["rows"] - out["silver_expected_gold"]
    out["no_value_signs"] = con.execute(
        f"select channel, attribute, count(*), max(abs(value)) from {sv} where {NO_VALUE} group by 1, 2 order by 3 desc"
    ).fetchall()
    out["signs_kept"] = con.execute(
        f"""select channel, attribute, count(*), count(*) filter (where value = 0) from {sv}
            where value is not null and attribute is not null and not ({NO_VALUE}) group by 1, 2 order by 3 desc limit 14"""
    ).fetchall()
    out["cumulative_labels"] = con.execute(
        f"""select json_extract_string(dimension_values, '$[0]'), count(*) from {sv} where value is not null
            and regexp_matches(lower(trim(json_extract_string(dimension_values, '$[0]'))), '{CUMULATIVE}')
            group by 1 order by 2 desc limit 10"""
    ).fetchall()
    out["channels"] = con.execute(f"select channel, unit_code_scheme, count(*) from {sv} group by 1, 2 order by 3 desc").fetchall()
    out["mb"] = {
        n: round(sum(h["bytes"] for h in et.state(cat.load_table((NS, f"{SOURCE}_{n}")), "block_id").values()) / 1e6, 1)
        for n in obs.TABLES
    }
    return out


def curated(cat, con, sample: list[str]) -> dict:
    """The curated facts' newest value per Polish series of the sampled subgroups (production Gold,
    read-only) against the same series of the trial. A rule key is the curated key plus `@<frequency>`
    (and without the curated stage's `rok` marker)."""
    from pyiceberg.expressions import And, EqualTo, StartsWith

    fields = ("series_key", "measure_key", "date_key", "frequency", "value", "period_label")
    found = []
    for table in ("fact_annual", "fact_quarterly", "fact_monthly"):
        rows = (
            cat.load_table(("gold", table))
            .scan(row_filter=And(StartsWith("measure_key", "gus_bdl:"), EqualTo("geography_key", "PL")), selected_fields=fields)
            .to_arrow()
        )
        con.register(f"c_{table}", rows)
        found.append(f"select * from c_{table}")
    blocks = ", ".join(f"'{b}'" for b in sample)
    o = files(paths_of(cat, "observations"))
    con.execute(
        f"""create temp table curated_newest as
            with c as ({' union all '.join(found)}),
            keyed as (
                select *, split_part(measure_key, ':', 2) as block_id,
                    regexp_replace(measure_key, '(:| / )rok$', '') || '@' || frequency
                        || substr(series_key, length(measure_key) + 1) as rule_series_key
                from c
            )
            select * from keyed where block_id in ({blocks})
            qualify date_key = max(date_key) over (partition by series_key)"""
    )
    con.execute(
        f"""create temp table compared as
            select c.block_id, c.series_key, c.rule_series_key, c.period_label, c.date_key, c.value as curated_value,
                t.value as trial_value
            from curated_newest as c
            left join (select series_key, date_key, value from {o} where geography_key = 'PL') as t
                on c.rule_series_key = t.series_key and c.date_key = t.date_key"""
    )
    by_block = con.execute(
        """select block_id, count(*), count(trial_value), count(*) filter (where trial_value = curated_value),
               count(*) filter (where trial_value <> curated_value)
           from compared group by 1 order by 1"""
    ).fetchall()
    spot = con.execute(
        """select block_id, substr(rule_series_key, 1, 110), period_label, curated_value, trial_value from compared
           where trial_value is not null
           qualify row_number() over (partition by block_id order by length(rule_series_key), rule_series_key) = 1
           order by 1 limit 12"""
    ).fetchall()
    differ = con.execute(
        """select substr(rule_series_key, 1, 110), period_label, curated_value, trial_value from compared
           where trial_value <> curated_value limit 4"""
    ).fetchall()
    missing = con.execute(
        "select substr(rule_series_key, 1, 110), period_label, curated_value from compared where trial_value is null limit 4"
    ).fetchall()
    totals = con.execute(
        """select count(*), count(trial_value), count(*) filter (where trial_value = curated_value),
               count(*) filter (where trial_value <> curated_value), count(distinct block_id) from compared"""
    ).fetchone()
    return {"totals_series_found_equal_differ_blocks": totals, "by_block": by_block, "spot": spot,
            "differ": differ, "missing": missing}


def main() -> None:
    minutes = float(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1] != "keep" else 150
    tmp = "/mnt/duckdb_tmp"
    try:
        subprocess.run(["sudo", "mkdir", "-p", tmp], check=True)
        subprocess.run(["sudo", "chown", str(os.getuid()), tmp], check=True)
    except Exception:
        tmp = "/tmp/duckdb_tmp"
        Path(tmp).mkdir(parents=True, exist_ok=True)
    os.environ["DUCKDB_TEMP_DIRECTORY"] = tmp
    cat, client = catalog(), s3()
    annotate("notice", "bdl gold trial cleanup before", cleanup(cat, client))  # leftovers of a killed earlier run
    try:
        silver = et.state(cat.load_table(obs.SOURCES[SOURCE]["silver"]), "subgroup_id")
        silver = {str(b): v for b, v in silver.items() if b is not None}
        with tempfile.TemporaryDirectory(dir=tmp) as directory:
            lookups, geo_map = obs.export_lookups(cat, SOURCE, Path(directory))
            ranks = obs.load_ranks(SOURCE, lookups)
            named = duckdb.sql(
                f"select block_id, block_name from read_parquet('{lookups['names']}') where lower(block_name) like '%prognoz%' order by 1"
            ).fetchall()
        projection_names = {b: n[:90] for b, n in named if b in silver}
        projections = sorted(projection_names, key=lambda b: silver[b]["rows"])
        sample = pick(silver, ranks, projections[:2])
        ids = [k for c, k in geo_map.items() if len(c) == 12]
        silver_paths = [p for b in sample for p in silver[b]["paths"]]
        groups = {}
        for b, held in silver.items():
            item = groups.setdefault(ranks.get(b, 3), {"blocks": 0, "silver_rows": 0, "mb": 0.0, "sample": 0})
            item["blocks"] += 1
            item["silver_rows"] += held["rows"]
            item["mb"] = round(item["mb"] + held["bytes"] / 1e6, 1)
            item["sample"] += b in sample
        sizes = sorted(silver[b]["rows"] for b in sample)
        plan = {
            "subgroups_in_silver": len(silver),
            "geo_map_codes": len(geo_map),
            "unit_ids_in_map_and_unmatched": [len(ids), sum(k == "_X" for k in ids)],
            "subgroups_named_projection": projection_names,
            "load_order_groups (0 to voivodeship and subregion, 1 to powiat, 2 to gmina, 3 level unknown)": groups,
            "sample": len(sample),
            "sample_silver_rows": sum(sizes),
            "sample_silver_mb": round(sum(silver[b]["bytes"] for b in sample) / 1e6, 1),
            "sample_rows_min_median_max": [sizes[0], sizes[len(sizes) // 2], sizes[-1]],
            "largest": [[b, silver[b]["rows"], round(silver[b]["bytes"] / 1e6, 1)]
                        for b in sorted(silver, key=lambda b: -silver[b]["rows"])[:5]],
            "sample_blocks": " ".join(sample),
        }
        annotate("notice", "bdl gold trial plan", plan)
        args = ["--source", SOURCE, "--trial", NS, "--minutes", str(minutes), "--only", *sample]
        started = time.monotonic()
        first_exit, first = build(args)
        build_seconds = time.monotonic() - started
        started = time.monotonic()
        second_exit, second = build([*args[:5], "10", *args[6:]])  # the same again: nothing to build
        second_seconds = round(time.monotonic() - started)
        annotate("notice", "bdl gold trial runs", {
            "first_exit": first_exit, "second_exit": second_exit, "build_minutes": round(build_seconds / 60, 1),
            "second_run_seconds": second_seconds,
            "second_run": {k: second.get(k) for k in ("todo", "built", "rows", "remaining", "rejected_earlier", "result")},
            "first_run": {k: v for k, v in first.items() if k not in ("operation",)},
        })
        con = connect(tmp)
        report = checks(cat, con, silver_paths)
        rows = report["rows"] or 1
        report["seconds_per_million_rows"] = round(build_seconds / rows * 1e6, 1)
        report["bytes_per_million_rows"] = round(sum(report["mb"].values()) * 1e6 / rows * 1e6)
        report["builder_identity"] = {
            "silver_rows_of_built_blocks": first.get("silver_rows"), "gold_rows": first.get("rows"),
            "not_loaded": first.get("not_loaded"),
            "difference": (first.get("silver_rows") or 0) - (first.get("rows") or 0)
            - sum((first.get("not_loaded") or {}).values()),
        }
        geo_keys = ("geo", "geography_codes (kind, block x code, codes, points)", "unmatched_share_of_rows",
                "other_names_under_a_code", "twins (kind, rows, holder has the cell, same value, other value)",
                "not_in_map_top", "stand_in_sample")
        cell_keys = ("no_value_signs", "signs_kept", "cumulative_labels", "channels", "status", "forecast_blocks")
        annotate("notice", "bdl gold trial checks", {k: v for k, v in report.items() if k not in (*geo_keys, *cell_keys)})
        annotate("notice", "bdl gold trial geography", {k: report[k] for k in geo_keys})
        annotate("notice", "bdl gold trial cells", {k: report[k] for k in cell_keys})
        try:
            annotate("notice", "bdl gold trial curated", curated(cat, con, sample))
        except Exception as exc:  # the comparison must not lose the other results
            annotate("warning", "bdl gold trial curated", {"error": f"{type(exc).__name__}: {str(exc)[:600]}"})
        listed = (NS, obs.REJECTED[1]) in cat.list_tables(NS)
        rejected = cat.load_table((NS, obs.REJECTED[1])).scan().to_arrow().to_pylist() if listed else []
        annotate("notice", "bdl gold trial rejected",
                 {"count": len(rejected), "sample": [[r["entity"], r["error"][:220]] for r in rejected[:12]]})
    finally:
        if "keep" not in sys.argv:
            annotate("notice", "bdl gold trial cleanup", cleanup(cat, client))


if __name__ == "__main__":
    main()
