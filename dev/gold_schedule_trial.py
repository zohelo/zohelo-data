"""Dev trial (scratch only): the scheduled Eurostat per-block Gold load, proven on R2 before it runs in production.

Runs warehouse/observations.py into the scratch namespace and prefix zz_dev_gold_schedule over about 18 small
datasets and shows, on R2: the first-load guard, the load order groups (O43), a second run with nothing to do,
a run interrupted between the four table commits and the run after it, a geography remap (the Polish NUTS 3
codes of this branch's Eurostat geography macro) that rebuilds only the blocks holding such a code, the removal
cap (refused, then allowed), and the coverage numbers. Reads production Silver, gold.map_geography,
gold.dim_geography and gold.build_eurostat; writes nothing outside the scratch, which it removes. Three things
are simulated in this process, because production must not change: the geography map the builder reads (a
local copy, old or new), the Silver listing (blocks hidden from it), and the interruption (the second table
commit raises). Deleted before merge.

python -m dev.gold_schedule_trial [keep]
"""

from __future__ import annotations

import contextlib
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time

import duckdb

from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from warehouse import observations as obs

NS = "zz_dev_gold_schedule"
SOURCE = "eurostat"
ROOT = Path(__file__).resolve().parent.parent
NUTS3 = ["DEMO_R_PJANAGGR3", "DEMO_R_D3DENS", "DEMO_R_D3AREA", "NAMA_10R_3POPGDP", "DEMO_R_GIND3", "NAMA_10R_3GDP"]
COUNTRIES = ["UNE_RT_M", "EARN_MW_CUR", "DEMO_PJAN"]
SAMPLE = 18
MAX_ROWS = 20_000_000
PL_NUTS3 = re.compile(r"^PL[0-9A-Z]{3}$")
STATE: dict = {"map": None, "hidden": set(), "interrupt": False, "replace_calls": 0}


def connect(remote: bool = True):
    con = duckdb.connect()
    con.execute("SET memory_limit = '6GB'; SET preserve_insertion_order = false")
    if remote:
        con.execute("INSTALL httpfs; LOAD httpfs")
        endpoint = req("STORAGE_ENDPOINT_URL").replace("https://", "").rstrip("/")
        con.execute(
            "CREATE SECRET r2 (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION 'auto', URL_STYLE 'path')",
            [req("STORAGE_WRITE_ACCESS_KEY"), req("STORAGE_WRITE_SECRET_KEY"), endpoint],
        )
    return con


def files(paths: list[str]) -> str:
    return "read_parquet([" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "])"


# ----------------------------------------------------------------------------- the three simulations
def install() -> None:
    """Patches of this process only (workers are separate processes and are not patched)."""
    real_lookups, real_blocks, real_replace = obs.export_lookups, obs.blocks_of, et.replace

    def lookups(cat, source, directory):
        import pyarrow as pa
        import pyarrow.parquet as pq

        paths, geo_map = real_lookups(cat, source, directory)
        if STATE["map"] is None:
            return paths, geo_map
        table = pa.table({"source_code": list(STATE["map"]), "geography_key": list(STATE["map"].values())})
        pq.write_table(table, paths["geo_map"])
        return paths, dict(STATE["map"])

    def blocks(source, silver_state):
        return {b: v for b, v in real_blocks(source, silver_state).items() if b not in STATE["hidden"]}

    def replace(*args, **kwargs):
        if STATE["interrupt"]:
            STATE["replace_calls"] += 1
            if STATE["replace_calls"] == 2:  # the first table (series) is committed, the other three are not
                raise RuntimeError("trial: the run is interrupted between the table commits")
        return real_replace(*args, **kwargs)

    obs.export_lookups, obs.blocks_of, et.replace = lookups, blocks, replace
    obs.annotate = lambda *a, **k: None  # the trial writes its own compact annotations


def run(*extra: str) -> dict:
    out = obs.run(["--source", SOURCE, "--trial", NS, "--minutes", "40", *extra])
    keep = ("result", "todo", "todo_by_group", "built_by_group", "built", "rows", "series", "remaining", "removed",
            "removal_refused", "remapped_geography", "rejected", "failed", "minutes", "reason")
    brief = {k: out[k] for k in keep if k in out and out[k] not in (0, [], {}, None)}
    for key in ("todo_by_group", "built_by_group"):  # as [Polish regions, countries and aggregates, other]
        if key in brief:
            brief[key] = list(brief[key].values())
            if not any(brief[key]):
                del brief[key]
    brief["result"] = out["result"]
    if "coverage" in out:
        c = out["coverage"]
        brief["coverage"] = [c["blocks_in_gold"], c["blocks_in_silver"], c["series_in_gold"], c["loaded_series"],
                             c["coverage_pct"]]
    for key in ("rejected_blocks", "failed_blocks"):
        if out.get(key):
            brief[key] = out[key][:3]
    return brief


# ----------------------------------------------------------------------------- the new geography map
def macro_body(path: str, name: str) -> str:
    text = (ROOT / path).read_text(encoding="utf-8")
    return re.search(r"\{%-?\s*macro\s+" + name + r"\(\)\s*-?%\}(.*?)\{%-?\s*endmacro", text, re.S).group(1)


def eurostat_map_sql() -> str:
    """The Eurostat rows of poc_map_geography as this branch builds them: the CTEs of poc_geography__eurostat()
    (warehouse/macros/sources/eurostat.sql) and the model's own final select, over the relations `geo`
    (gold.dim_geography) and `eurostat_codes_table` (silver.eurostat_codes)."""
    ctes = macro_body("warehouse/macros/sources/eurostat.sql", "poc_geography__eurostat")
    ctes = ctes.replace("{{ poc_nuts_pl() }}", macro_body("warehouse/macros/poc_blocks.sql", "poc_nuts_pl"))
    ctes = ctes.replace("{{ ref('eurostat_codes') }}", "eurostat_codes_table")
    if "{{" in ctes or "{%" in ctes:
        raise RuntimeError("the Eurostat geography macro uses Jinja this trial does not render")
    return f"""with {ctes.strip()}
        select m.source_code, coalesce(g.geography_key, '_X') as geography_key
        from eurostat as m
        left join geo as g on m.geography_key = g.geography_key and g.geography_key <> '_X'
        where m.source_code is not null
        qualify row_number() over (partition by m.source_code order by g.geography_key nulls last) = 1"""


def new_map(con, geo, codes_path: str, old: dict[str, str]) -> tuple[dict[str, str], dict]:
    """The map of this branch (code -> area) and how it differs from the map production holds."""
    con.register("geo", geo)
    con.execute(f"create or replace view eurostat_codes_table as select * from read_parquet('{codes_path}')")
    new = dict(con.execute(eurostat_map_sql()).fetchall())
    changed = sorted(c for c in set(old) | set(new) if old.get(c) != new.get(c))
    pl3 = sorted(c for c in new if PL_NUTS3.match(c))
    names = dict(con.execute(
        "select code, any_value(name_en) from eurostat_codes_table where codelist_id = 'GEO' group by 1"
    ).fetchall())
    report = {
        "codes_old": len(old),
        "codes_new": len(new),
        "changed": len(changed),
        "changed_not_polish_nuts3": [[c, old.get(c), new.get(c)] for c in changed if not PL_NUTS3.match(c)][:10],
        "polish_nuts3_codes": len(pl3),
        "polish_nuts3_mapped": sum(1 for c in pl3 if new[c] != "_X"),
        "polish_nuts3_unmatched": [[c, names.get(c)] for c in pl3 if new[c] == "_X"][:20],
        "podregions_used_twice": len([c for c in pl3 if new[c] != "_X"]) - len({new[c] for c in pl3 if new[c] != "_X"}),
        "podregions_in_dim_geography": con.execute("select count(*) from geo where level_name = 'podregion'").fetchone()[0],
        "sample": [[c, names.get(c), new[c]] for c in pl3[:3]],
    }
    return new, report


# ----------------------------------------------------------------------------- scratch state
def tables_state(cat) -> dict[str, dict]:
    out = {}
    for name, table_id in obs.table_ids(SOURCE, NS).items():
        try:
            out[name] = et.state(cat.load_table(table_id), obs.ENTITY)
        except Exception:
            out[name] = {}
    return out


def objects(client, prefix: str = "") -> dict[str, str]:
    bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME")
    found = {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{NS}/{prefix}"):
        found.update({o["Key"]: o["ETag"] for o in page.get("Contents", [])})
    return found


def of_blocks(found: dict[str, str], blocks: list[str]) -> dict[str, str]:
    return {k: v for k, v in found.items() if k.split("/")[-2] in blocks}


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
    production = {t[1] for t in cat.list_tables("gold")}
    return {
        **gone,
        "objects_left": len(objects(client)),
        "namespace_left": (NS,) in cat.list_namespaces(),
        "production_gold_tables_of_the_load": sorted(
            production & {*(f"{SOURCE}_{t}" for t in obs.TABLES), obs.REJECTED[1]}
        ),
    }


def pick(silver: dict, no_area: set[str]) -> dict[str, list[str]]:
    """About SAMPLE small blocks: Polish NUTS 3 datasets, country datasets, two without a geography dimension,
    and the smallest others."""
    rows = {str(e).upper(): h["rows"] for e, h in silver.items() if e is not None}
    small = sorted((b for b in rows if 1_000 <= rows[b] <= MAX_ROWS), key=lambda b: (rows[b], b))
    nuts3 = [b for b in NUTS3 if b in rows and rows[b] <= MAX_ROWS][:4]
    countries = [b for b in COUNTRIES if b in rows and rows[b] <= MAX_ROWS]
    nogeo = [b for b in small if b in no_area][:2]
    taken = {*nuts3, *countries, *nogeo}
    fill = [b for b in small if b not in taken and b not in no_area][: max(0, SAMPLE - len(taken))]
    return {"nuts3": nuts3, "countries": countries, "nogeo": nogeo, "fill": fill, "rows": rows}


def main() -> None:
    tmp = "/mnt/duckdb_tmp"
    try:
        subprocess.run(["sudo", "mkdir", "-p", tmp], check=True)
        subprocess.run(["sudo", "chown", str(os.getuid()), tmp], check=True)
    except Exception:
        tmp = tempfile.mkdtemp(prefix="duckdb_tmp")
    os.environ["DUCKDB_TEMP_DIRECTORY"] = tmp
    cat, client = catalog(), s3()
    annotate("notice", "schedule trial: cleanup before", cleanup(cat, client))  # leftovers of a killed earlier run
    install()
    try:
        scenario(cat, client, tmp)
    finally:
        if "keep" not in sys.argv:
            annotate("notice", "schedule trial: cleanup", cleanup(cat, client))


def scenario(cat, client, tmp: str) -> None:
    con = connect()
    lookup_dir = Path(tempfile.mkdtemp(prefix="trial-lookups-", dir=tmp))
    lookups, old_map = obs.export_lookups(cat, SOURCE, lookup_dir)  # production map, as the builder reads it
    no_area = obs.without_geography(SOURCE, lookups) or set()
    silver = et.state(cat.load_table(obs.SOURCES[SOURCE]["silver"]), "dataset_id")
    sample = pick(silver, no_area)
    everything = [*sample["nuts3"], *sample["countries"], *sample["nogeo"], *sample["fill"]]
    first = [b for b in (sample["nuts3"][:1] + sample["countries"][:1] + sample["nogeo"][:1])]
    interrupted = [*sample["nuts3"][1:2], *sample["fill"][-2:]][:2]
    rest = [b for b in everything if b not in interrupted]
    geo_lists = con.execute(
        f"""select codelist_id, count(distinct dataset_id) from read_parquet('{lookups["dims"]}')
            where lower(dimension_id) = 'geo' group by 1 order by 2 desc limit 4"""
    ).fetchall()
    todo = [{"block": b, "rows": sample["rows"][b], "paths": [p for e, h in silver.items() if str(e).upper() == b
                                                             for p in h["paths"]]} for b in everything]
    started = time.monotonic()
    groups = obs.load_order(SOURCE, todo, old_map, 600, no_area)
    annotate("notice", "schedule trial: sample and load order", {
        "blocks_in_silver": len(silver),
        "blocks_without_geography_dimension": len(no_area & set(sample["rows"])),
        "geo_dimension_code_lists": geo_lists,  # one list for all datasets: the structure cannot order the load
        "sample": {k: sample[k] for k in ("nuts3", "countries", "nogeo")},
        "fill": len(sample["fill"]),
        "silver_rows": sum(sample["rows"][b] for b in everything),
        "group_of": {obs.GROUPS[g]: sorted(b for b in groups if groups[b] == g)[:9] for g in range(3)},
        "group_sizes": [sum(1 for b in groups if groups[b] == g) for g in range(3)],
        "order_read_seconds": round(time.monotonic() - started, 1),
    })

    # 1. guard, first load by hand, continuation, nothing to do
    steps = {"no filter, empty Gold": run("--continue-only", "--only", *rest)}
    steps["tables after the guard"] = sorted(t[1] for t in cat.list_tables(NS)) if (NS,) in cat.list_namespaces() else []
    steps["first, by hand"] = run("--only", *first)
    steps["continued"] = run("--continue-only", "--only", *rest)
    started = time.monotonic()
    steps["again"] = run("--continue-only", "--only", *rest)
    steps["again_seconds"] = round(time.monotonic() - started)
    annotate("notice", "schedule trial: guard, first run, continuation", steps)

    # 2. interruption between the table commits, then the next run
    STATE.update(interrupt=True, replace_calls=0)
    try:
        run("--only", *interrupted)
        stopped = "the run did not stop"
    except RuntimeError as exc:
        stopped = str(exc)
    STATE["interrupt"] = False
    state = tables_state(cat)
    held_after_stop = {name: sorted(b for b in interrupted if b in state[name]) for name in state}
    before = of_blocks(objects(client, "gold/"), interrupted)
    referenced = {p.split("/", 3)[3] for name in state for b in interrupted for p in state[name].get(b, {}).get("paths", [])}
    resumed = run("--only", *interrupted)
    state = tables_state(cat)
    after = of_blocks(objects(client, "gold/"), interrupted)
    paths = {name: [p for b in interrupted for p in state[name].get(b, {}).get("paths", [])] for name in state}
    annotate("notice", "schedule trial: interruption", {
        "blocks": interrupted,
        "stopped": stopped,
        "tables_holding_the_blocks_after_the_stop": held_after_stop,
        "objects_after_the_stop": len(before),
        "objects_a_table_pointed_to": len(referenced),
        "next_run": resumed,
        "objects_after_the_next_run": len(after),
        "earlier_objects_missing": sorted(set(before) - set(after)),
        "earlier_objects_with_other_bytes": sorted(k for k in before if k in after and before[k] != after[k]),
        "new_objects": len(set(after) - set(before)),
        "tables_holding_the_blocks": {name: len(p) for name, p in paths.items()},
        "one_recipe_in_all_tables": all(
            len({obs.recipe_of_file(state[name][b]["paths"][0]) for name in state if b in state[name]}) == 1
            for b in interrupted
        ),
        "referenced_files_exist": all(p.split("/", 3)[3] in after for ps in paths.values() for p in ps),
    })

    # 3. geography remap: the Polish NUTS 3 codes of this branch's macro
    geo = cat.load_table(("gold", "dim_geography")).scan(
        selected_fields=("geography_key", "level_name", "parent_key", "name_pl")
    ).to_arrow()
    mapped, map_report = new_map(connect(remote=False), geo, lookups["codes"], old_map)
    state = tables_state(cat)
    g_before = con.execute(
        f"""select block_id, sum(points), sum(points) filter (where geography_key = '_X'),
                   count(*) filter (where geography_key = '_X' and regexp_matches(geography_source_code, '^PL[0-9A-Z]{{3}}$'))
            from {files([p for h in state['geography'].values() for p in h['paths']])} group by 1"""
    ).fetchall()
    with_pl3 = sorted(b for b, _, _, n in g_before if n)
    obs_paths = {b: h["paths"] for b, h in state["observations"].items()}
    STATE["map"] = mapped
    remap = run("--only", *everything)
    again = run("--only", *everything)
    state = tables_state(cat)
    g_after = con.execute(
        f"""select sum(points), sum(points) filter (where geography_key = '_X') from
            {files([p for h in state['geography'].values() for p in h['paths']])}"""
    ).fetchone()
    annotate("notice", "schedule trial: geography remap", {
        "map": map_report,
        "blocks_holding_an_unmatched_polish_nuts3_code": with_pl3,
        "run_with_the_new_map": remap,
        "rebuilt": sorted(b for b in obs_paths if state["observations"].get(b, {}).get("paths") != obs_paths[b]),
        "run_again": again,
        "points": {"all": sum(r[1] for r in g_before), "unmatched_before": sum(r[2] or 0 for r in g_before),
                   "unmatched_after": g_after[1] or 0},
    })

    # 4. removal cap: blocks hidden from the Silver listing
    held = len(state["observations"])
    STATE["hidden"] = set([b for b in everything if b not in first][:12])
    refused = run("--only", *everything)
    held_refused = len(tables_state(cat)["observations"])
    STATE["hidden"] = set(sample["fill"][:2])
    removed = run("--only", *everything)
    state = tables_state(cat)
    left = of_blocks(objects(client, "gold/"), sorted(STATE["hidden"]))
    annotate("notice", "schedule trial: removal cap", {
        "blocks_held": held,
        "allowed_to_remove": max(obs.MIN_REMOVABLE, int(held * obs.MAX_REMOVE_SHARE)),
        "12_blocks_left_silver": refused,
        "blocks_held_after_the_refusal": held_refused,
        "2_blocks_left_silver": removed,
        "blocks_held_after_the_removal": {name: len(s) for name, s in state.items()},
        "removed_blocks_still_in_a_table": sorted(b for b in STATE["hidden"] for s in state.values() if b in s),
        "their_objects_left_for_orphan_files": len(left),
    })
    STATE["hidden"] = set()

    # 5. the scratch tables as a whole, and the curated Eurostat block
    o = files([p for h in state["observations"].values() for p in h["paths"]])
    s = files([p for h in state["series"].values() for p in h["paths"]])
    kept = sorted(state["observations"])
    silver_paths = [p for e, h in silver.items() if str(e).upper() in kept for p in h["paths"]]
    rows, duplicates = con.execute(
        f"select sum(n), count(*) filter (where n > 1) from (select count(*) as n from {o} group by series_key, date_key)"
    ).fetchone()
    checks = {
        "blocks": len(kept),
        "rows": rows,
        "duplicate_series_key_x_date_key": duplicates,
        "series_rows_and_points": con.execute(f"select count(*), sum(points) from {s}").fetchone(),
        "silver_rows_with_value": con.execute(f"select count(*) from {files(silver_paths)} where value is not null").fetchone()[0],
        "status": con.execute(f"select observation_status_key, count(*) from {o} group by 1 order by 2 desc").fetchall(),
        "polish_podregion_rows": con.execute(f"select count(*) from {o} where geography_key like 'BDL:%'").fetchone()[0],
        "mb": {n: round(sum(h["bytes"] for h in state[n].values()) / 1e6, 1) for n in state},
    }
    try:
        curated = cat.load_table(("gold", "build_eurostat")).scan(selected_fields=("geography_source_code",)).to_arrow()
        codes = curated.column("geography_source_code").to_pylist()
        checks["curated_build_eurostat"] = {"rows": len(codes), "rows_with_a_polish_nuts3_code": sum(
            1 for c in codes if c and PL_NUTS3.match(c))}
    except Exception as exc:
        checks["curated_build_eurostat"] = f"{type(exc).__name__}: {str(exc)[:120]}"
    annotate("notice", "schedule trial: checks", checks)


if __name__ == "__main__":
    main()
