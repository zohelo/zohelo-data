"""Dev trial (read-only) for #2775: Eurostat aggregate codes as EUROSTAT: aggregates in dim_geography.

Runs the compiled SQL of poc_dim_geography and poc_map_geography from origin/main (old) and from the PR head (new)
in local DuckDB over production Silver and Gold (the R2 Data Catalog attached read-only by use: nothing is written
to R2, no scratch namespace). The map's reference to gold.dim_geography is pointed at the local old or new
dimension. Reports: dimension keys that leave or arrive and changed levels; map rows whose area changes, and
unmatched (_X) codes per source old vs new; the dbt tests of both models on the new tables (unique, not null,
relationships from map_geography, geo_unit, geo_neighbour, dim_series); production fact, series and metrics
rows at the keys that change; per-block Gold blocks whose geography codes are remapped (the rebuild this
causes). At most 10 annotations. Deleted before merge.

python -m dev.geo_aggregates_trial
"""

from __future__ import annotations

import contextlib
from pathlib import Path
import time
import traceback

import duckdb

from ingestion.common import r2
from ingestion.common.report import annotate

SQL = Path(__file__).parent / "sql"
FACTS = ("fact_annual", "fact_quarterly", "fact_monthly", "fact_daily", "fact_semiannual")


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")
    con.execute("SET preserve_insertion_order = false; SET memory_limit = '10GB'")
    endpoint = r2.req("STORAGE_ENDPOINT_URL").replace("https://", "").strip("/")
    con.execute("CREATE SECRET r2_catalog (TYPE iceberg, TOKEN ?)", [r2.req("STORAGE_LAKEHOUSE_CATALOG_WRITE_TOKEN")])
    con.execute(
        "CREATE SECRET r2_files (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION 'auto', URL_STYLE 'path')",
        [r2.req("STORAGE_WRITE_ACCESS_KEY"), r2.req("STORAGE_WRITE_SECRET_KEY"), endpoint],
    )
    con.execute(
        "ATTACH ? AS r2 (TYPE iceberg, SECRET r2_catalog, ENDPOINT ?)",
        [r2.req("STORAGE_LAKEHOUSE_CATALOG_NAME"), r2.req("STORAGE_LAKEHOUSE_CATALOG_URL")],
    )
    return con


def sql(name: str, dim: str | None = None) -> str:
    text = (SQL / f"{name}.sql").read_text(encoding="utf-8")
    if dim:
        assert text.count('"r2"."gold"."dim_geography"') == 1, name
        text = text.replace('"r2"."gold"."dim_geography"', dim)
    return text


def rows(con, query: str) -> list:
    return con.execute(query).fetchall()


def one(con, query: str):
    return con.execute(query).fetchone()


def main() -> int:
    started = time.monotonic()
    con = connect()
    status = 0
    try:
        for tag in ("old", "new"):
            con.execute(f"create temp table dim_{tag} as {sql(f'poc_dim_geography_{tag}')}")
            con.execute(f"create temp table map_{tag} as {sql(f'poc_map_geography_{tag}', f'dim_{tag}')}")
        prod = {
            "dim_rows_prod_old_new": one(
                con, "select (select count(*) from r2.gold.dim_geography), (select count(*) from dim_old), "
                "(select count(*) from dim_new)"
            ),
            "map_rows_prod_old_new": one(
                con, "select (select count(*) from r2.gold.map_geography), (select count(*) from map_old), "
                "(select count(*) from map_new)"
            ),
            "old_equals_prod_dim": one(
                con,
                "select count(*) from (select geography_key, level_name from dim_old "
                "except select geography_key, level_name from r2.gold.dim_geography)",
            )[0],
            "old_equals_prod_map": one(
                con,
                "select count(*) from (select source_system, source_code, geography_key from map_old "
                "except select source_system, source_code, geography_key from r2.gold.map_geography)",
            )[0],
        }
        dim = {
            "left": rows(
                con,
                "select o.geography_key, o.level_name, o.name_en from dim_old o anti join dim_new n using "
                "(geography_key) order by 1",
            ),
            "arrived": rows(
                con,
                "select n.geography_key, n.level_name, n.name_en from dim_new n anti join dim_old o using "
                "(geography_key) order by 1",
            ),
            "level_changed": rows(
                con,
                "select geography_key, o.level_name, n.level_name from dim_old o join dim_new n using "
                "(geography_key) where o.level_name is distinct from n.level_name or o.parent_key is distinct "
                "from n.parent_key order by 1",
            ),
            "nuts_prefixes_new": rows(
                con,
                "select substr(nuts_code, 1, 2), count(*) from dim_new where level_name like 'nuts%' group by 1 "
                "order by 1",
            ),
        }
        left = [r[0].removeprefix("NUTS:") for r in dim.pop("left")]
        dim["arrived"] = len(dim["arrived"])
        dim["level_changed"] = len(dim["level_changed"])
        dim["moved_with_country_prefix"] = rows(
            con,
            "select o.nuts_code, o.name_en from dim_old o anti join dim_new n using (geography_key) "
            "where o.level_name like 'nuts%' and substr(o.nuts_code, 1, 2) in "
            "(select geography_key from dim_new where level_name = 'country' union select 'EL' union select 'UK') "
            "order by 1",
        )
        dim["nuts_left_named_like_aggregate"] = rows(
            con,
            "select nuts_code, name_en from dim_new where level_name like 'nuts%' and regexp_matches(lower(name_en), "
            "'countr|union|euro area|bank|fund|association|community|organi|group|total|world') order by 1",
        )
        dim["left_count"] = len(left)
        dim["nuts_letter_third"] = rows(
            con,
            "select substr(nuts_code, 1, 2), string_agg(distinct substr(nuts_code, 3, 1), '' order by "
            "substr(nuts_code, 3, 1)) from dim_new where level_name like 'nuts%' and "
            "regexp_matches(substr(nuts_code, 3, 1), '[A-Z]') group by 1 order by 1",
        )
        dim.pop("moved_with_country_prefix")
        annotate("notice", "2775 dimension", {**prod, **dim})
        countries = (
            "'AT','BE','BG','CY','CZ','DE','DK','EE','EL','ES','FI','FR','HR','HU','IE','IT','LT','LU','LV','MT',"
            "'NL','PL','PT','RO','SE','SI','SK','UK','CH','IS','LI','NO','AL','BA','ME','MK','RS','TR','UA','XK'"
        )
        annotate("notice", "2775 dimension: NUTS keys that leave (all)", {
            "left": ",".join(left),
            "names_left_nuts_country_prefix": [
                f"{r[0]}={r[1]}" for r in rows(
                    con,
                    "select o.nuts_code, o.name_en from dim_old o anti join dim_new n using (geography_key) "
                    f"where o.level_name like 'nuts%' and substr(o.nuts_code, 1, 2) in ({countries}) order by 1",
                )
            ],
            "names_uk_a_b_kept": [
                f"{r[0]}={r[1]}" for r in rows(
                    con, "select nuts_code, name_en from dim_new where level_name like 'nuts%' and "
                    "regexp_matches(nuts_code, '^UK[AB]') order by 1",
                )
            ],
            "names_pl_aggregates": [
                f"{r[0]}={r[1]}" for r in rows(
                    con, "select geography_key, name_en from dim_new where geography_key like 'EUROSTAT:PL%' order by 1"
                )
            ],
        })
        changed = rows(
            con,
            "select n.source_system, n.source_code, o.geography_key, n.geography_key from map_new n "
            "join map_old o using (source_system, source_code) where o.geography_key is distinct from n.geography_key "
            "order by 1, 2",
        )
        only = rows(
            con,
            "select 'old_only', source_system, source_code from (select source_system, source_code from map_old "
            "except select source_system, source_code from map_new) union all "
            "select 'new_only', source_system, source_code from (select source_system, source_code from map_new "
            "except select source_system, source_code from map_old)",
        )
        unmatched = rows(
            con,
            "select source_system, count(*) filter (where o.geography_key = '_X'), "
            "count(*) filter (where n.geography_key = '_X') from map_new n join map_old o "
            "using (source_system, source_code) group by 1 having count(*) filter (where o.geography_key = '_X') "
            "<> count(*) filter (where n.geography_key = '_X') order by 1",
        )
        compact = {
            "eurostat_codes_moved": len([c for c in changed if c[0] == "eurostat"]),
            "eurostat_not_nuts_to_eurostat": [
                c[1] for c in changed if c[0] == "eurostat" and (c[2], c[3]) != ("NUTS:" + c[1], "EUROSTAT:" + c[1])
            ],
            "other_sources": [c for c in changed if c[0] != "eurostat"],
        }
        annotate("notice", "2775 map", {**compact, "rows_only_in_one": only, "unmatched_old_new": unmatched})
        tests = {
            "dim_unique_dup": one(
                con, "select count(*) from (select geography_key from dim_new group by 1 having count(*) > 1)"
            )[0],
            "dim_null_key_level": one(
                con, "select count(*) from dim_new where geography_key is null or level_name is null"
            )[0],
            "map_null": one(
                con, "select count(*) from map_new where source_code is null or geography_key is null"
            )[0],
            "map_dup_source_code": one(
                con,
                "select count(*) from (select source_system, source_code from map_new group by 1, 2 "
                "having count(*) > 1)",
            )[0],
            "rel_map_to_dim": rows(
                con, "select distinct geography_key from map_new anti join dim_new using (geography_key)"
            ),
            "rel_geo_unit_to_dim": rows(
                con, "select distinct geography_key from r2.gold.geo_unit anti join dim_new using (geography_key) "
                "where geography_key is not null"
            ),
            "rel_geo_neighbour_to_dim": rows(
                con,
                "select distinct k from (select geography_key as k from r2.gold.geo_neighbour union all "
                "select neighbour_key from r2.gold.geo_neighbour) anti join dim_new on k = geography_key "
                "where k is not null",
            ),
            "rel_dim_series_prod_to_new_dim": rows(
                con,
                "select geography_key, count(*) from r2.gold.dim_series anti join dim_new using (geography_key) "
                "where geography_key is not null group by 1 order by 2 desc",
            ),
        }
        annotate("notice", "2775 tests on the new tables", tests)
        con.execute(
            "create temp table moved as select distinct o.geography_key as old_key, n.geography_key as new_key "
            "from map_new n join map_old o using (source_system, source_code) "
            "where o.geography_key is distinct from n.geography_key union "
            "select geography_key, null from dim_old anti join dim_new using (geography_key)"
        )
        facts = {}
        for fact in FACTS:
            with contextlib.suppress(Exception):
                facts[fact] = rows(
                    con,
                    f"select f.source_system, f.geography_key, count(*), count(distinct f.series_key), "
                    f"min(f.date_key)::varchar, max(f.date_key)::varchar from r2.gold.{fact} f "
                    f"join moved m on f.geography_key = m.old_key group by 1, 2 order by 1, 2",
                )
        facts["dim_series_measures"] = rows(
            con,
            "select s.geography_key, s.measure_key from r2.gold.dim_series s join moved m "
            "on s.geography_key = m.old_key order by 1, 2",
        )[:60]
        annotate("notice", "2775 production facts at the moved keys", facts)
        cat = r2.catalog()
        gold_tables = sorted(t[1] for t in cat.list_tables("gold"))
        metrics = {}
        for name in [t for t in gold_tables if t.startswith("metrics_")]:
            try:
                hit = rows(
                    con,
                    f"select source_system, geography_key, count(*) from r2.gold.{name} t join moved m "
                    f"on t.geography_key = m.old_key group by 1, 2",
                )
            except Exception as exc:  # noqa: BLE001 - reported
                hit = [f"error {str(exc)[:80]}"]
            if hit:
                metrics[name] = hit
        annotate("notice", "2775 production metrics at the moved keys", {
            "metrics_tables_read": len([t for t in gold_tables if t.startswith("metrics_")]),
            "tables_with_rows": metrics,
        })
        blocks = {}
        shared = ("map_geography", "dim_geography")
        for name in [t for t in gold_tables if t.endswith("_geography") and t not in shared]:
            source = name.removesuffix("_geography")
            try:
                blocks[source] = list(
                    one(
                        con,
                        f"select count(distinct g.block_id), count(*), coalesce(sum(g.points), 0), "
                        f"(select count(distinct block_id) from r2.gold.{name}) from r2.gold.{name} g "
                        f"join map_new n on n.source_system = '{source}' and n.source_code = g.geography_source_code "
                        f"join map_old o on o.source_system = n.source_system and o.source_code = n.source_code "
                        f"where o.geography_key is distinct from n.geography_key",
                    )
                )
            except Exception as exc:  # noqa: BLE001 - reported
                blocks[source] = f"error {str(exc)[:120]}"
        annotate("notice", "2775 per-block Gold blocks remapped [blocks, codes, points, blocks in table]", blocks)
        bad = [k for k in ("dim_unique_dup", "dim_null_key_level", "map_null", "map_dup_source_code") if tests[k]]
        if bad or tests["rel_map_to_dim"] or tests["rel_geo_unit_to_dim"] or tests["rel_geo_neighbour_to_dim"]:
            status = 1
    except Exception:  # noqa: BLE001 - reported
        status = 1
        annotate("error", "2775 trial failed", traceback.format_exc()[-3000:])
    annotate("notice" if status == 0 else "error", "2775 summary", {
        "status": status, "seconds": round(time.monotonic() - started), "writes_to_r2": 0,
    })
    return status


if __name__ == "__main__":
    raise SystemExit(main())
