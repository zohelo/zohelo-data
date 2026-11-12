"""Dev trial for the D33 unit follow-ups (#2931), deleted before merge. Read-only against production: the R2 Data
Catalog is attached READ_ONLY and nothing is written to R2 (all work tables live in the in-memory database).

The branch's models are compiled offline (as ops.dbt_compile does, no credentials). The four build stages
(Findex, ICP, IDS, UN WPP) run on production Silver; the facts, dim_series, dim_measure, concept_measures and the
metrics of the affected KPIs are built in memory from them with the branch seeds, the other sources' blocks being
production's rows of the same KPIs. Reports (compact notices, at most 10): Silver markers, rows and units per
block, unmatched areas, grain by series_key x date_key, the three unit tests, the value-range test on the new
bounds, metrics per KPI x source with spot values for Poland, and every difference from production's metrics
tables. Errors are collected and reported as warnings."""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path
import sys
import tempfile
import traceback

import duckdb

from ops import dbt_compile as dc

ROOT = Path(__file__).resolve().parents[1]
W = ROOT / "warehouse"
SEEDS = ("unit", "unit_map", "kpi", "kpi_measure")
BLOCKS = {
    "build_findex": "world_bank_findex",
    "build_icp": "world_bank_icp",
    "build_ids": "world_bank_ids",
    "build_un_wpp": "un_wpp",
}
FACTS = ("fact_annual", "fact_semiannual", "fact_quarterly", "fact_monthly", "fact_daily")
KPIS = (
    "account_ownership", "price_level_index", "gdp_per_capita_pps", "old_age_dependency", "external_debt_stocks",
    "external_debt_gni", "external_debt_service", "external_debt_service_gni", "external_debt_service_exports",
)  # fmt: skip
RANGE_MODELS = ("account_ownership", "external_debt_stocks", "external_debt_service")
TESTS = ("metrics_unit_base", "metrics_unit_unmapped", "unit_map_pattern_ties", "metrics_value_range", "fact_grain",
         "fact_grain_semiannual")  # fmt: skip
REPORT: dict[str, object] = {}
ERRORS: list[str] = []
CHUNK = 3700


def emit(level: str, title: str, data) -> int:
    text = json.dumps(data, ensure_ascii=False, default=str, separators=(",", ":"))
    parts = [text[i : i + CHUNK] for i in range(0, max(len(text), 1), CHUNK)]
    for n, part in enumerate(parts, 1):
        msg = part.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        print(f"::{level} title={title} [{n}/{len(parts)}]::{msg}", flush=True)
    return len(parts)


def flush() -> None:
    """REPORT as notices (at most 9 annotations, sections in order), ERRORS as one warning."""
    budget = 9
    for key, value in REPORT.items():
        size = len(json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":")))
        need = size // CHUNK + 1
        if need > budget:
            ERRORS.append(f"report section {key} left out ({size} chars, {budget} annotations left)")
            continue
        budget -= emit("notice", key, value)
    if ERRORS:
        emit("warning", "errors", [e[-900:] for e in ERRORS[:4]])
    out = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
    out.mkdir(parents=True, exist_ok=True)
    (out / "d33_unit_followups.json").write_text(
        json.dumps({"report": REPORT, "errors": ERRORS}, ensure_ascii=False, default=str, indent=1), encoding="utf-8"
    )


def guard(name: str, fn, *args):
    try:
        return fn(*args)
    except Exception:  # noqa: BLE001 - report, keep the other checks
        ERRORS.append(f"{name}: {traceback.format_exc()[-1200:]}")
        return None


def compile_branch(tmp: Path) -> Path:
    """The branch's models and tests compiled offline, exactly as ops.dbt_compile compiles them for `check`."""
    from dbt.cli.main import dbtRunner
    import yaml

    topics = yaml.safe_load((W / "warehouse.yaml").read_text(encoding="utf-8"))["topics"]
    catalog = tmp / "r2.duckdb"
    (tmp / "profiles.yml").write_text(dc.PROFILE.format(catalog=json.dumps(str(catalog))), encoding="utf-8")
    os.environ["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    flags = [
        "--project-dir", str(W), "--profiles-dir", str(tmp), "--target", "prod", "--target-path", str(tmp / "target"),
        "--log-path", str(tmp / "logs"), "--log-level", "none", "--log-level-file", "none", "--no-partial-parse",
        "--vars", json.dumps({"topics": topics}),
    ]  # fmt: skip
    parsed = dbtRunner().invoke(["parse", *flags])
    if not parsed.success:
        raise RuntimeError(f"dbt parse failed: {parsed.exception}")
    manifest = parsed.result
    with duckdb.connect(str(catalog)) as con:
        optional = {n for node in manifest.nodes.values() for n in node.config.meta.get(dc.OPTIONAL_PARENTS) or []}
        for node in manifest.nodes.values():
            if node.resource_type == "model" and (dc.BLOCK_TAG in node.tags or node.name in optional):
                con.execute(f'create schema if not exists "{node.schema}"')
                con.execute(f'create table "{node.schema}"."{node.alias}" (stub integer)')
    select = [
        *(f"poc_{b}" for b in BLOCKS), *(f"poc_{f}" for f in FACTS), "poc_dim_series", "poc_dim_measure",
        "poc_concept_measures", *(f"poc_metrics_{k}" for k in KPIS), *TESTS,
    ]  # fmt: skip
    done = dbtRunner(manifest=manifest).invoke(["compile", *flags, "--select", *select])
    if not done.success:
        raise RuntimeError(f"dbt compile failed: {done.exception}")
    return tmp / "target" / "compiled" / "zohelo"


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET memory_limit='10GB'; SET preserve_insertion_order=false;")
    local = os.environ.get("DEV_LOCAL_R2")  # a local DuckDB file in place of the catalog, for a dry run of this script
    if local:
        con.execute(f"ATTACH '{local}' AS r2 (READ_ONLY)")
        return con
    from ingestion.common.r2 import req

    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")
    con.execute(f"CREATE SECRET r2_catalog (TYPE iceberg, TOKEN '{req('STORAGE_LAKEHOUSE_CATALOG_WRITE_TOKEN')}')")
    endpoint = req("STORAGE_ENDPOINT_URL").replace("https://", "").strip("/")
    con.execute(
        f"CREATE SECRET r2_files (TYPE s3, KEY_ID '{req('STORAGE_WRITE_ACCESS_KEY')}', "
        f"SECRET '{req('STORAGE_WRITE_SECRET_KEY')}', ENDPOINT '{endpoint}', REGION 'auto', URL_STYLE 'path')"
    )
    con.execute(
        f"ATTACH '{req('STORAGE_LAKEHOUSE_CATALOG_NAME')}' AS r2 "
        f"(TYPE iceberg, SECRET r2_catalog, ENDPOINT '{req('STORAGE_LAKEHOUSE_CATALOG_URL')}', READ_ONLY)"
    )
    return con


def rows(con, sql: str) -> list[list]:
    return [[v if isinstance(v, (int, float)) or v is None else str(v) for v in r] for r in con.execute(sql).fetchall()]


def short(x, n: int = 60) -> str:
    x = "" if x is None else str(x)
    return x if len(x) <= n else x[: n - 1] + "…"


def prod_tables(con, schema: str) -> set[str]:
    return {r[0] for r in con.execute(
        f"SELECT table_name FROM information_schema.tables WHERE table_catalog = 'r2' AND table_schema = '{schema}'").fetchall()}


class Trial:
    def __init__(self, con, compiled: Path):
        self.con = con
        self.compiled = compiled
        self.local: set[str] = set()
        self.gold = prod_tables(con, "gold")
        self.silver = prod_tables(con, "silver")

    def sql(self, relative: str) -> str:
        """Compiled SQL with every relation this trial built in place of production's."""
        text = (self.compiled / relative).read_text(encoding="utf-8")
        for name in sorted(self.local, key=len, reverse=True):
            text = text.replace(f'"r2"."gold"."{name}"', f"t.{name}")
        return text

    def table(self, name: str, sql: str) -> int:
        self.con.execute(f"CREATE OR REPLACE TABLE t.{name} AS {sql}")
        self.local.add(name)
        return self.con.execute(f"SELECT count(*) FROM t.{name}").fetchone()[0]


def seeds(tr: Trial) -> None:
    con = tr.con
    con.execute("CREATE SCHEMA t")
    for s in SEEDS:
        tr.table(s, f"SELECT * FROM read_csv('{W}/seeds/{s}.csv', header=true, all_varchar=true)")
    con.execute("CREATE OR REPLACE TABLE t.unit AS SELECT * REPLACE (CAST(factor AS DECIMAL(38,9)) AS factor) FROM t.unit")
    con.execute("CREATE OR REPLACE TABLE t.kpi_measure AS SELECT * REPLACE (CAST(priority AS INTEGER) AS priority) FROM t.kpi_measure")


def silver_report(tr: Trial) -> None:
    """What the sources publish: value markers that are not numbers, the published scale and the ICP unit names."""
    con, out = tr.con, {}
    out["silver tables present"] = sorted(
        t for t in ("findex_observations", "icp_observations", "ids_observations", "ids_series",
                    "un_wpp_demographic_indicators", "un_wpp_population_age_groups") if t in tr.silver
    )  # fmt: skip
    out["findex account_t_d group all [rows, with value, text without a number: markers, min, max]"] = rows(con, """
        SELECT count(*), count(value), list(DISTINCT value_text) FILTER (WHERE value IS NULL), min(value), max(value)
        FROM r2.silver.findex_observations WHERE indicator = 'account_t_d' AND group_name = 'all' AND group_value = 'all'""")
    out["icp series 1000000 [classification code, name, rows, with value, markers, min, max]"] = rows(con, """
        SELECT classification_code, any_value(classification_name), count(*), count(value),
            list(DISTINCT value_text) FILTER (WHERE value IS NULL), min(value), max(value)
        FROM r2.silver.icp_observations WHERE series_code = '1000000' GROUP BY 1 ORDER BY 1""")
    out["ids built series [series, rows counterpart WLD, with value, markers, zero values, min year, max year]"] = rows(con, """
        SELECT series_code, count(*), count(value), list(DISTINCT value_text) FILTER (WHERE value IS NULL),
            count(*) FILTER (WHERE value = 0), min(year), max(year)
        FROM r2.silver.ids_observations
        WHERE counterpart_code = 'WLD' AND series_code IN ('DT.DOD.DECT.CD','DT.DOD.DECT.GN.ZS','DT.TDS.DECT.CD','DT.TDS.DECT.GN.ZS','DT.TDS.DECT.EX.ZS')
        GROUP BY 1 ORDER BY 1""")
    out["un_wpp age groups [rows, ratio null, min, max, projection rows]"] = rows(con, """
        SELECT count(*), count(*) FILTER (WHERE old_age_dependency_ratio IS NULL), min(old_age_dependency_ratio),
            max(old_age_dependency_ratio), count(*) FILTER (WHERE is_projection)
        FROM r2.silver.un_wpp_population_age_groups""")
    REPORT["1 silver and map"] = out


def geography(tr: Trial) -> None:
    """Production's map rows of the four sources; a source the production map does not hold yet gets the rule of
    its map section from dim_geography (World Bank family: wb_code or iso3; UN WPP: iso2 and the two groups), and
    the report says so."""
    con = tr.con
    tr.table("map_geography", "SELECT * FROM r2.gold.map_geography WHERE source_system IN ('world_bank_findex','world_bank_icp','world_bank_ids','un_wpp')")
    have = dict(con.execute("SELECT source_system, count(*) FROM t.map_geography GROUP BY 1").fetchall())
    fallback = []
    cols = [r[0] for r in con.execute("DESCRIBE t.map_geography").fetchall()]
    rest = ", ".join(f"NULL AS {c}" for c in cols if c not in ("source_system", "source_code", "geography_key"))
    rest = f", {rest}" if rest else ""
    for source in BLOCKS.values():
        if have.get(source):
            continue
        fallback.append(source)
        if source == "un_wpp":
            body = """SELECT 'un_wpp' AS source_system, iso2 AS source_code, geography_key FROM r2.gold.dim_geography
                WHERE level_name = 'country' AND iso2 IS NOT NULL
                UNION ALL SELECT 'un_wpp', '1551', 'EUROSTAT:EU27_2020' UNION ALL SELECT 'un_wpp', '900', 'WB:WLD'"""
        else:
            body = f"""SELECT DISTINCT ON (source_code) '{source}' AS source_system, source_code, geography_key FROM (
                    SELECT wb_code AS source_code, geography_key, 1 AS o FROM r2.gold.dim_geography WHERE wb_code IS NOT NULL
                    UNION ALL
                    SELECT iso3, geography_key, 2 FROM r2.gold.dim_geography WHERE iso3 IS NOT NULL AND level_name = 'country'
                ) ORDER BY source_code, o"""
        con.execute(f"INSERT INTO t.map_geography BY NAME SELECT source_system, source_code, geography_key{rest} FROM ({body})")
    REPORT.setdefault("1 silver and map", {})["geography map"] = {
        "production map rows per source": have, "trial-only map built from dim_geography for": fallback}


def blocks(tr: Trial) -> None:
    con, out = tr.con, {}
    for alias, source in BLOCKS.items():
        n = tr.table(alias, tr.sql(f"models/gold/poc_{alias}.sql"))
        entry: dict[str, object] = {"rows": n}
        entry["null values, null keys"] = rows(con, f"""SELECT count(*) FILTER (WHERE value IS NULL),
            count(*) FILTER (WHERE measure_key IS NULL OR geography_key IS NULL OR date_key IS NULL) FROM t.{alias}""")[0]
        entry["measures [measure, unit code, unit name, frequency, rows, areas, first, last, min, max]"] = rows(con, f"""
            SELECT measure_key, unit_code, left(any_value(unit_name), 60), frequency, count(*), count(DISTINCT geography_key),
                year(min(date_key)), year(max(date_key)), min(value), max(value)
            FROM t.{alias} GROUP BY ALL ORDER BY 1""")
        unmatched = rows(con, f"""SELECT geography_source_code, count(*) FROM t.{alias} WHERE geography_key = '_X'
            GROUP BY 1 ORDER BY 1""")
        entry["unmatched rows, codes"] = [sum(r[1] for r in unmatched), " ".join(r[0] or "" for r in unmatched)[:300]]
        entry["duplicates series_key x date_key"] = con.execute(f"""
            SELECT count(*) FROM (SELECT 1 FROM t.{alias}
                GROUP BY measure_key || '|' || geography_key || CASE WHEN geography_key = '_X' THEN ':' || coalesce(geography_source_code, '') ELSE '' END || '|' || breakdown_json, date_key
                HAVING count(*) > 1)""").fetchone()[0]
        entry["status keys"] = rows(con, f"SELECT observation_status_key, count(*) FROM t.{alias} GROUP BY 1 ORDER BY 1")
        if alias in tr.gold:
            entry["production table [rows, unit codes]"] = rows(con, f"SELECT count(*), list(DISTINCT unit_code) FROM r2.gold.{alias}")[0]
            entry["against production [matched rows, value equal, new x 100 = old (tolerance 1e-9), only new, only old]"] = rows(con, f"""
                SELECT count(*) FILTER (WHERE n.value IS NOT NULL AND o.value IS NOT NULL),
                    count(*) FILTER (WHERE n.value = o.value),
                    count(*) FILTER (WHERE abs(n.value * 100 - o.value) <= 1e-9 * greatest(abs(o.value), 1)),
                    count(*) FILTER (WHERE o.value IS NULL), count(*) FILTER (WHERE n.value IS NULL)
                FROM t.{alias} n FULL JOIN r2.gold.{alias} o
                    ON n.measure_key = o.measure_key AND n.geography_source_code = o.geography_source_code AND n.date_key = o.date_key""")[0]
        else:
            entry["production table"] = "does not exist yet"
        out[alias] = entry
    REPORT["2 build stages on production Silver"] = out


def other_blocks(tr: Trial, text: str) -> None:
    """Every other block the facts read: production's rows of the affected KPIs' measures, else no rows."""
    import re

    patterns: dict[str, list[str]] = {}
    for r in csv.DictReader((W / "seeds/kpi_measure.csv").open(encoding="utf-8")):
        if r["kpi_key"] in KPIS and r["source_system"] not in BLOCKS.values():
            patterns.setdefault(r["source_system"], []).append(r["measure_key_pattern"])
    source_of = dict(re.findall(r"poc_(build_\w+):(\w+)", (W / "macros/poc_blocks.sql").read_text(encoding="utf-8")))
    used, missing = {}, []
    for name in sorted(set(re.findall(r'"r2"\."gold"\."(build_[a-z0-9_]+)"', text)) - set(BLOCKS)):
        wanted = patterns.get(source_of.get(name, ""), [])
        if name not in tr.gold:
            missing.append(name)
            wanted = []
        if wanted:
            cond = " OR ".join("measure_key LIKE '" + p.replace("'", "''") + "'" for p in wanted)
            used[name] = tr.table(name, f"SELECT * FROM r2.gold.{name} WHERE {cond}")
        else:
            tr.table(name, "SELECT * FROM t.build_findex WHERE false")
    REPORT["3 facts, grain, range test"] = {"other blocks read from production (rows of these KPIs)": used,
                                            "block tables production does not have": missing}


def facts_and_dims(tr: Trial) -> None:
    con = tr.con
    other_blocks(tr, (tr.compiled / "models/gold/poc_fact_annual.sql").read_text(encoding="utf-8"))
    out = REPORT["3 facts, grain, range test"]
    out["fact rows"] = {f: tr.table(f, tr.sql(f"models/gold/poc_{f}.sql")) for f in FACTS}
    out["dim_series rows"] = tr.table("dim_series", tr.sql("models/gold/poc_dim_series.sql"))
    out["dim_measure activated rows"] = tr.table("dim_measure", tr.sql("models/gold/poc_dim_measure.sql"))
    con.execute("DELETE FROM t.dim_measure WHERE NOT activated")
    out["dim_measure activated rows"] = con.execute("SELECT count(*) FROM t.dim_measure").fetchone()[0]
    out["concept_measures rows"] = tr.table("concept_measures", tr.sql("models/gold/poc_concept_measures.sql"))
    out["test fact_grain rows (series_key x date_key, as warehouse/tests/fact_grain.sql)"] = len(rows(con, tr.sql("tests/fact_grain.sql")))
    out["test fact_grain_semiannual rows"] = len(rows(con, tr.sql("tests/fact_grain_semiannual.sql")))
    out["fact_annual of the four sources [source, rows, series, first, last]"] = rows(con, """
        SELECT source_system, count(*), count(DISTINCT series_key), min(date_key), max(date_key) FROM t.fact_annual
        WHERE source_system IN ('world_bank_findex','world_bank_icp','world_bank_ids','un_wpp') GROUP BY 1 ORDER BY 1""")
    out["dim_measure unit keys of the four sources"] = rows(con, """
        SELECT source_system, unit_key, count(*) FROM t.dim_measure
        WHERE source_system IN ('world_bank_findex','world_bank_icp','world_bank_ids','un_wpp') GROUP BY ALL ORDER BY ALL""")


def unit_tests(tr: Trial) -> None:
    import re

    con = tr.con
    text = tr.sql("tests/metrics_unit_base.sql")
    inner = re.search(r"from \((.*)\)\s*where", text, re.S).group(1)
    tr.con.execute(f"CREATE OR REPLACE TABLE t.measure_units AS {inner}")
    REPORT["4 units"] = {
        "measure units [concept, measure, unit code, source unit, target unit, ratio]": [
            [r[0], short(r[1], 70), short(r[2], 40), r[3], r[4], r[5]] for r in rows(con, """
                SELECT concept_key, measure_key, unit_code, source_unit_key, target_unit_key, ratio FROM t.measure_units
                WHERE source_system IN ('world_bank_findex','world_bank_icp','world_bank_ids','un_wpp') OR ratio IS DISTINCT FROM 1
                ORDER BY ALL""")],
        "measures of these KPIs": con.execute("SELECT count(*) FROM t.measure_units").fetchone()[0],
        "test metrics_unit_base rows": [[r[0], short(r[1], 70), r[3], r[4], r[6]] for r in rows(con, text)],
        "test metrics_unit_unmapped rows": [[r[0], short(r[1], 70), r[2], short(r[3], 40), r[4]] for r in rows(con, tr.sql("tests/metrics_unit_unmapped.sql"))],
        "test unit_map_pattern_ties rows": rows(con, tr.sql("tests/unit_map_pattern_ties.sql")),
    }


def metrics(tr: Trial) -> None:
    import re

    con = tr.con
    per, diff, spots, overlap = [], [], [], {}
    for kpi in KPIS:
        try:
            tr.table(f"metrics_{kpi}", tr.sql(f"models/gold/poc_metrics_{kpi}.sql"))
            per += [[kpi, *r] for r in rows(con, f"""
                SELECT source_system, measure_key, unit, unit_published, count(*), count(DISTINCT geography_key),
                    year(min(date_key)), year(max(date_key)), min(value), max(value), arg_max(geography_key, value)
                FROM t.metrics_{kpi} GROUP BY ALL ORDER BY 1, 2""")]
            overlap[kpi] = con.execute(f"""SELECT coalesce(max(c), 0) FROM (SELECT count(*) AS c FROM t.metrics_{kpi}
                GROUP BY source_system, geography_key, date_key)""").fetchone()[0]
            spots += [[kpi, *r] for r in rows(con, f"""
                SELECT source_system, geography_key, date_key, value_published, unit_published, value, unit
                FROM t.metrics_{kpi}
                WHERE geography_key IN ('PL', 'UA', 'TR', 'WB:LMY', 'EUROSTAT:EU27_2020')
                    AND (source_system IN ('world_bank_findex','world_bank_icp','world_bank_ids','un_wpp') OR geography_key = 'PL')
                QUALIFY row_number() OVER (PARTITION BY source_system, measure_key, geography_key ORDER BY date_key DESC) = 1
                ORDER BY 1, 2""")]
            if f"metrics_{kpi}" not in tr.gold:
                diff.append([kpi, "no table in production"])
                continue
            diff += [[kpi, *r] for r in rows(con, f"""
                SELECT coalesce(n.source_system, o.source_system), count(*) FILTER (WHERE o.series_key IS NOT NULL),
                    count(*) FILTER (WHERE n.series_key IS NOT NULL), count(*) FILTER (WHERE o.series_key IS NULL),
                    count(*) FILTER (WHERE n.series_key IS NULL),
                    count(*) FILTER (WHERE n.value IS DISTINCT FROM o.value AND n.series_key IS NOT NULL AND o.series_key IS NOT NULL),
                    max(abs(n.value - o.value)),
                    count(*) FILTER (WHERE n.unit IS DISTINCT FROM o.unit AND n.series_key IS NOT NULL AND o.series_key IS NOT NULL),
                    any_value(o.unit), any_value(n.unit), any_value(o.unit_published), any_value(n.unit_published)
                FROM t.metrics_{kpi} n FULL JOIN r2.gold.metrics_{kpi} o USING (series_key, date_key)
                GROUP BY 1 ORDER BY 1""")]
        except Exception:  # noqa: BLE001
            ERRORS.append(f"metrics {kpi}: {traceback.format_exc()[-500:]}")
    REPORT["5 metrics [kpi, source, measure, unit, unit published, rows, areas, first, last, min, max, area of max]"] = [
        [r[0], r[1], short(r[2], 60), r[3], short(r[4], 34), *r[5:]] for r in per]
    REPORT["6 spot values [kpi, source, area, period, value published, unit published, value, unit]"] = {
        "newest per series": [[*r[:5], short(r[5], 34), *r[6:]] for r in spots],
        "max metrics rows per source x area x period, per kpi": overlap,
    }
    REPORT["7 against production metrics [kpi, source, rows prod, rows trial, only trial, only prod, value differs, max abs diff, unit differs, unit prod, unit trial, unit published prod, unit published trial]"] = [
        [short(v, 34) if isinstance(v, str) else v for v in r] for r in diff]
    text = tr.sql("tests/metrics_value_range.sql")
    for name in sorted(set(re.findall(r'"r2"\."gold"\."(metrics_[a-z0-9_]+)"', text))):
        text = text.replace(f'"r2"."gold"."{name}"', "(SELECT * FROM t.metrics_account_ownership WHERE false)")
    failing = rows(con, text)
    REPORT["3 facts, grain, range test"]["test metrics_value_range, only the three models added to its union"] = {
        "models built": [f"metrics_{m}" for m in RANGE_MODELS if f"metrics_{m}" in tr.local], "failing rows": len(failing),
        "sample": [[r[0], r[1], short(r[2], 50), r[3], r[4], r[5], r[6], r[7]] for r in failing[:12]],
    }


def main() -> int:
    con = None
    try:
        with tempfile.TemporaryDirectory(prefix="d33_followups_") as tmp:
            compiled = compile_branch(Path(tmp))
            con = connect()
            tr = Trial(con, compiled)
            seeds(tr)
            guard("silver", silver_report, tr)
            geography(tr)
            blocks(tr)
            facts_and_dims(tr)
            guard("units", unit_tests, tr)
            guard("metrics", metrics, tr)
    except Exception:  # noqa: BLE001
        ERRORS.append(f"trial: {traceback.format_exc()[-1500:]}")
    flush()
    return 1 if ERRORS else 0


if __name__ == "__main__":
    sys.exit(main())
