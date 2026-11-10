"""Read-only probe for the zohelo-bi first slice, all sources (O31). Deleted before merge; dev/ never on main.

Subcommands (output files in DEV_OUTPUT_DIR, kept as the run artifact; short summaries as annotations):
  coverage                     gold.coverage of eurostat, gus_bdl, gus_dbw, world_bank_wdi, nbp + silver.bdl_variables
  eurostat DATASET ...         per dataset: freq, unit, breakdown dims, codes for PL and EU27_2020, last period
  bdl SUBGROUP ...             per subgroup: Poland rows by dimension_values, unit, years; voivodeship count
  dbw PREFIX ...               per DBW block prefix: dictionary + Poland/summary rows
  wdi CODE ...                 per WDI indicator: PL rows and last year
"""

from __future__ import annotations

import os
from pathlib import Path
import sys

import duckdb
import pyarrow as pa

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
OUT.mkdir(parents=True, exist_ok=True)
cat = catalog()


def write(name: str, table: pa.Table) -> None:
    con = duckdb.connect()
    con.register("t", table)
    con.execute(f"copy t to '{OUT / name}' (header, delimiter ',')")
    print(f"=====BEGIN {name}", flush=True)
    print((OUT / name).read_text(encoding="utf-8"), flush=True)
    print(f"=====END {name}", flush=True)


def scan(ns: str, name: str, row_filter, columns=None) -> pa.Table:
    t = cat.load_table((ns, name))
    kwargs = {"row_filter": row_filter}
    if columns:
        kwargs["selected_fields"] = tuple(columns)
    return t.scan(**kwargs).to_arrow()


def coverage() -> None:
    from pyiceberg.expressions import In

    cov = scan("gold", "coverage", In("source", ["eurostat", "gus_bdl", "gus_dbw", "world_bank_wdi", "nbp"]))
    write("coverage.csv", cov)
    con = duckdb.connect()
    con.register("c", cov)
    summary = con.sql("select source, status, count(*) n, sum(silver_rows) r from c group by all order by all").fetchall()
    annotate("notice", "probe coverage", summary)
    var = cat.load_table(("silver", "bdl_variables")).scan().to_arrow()
    write("bdl_variables.csv", var)
    annotate("notice", "probe bdl_variables", {"rows": var.num_rows})


def eurostat(ids: list[str]) -> None:
    from pyiceberg.expressions import And, In

    cols = ["dataset_id", "freq_code", "unit_code", "geo_code", "time_period", "value", "breakdown_dimensions",
            "breakdown_codes"]
    for d in ids:
        variants = [d, d.lower()]
        t = scan("silver", "eurostat_observations",
                 And(In("dataset_id", variants), In("geo_code", ["PL", "EU27_2020", "DE", "PL22", "PL9"])), cols)
        con = duckdb.connect()
        con.register("o", t)
        rows = con.sql("""
            select freq_code, unit_code, breakdown_dimensions, breakdown_codes,
                   string_agg(distinct geo_code, ' ' order by geo_code) geos,
                   count(*) n, min(time_period) first, max(time_period) last
            from o where value is not null group by all order by all
        """).fetch_arrow_table()
        write(f"eurostat_{d}.csv", rows)
        short = con.sql("""
            select freq_code, unit_code, breakdown_dimensions, count(distinct breakdown_codes) series,
                   max(time_period) last from o where value is not null and geo_code = 'PL' group by all order by all
        """).fetchall()
        annotate("notice", f"probe eurostat {d}", {"rows": t.num_rows, "pl": short[:40]})


def bdl(subgroups: list[str]) -> None:
    from pyiceberg.expressions import In

    t = scan("silver", "bdl_observations", In("subgroup_id", subgroups),
             ["subgroup_id", "variable_id", "unit_code", "unit_name", "year", "value", "measure_unit",
              "dimension_values", "channel"])
    var = cat.load_table(("silver", "bdl_variables")).scan(row_filter=In("subgroup_id", subgroups)).to_arrow()
    con = duckdb.connect()
    con.register("o", t)
    con.register("v", var)
    rows = con.sql("""
        select o.subgroup_id, o.measure_unit, o.dimension_values, any_value(v.variable_id) variable_id,
               count(*) filter (where o.unit_code in ('000000000000', '0000000')) pl_rows,
               count(distinct o.unit_code) filter (where o.unit_code like '__00000' or o.unit_code like '0_1__0000000'
                                                   or o.unit_code like '0_2__0000000') voiv_units,
               count(distinct o.unit_code) units,
               min(o.year) first, max(o.year) last,
               any_value(o.value) filter (where o.unit_code in ('000000000000', '0000000') and o.year = 2024) pl_2024
        from o left join v on v.subgroup_id = o.subgroup_id and v.dimension_key = o.dimension_values
        where o.value is not null
        group by all order by all
    """).fetch_arrow_table()
    write("bdl_" + "_".join(subgroups)[:80] + ".csv", rows)
    con.register("r", rows)
    for s in subgroups:
        short = con.sql(f"select measure_unit, dimension_values, pl_rows, voiv_units, first, last, pl_2024 "
                        f"from r where subgroup_id = '{s}' limit 30").fetchall()
        annotate("notice", f"probe bdl {s}", short)


def dbw(prefixes: list[str]) -> None:
    from pyiceberg.expressions import Or, StartsWith

    filters = [StartsWith("entity_id", f"{p}_") for p in prefixes]
    f = Or(*filters) if len(filters) > 1 else filters[0]
    dic = scan("bronze", "dbw_dictionary_items", f, ["entity_id", "nazwa_kolumny", "id_elementu", "opis"])
    obs = scan("silver", "dbw_observations", f)
    con = duckdb.connect()
    con.register("d", dic)
    con.register("o", obs)
    dd = con.sql("select distinct regexp_replace(entity_id, '_[0-9]{4}_pl$|_pl$', '') block, lower(nazwa_kolumny) col, "
                 "id_elementu, opis from d order by all").fetch_arrow_table()
    write("dbw_dict_" + "_".join(prefixes)[:80] + ".csv", dd)
    summary = con.sql("""
        select regexp_replace(entity_id, '_[0-9]{4}_pl$|_pl$', '') block, indicator_id, presentation_id, period_id,
               dimension_1_id, dimension_2_id, dimension_3_id, count(*) n, min(year) first, max(year) last,
               count(distinct position_1_id) p1, count(distinct position_2_id) p2, count(distinct position_3_id) p3
        from o where value is not null group by all order by all
    """).fetch_arrow_table()
    write("dbw_obs_" + "_".join(prefixes)[:80] + ".csv", summary)
    sample = con.sql("""
        select regexp_replace(entity_id, '_[0-9]{4}_pl$|_pl$', '') block, year, period_id, presentation_id,
               position_1_id, position_2_id, position_3_id, value
        from o where value is not null
        qualify row_number() over (partition by block order by year desc, period_id desc) <= 40
    """).fetch_arrow_table()
    write("dbw_sample_" + "_".join(prefixes)[:80] + ".csv", sample)
    con.register("s", summary)
    annotate("notice", "probe dbw", con.sql("select block, count(*), sum(n), max(last) from s group by 1").fetchall())


def wdi(codes: list[str]) -> None:
    from pyiceberg.expressions import And, EqualTo, In

    t = scan("silver", "wdi_observations", And(In("indicator_code", codes), EqualTo("economy_code", "POL")))
    con = duckdb.connect()
    con.register("o", t)
    annotate("notice", "probe wdi", con.sql(
        "select indicator_code, count(*), min(year), max(year), arg_max(value, year) from o "
        "where value is not null group by 1 order by 1").fetchall())


def counts(names: list[str]) -> None:
    out = {}
    for n in names:
        ns, name = n.split(".")
        try:
            snap = cat.load_table((ns, name)).current_snapshot()
            out[n] = snap.summary.additional_properties.get("total-records") if snap else None
        except Exception as e:  # report, do not fail
            out[n] = f"error: {type(e).__name__}"
    annotate("notice", "probe counts", out)
    print(out, flush=True)


def main(argv: list[str]) -> int:
    cmd, args = argv[0], argv[1:]
    {"coverage": lambda: coverage(), "eurostat": lambda: eurostat(args), "bdl": lambda: bdl(args),
     "dbw": lambda: dbw(args), "wdi": lambda: wdi(args), "counts": lambda: counts(args)}[cmd]()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
