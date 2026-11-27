"""Trial for #2513: the measures of four GUS BDL subgroups in prod Gold and their PL totals. Writes nothing."""

from __future__ import annotations

import duckdb
from pyiceberg.expressions import Or, StartsWith

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

SUBGROUPS = ["P2425", "P1873", "P2497", "P3824"]


def _prefixes(field: str):
    expr = StartsWith(field, f"gus_bdl:{SUBGROUPS[0]}:")
    for sub in SUBGROUPS[1:]:
        expr = Or(expr, StartsWith(field, f"gus_bdl:{sub}:"))
    return expr


def main() -> None:
    cat = catalog()
    con = duckdb.connect()
    con.register("dim_measure", cat.load_table("gold.dim_measure").scan(
        row_filter=_prefixes("measure_key"), selected_fields=("measure_key", "measure_name", "unit_key", "frequencies")).to_arrow())
    con.register("dim_series", cat.load_table("gold.dim_series").scan(
        row_filter=_prefixes("series_key"),
        selected_fields=("series_key", "measure_key", "geography_key", "is_total")).to_arrow())
    con.register("fact_annual", cat.load_table("gold.fact_annual").scan(
        row_filter=_prefixes("series_key"), selected_fields=("series_key", "date_key", "value")).to_arrow())
    for sub in SUBGROUPS:
        rows = con.execute(
            """
            select m.measure_key, any_value(m.unit_key), any_value(m.frequencies),
                   count(distinct s.series_key) filter (where s.is_total and s.geography_key = 'PL') as pl_total_series,
                   max(f.value) filter (where s.is_total and s.geography_key = 'PL' and f.date_key = date '2022-01-01'),
                   max(f.value) filter (where s.is_total and s.geography_key = 'PL' and f.date_key = date '2023-01-01'),
                   max(f.value) filter (where s.is_total and s.geography_key = 'PL' and f.date_key = date '2024-01-01')
            from dim_measure m
            left join dim_series s on s.measure_key = m.measure_key
            left join fact_annual f on f.series_key = s.series_key
            where m.measure_key like ?
            group by m.measure_key
            order by m.measure_key
            """,
            [f"gus_bdl:{sub}:%"],
        ).fetchall()
        annotate("notice", f"bdl pin {sub}", [list(r) for r in rows])


if __name__ == "__main__":
    main()
