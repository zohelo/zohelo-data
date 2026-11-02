"""Trial for #2513: the measures of four GUS BDL subgroups and their 2023 PL totals. Writes nothing."""

from __future__ import annotations

import duckdb

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

SUBGROUPS = ["P2425", "P1873", "P2497", "P3824"]


def main() -> None:
    cat = catalog()
    con = duckdb.connect()
    measure = cat.load_table("gold.dim_measure").scan(
        selected_fields=("measure_key", "measure_name", "source_system")
    ).to_arrow()
    series = cat.load_table("gold.dim_series").scan(
        selected_fields=("series_key", "measure_key", "geography_key", "is_total")
    ).to_arrow()
    fact = cat.load_table("gold.fact_annual").scan(selected_fields=("series_key", "date_key", "value")).to_arrow()
    con.register("dim_measure", measure)
    con.register("dim_series", series)
    con.register("fact_annual", fact)
    for sub in SUBGROUPS:
        rows = con.execute(
            """
            select m.measure_key, any_value(m.measure_name),
                   max(f.value) filter (where s.is_total and s.geography_key = 'PL'
                                        and f.date_key = date '2023-01-01')
            from dim_measure m
            left join dim_series s on s.measure_key = m.measure_key
            left join fact_annual f on f.series_key = s.series_key
            where m.measure_key like ?
            group by m.measure_key
            order by m.measure_key
            """,
            [f"gus_bdl:{sub}:%"],
        ).fetchall()
        annotate("notice", f"bdl pin {sub}", [[k, n, v] for k, n, v in rows])


if __name__ == "__main__":
    main()
