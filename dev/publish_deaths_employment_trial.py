"""Trial for #2515: metrics rows of deaths and employment_rate per source. Writes nothing."""

from __future__ import annotations

import duckdb

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

KPIS = ["deaths", "employment_rate"]
FACT_FIELDS = ("series_key", "measure_key", "source_system", "geography_key", "date_key")


def main() -> None:
    cat = catalog()
    con = duckdb.connect()
    for name in ("fact_annual", "fact_monthly"):
        con.register(name, cat.load_table(f"gold.{name}").scan(selected_fields=FACT_FIELDS).to_arrow())
    con.register("concept_measures", cat.load_table("gold.concept_measures").scan().to_arrow())
    con.register(
        "dim_series", cat.load_table("gold.dim_series").scan(selected_fields=("series_key", "is_total")).to_arrow()
    )
    rows = con.execute(
        """
        with facts as (select * from fact_annual union all select * from fact_monthly)
        select cm.concept_key, f.source_system, count(*)
        from facts f
        join concept_measures cm on cm.measure_key = f.measure_key
        join dim_series s on s.series_key = f.series_key
        where s.is_total and f.geography_key <> '_X' and cm.concept_key in ?
        group by 1, 2
        order by 1, 2
        """,
        [KPIS],
    ).fetchall()
    result: dict[str, dict[str, int]] = {k: {} for k in KPIS}
    for kpi, source, n in rows:
        result[kpi][source] = n
    annotate("notice", "metrics rows", result)


if __name__ == "__main__":
    main()
