"""Trial dbt build of the NBP Silver models into zz_dbt_trial, row counts, then cleanup."""
from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import build

code = build.main("silver.nbp dim_date dim_period dim_currency fact_fx_rate", target="trial")
cat = catalog()
counts = {}
for name in ("nbp_fx_rates", "nbp_gold_fixings", "dim_date", "dim_period", "dim_currency", "fact_fx_rate"):
    try:
        table = cat.load_table(("zz_dbt_trial", name))
        arrow = table.scan().to_arrow()
        counts[name] = arrow.num_rows
        if name == "nbp_fx_rates":
            counts["fx_by_table"] = {t: arrow.filter(arrow["table_code"] == t).num_rows for t in ("A", "B", "C")}
            counts["fx_date_range"] = [str(min(arrow["effective_date"].to_pylist())), str(max(arrow["effective_date"].to_pylist()))]
        cat.drop_table(("zz_dbt_trial", name))
    except Exception as exc:
        counts[name] = f"{type(exc).__name__}: {exc}"[:200]
annotate("notice", "dbt trial nbp", {"dbt_exit": code, **counts})
raise SystemExit(code)
