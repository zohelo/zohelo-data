"""Trial of the incremental Eurostat Silver in zz_dbt_trial: two builds of 5 datasets each,
row and dataset counts after each, then the trial tables are dropped."""
from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import build

out = {}
cat = catalog()
for step in (1, 2):
    code = build.main("eurostat_dataset_versions eurostat_observations", target="trial",
                      variables={"eurostat_datasets_per_build": 5})
    table = cat.load_table(("zz_dbt_trial", "eurostat_observations"))
    arrow = table.scan(selected_fields=("dataset_id", "time_period", "value")).to_arrow()
    out[f"build_{step}"] = {"exit": code, "rows": arrow.num_rows,
                            "datasets": len(set(arrow.column("dataset_id").to_pylist())),
                            "periods_sample": sorted(set(arrow.column("time_period").to_pylist()))[:6]}
versions = cat.load_table(("zz_dbt_trial", "eurostat_dataset_versions")).scan().to_arrow().num_rows
out["datasets_with_versions"] = versions
for name in ("eurostat_observations", "eurostat_dataset_versions"):
    try:
        cat.drop_table(("zz_dbt_trial", name))
    except Exception as exc:
        out[f"drop_{name}"] = str(exc)[:200]
annotate("notice", "eurostat silver trial", out)
