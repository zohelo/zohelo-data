"""Trial of the batched Eurostat Silver in zz_dbt_trial: two builds with a small row
budget, counts after each, then the trial tables are dropped."""
import time

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import build

out = {}
cat = catalog()
for step in (1, 2):
    started = time.time()
    code = build.main("eurostat_dataset_versions eurostat_observations", target="trial",
                      variables={"eurostat_row_budget": 3_000_000})
    table = cat.load_table(("zz_dbt_trial", "eurostat_observations"))
    arrow = table.scan(selected_fields=("dataset_id", "time_period", "value")).to_arrow()
    log = cat.load_table(("zz_dbt_trial", "eurostat_loaded_files")).scan().to_arrow()
    out[f"build_{step}"] = {"exit": code, "seconds": round(time.time() - started), "rows": arrow.num_rows,
                            "datasets": len(set(arrow.column("dataset_id").to_pylist())),
                            "logged_files": log.num_rows,
                            "periods_sample": sorted(set(arrow.column("time_period").to_pylist()))[:6]}
out["datasets_with_versions"] = cat.load_table(("zz_dbt_trial", "eurostat_dataset_versions")).scan().to_arrow().num_rows
for name in ("eurostat_observations", "eurostat_dataset_versions", "eurostat_file_index", "eurostat_loaded_files"):
    try:
        cat.drop_table(("zz_dbt_trial", name))
    except Exception as exc:
        out[f"drop_{name}"] = str(exc)[:200]
annotate("notice", "eurostat silver trial", out)
