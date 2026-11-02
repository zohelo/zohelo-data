"""Dev trial for #2307 (read only, prod Gold): the population metrics of this branch have one row per key,
and the metrics_geography_unmatched warning counts the left-out _X rows."""

from pathlib import Path
import subprocess

from ingestion.common.report import annotate

BASE = ["--target", "prod", "--project-dir", "warehouse", "--profiles-dir", "warehouse"]


def main() -> int:
    """Compile the population metrics model and the new test, query them with dbt show, annotate."""
    subprocess.run(["dbt", "compile", "--select", "poc_metrics_population metrics_geography_unmatched", *BASE],
                   capture_output=True, text=True)
    compiled = Path("warehouse/target/compiled")
    model = next(compiled.rglob("poc_metrics_population.sql")).read_text()
    test = next(compiled.rglob("metrics_geography_unmatched.sql")).read_text()
    queries = {
        "duplicate keys (branch metrics_population)": f"""select count(*) as duplicate_keys from (
            select concept_key, source_system, measure_key, geography_key, frequency, date_key
            from ({model}) group by all having count(*) > 1)""",
        "_X rows (branch metrics_population)": f"select count(*) as x_rows from ({model}) where geography_key = '_X'",
        "left out (metrics_geography_unmatched)": f"""select concept_key, source_system, count(*) as n
            from ({test}) group by all""",
    }
    for title, sql in queries.items():
        shown = subprocess.run(["dbt", "show", "--inline", sql, "--limit", "20", *BASE], capture_output=True, text=True)
        out = shown.stdout if shown.returncode == 0 else shown.stdout + shown.stderr
        annotate("notice", title, {"rc": shown.returncode, "out": out[-1500:]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
