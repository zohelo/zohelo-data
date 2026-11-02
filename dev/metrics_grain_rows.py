"""Dev diagnostic for #2219: show the rows metrics_grain fails on in prod Gold (read only)."""

from pathlib import Path
import subprocess

from ingestion.common.report import annotate

BASE = ["--target", "prod", "--project-dir", "warehouse", "--profiles-dir", "warehouse"]


def main() -> int:
    """Compile the test, run its SQL with dbt show and annotate the output."""
    subprocess.run(["dbt", "compile", "--select", "metrics_grain", *BASE], capture_output=True, text=True)
    compiled = next(Path("warehouse/target/compiled").rglob("metrics_grain.sql")).read_text()
    shown = subprocess.run(["dbt", "show", "--inline", compiled, "--limit", "20", *BASE], capture_output=True, text=True)
    annotate("notice", "metrics_grain rows", {"rc": shown.returncode, "out": shown.stdout[-3000:]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
