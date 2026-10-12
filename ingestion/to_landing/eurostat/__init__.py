"""Eurostat Landing activities (code in extract.py).

landing.inspect   extract.py --mode inspect   provider inventory vs Landing, no downloads
landing.extract   extract.py --mode load      download new or changed datasets (hash-sliced jobs)
landing.verify    extract.py --mode verify    Landing integrity and remaining gaps
"""
from __future__ import annotations


def _extract(mode: str, p) -> int:
    from ingestion.to_landing.eurostat import extract
    args = ["--mode", mode, "--dataset-filter", p.filter,
            "--parallel-downloads", str(p.parallel_downloads)]
    if mode == "load":
        args += ["--allow-production-write", "--max-datasets", str(p.max_items),
                 "--job-index", str(p.job_index), "--parallel-jobs", str(p.parallel_jobs),
                 "--budget-minutes", str(p.budget_minutes), "--grace-minutes", "20"]
    return extract.main(args)


ACTIVITIES = {
    "landing.inspect": lambda p: _extract("inspect", p),
    "landing.extract": lambda p: _extract("load", p),
    "landing.verify": lambda p: _extract("verify", p),
}
