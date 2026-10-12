"""Eurostat activities: bulk TSV distributions from the Files API.

landing.inspect   landing.py --mode inspect
landing.extract   landing.py --mode load (hash-sliced across parallel jobs)
landing.verify    landing.py --mode verify
bronze.load       bronze.py (batches of bronze_batch.py, decode.py per file)
bronze.verify     pipeline/common/lineage.py --source eurostat
landing.archive   pipeline/common/archive.py --source eurostat
"""
from __future__ import annotations


def _landing(mode: str, p) -> int:
    from pipeline.sources.eurostat import landing
    args = ["--mode", mode, "--dataset-filter", p.filter,
            "--parallel-downloads", str(p.parallel_downloads)]
    if mode == "load":
        args += ["--allow-production-write", "--max-datasets", str(p.max_items),
                 "--job-index", str(p.job_index), "--parallel-jobs", str(p.parallel_jobs),
                 "--budget-minutes", str(p.budget_minutes), "--grace-minutes", "20"]
    return landing.main(args)


def _bronze_load(p) -> int:
    from pipeline.sources.eurostat import bronze
    return bronze.load(p.budget_minutes)


def _bronze_verify(p) -> int:
    from pipeline.common import lineage
    return lineage.main(["--source", "eurostat"])


def _archive(p) -> int:
    from pipeline.common import archive
    return archive.main(["--source", "eurostat"])


ACTIVITIES = {
    "landing.inspect": lambda p: _landing("inspect", p),
    "landing.extract": lambda p: _landing("load", p),
    "landing.verify": lambda p: _landing("verify", p),
    "bronze.load": _bronze_load,
    "bronze.verify": _bronze_verify,
    "landing.archive": _archive,
}
