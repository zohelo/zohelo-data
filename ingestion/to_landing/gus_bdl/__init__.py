"""GUS BDL → Landing (docs/landing.md). Entity = subgroup (P####), version = its lastUpdate.

landing.inspect   inspect.py: subgroups' lastUpdate (BDL API) vs Landing → current / stale / missing
landing.extract   metadata.py: variables of every subgroup with data, as API pages (resumable);
                  the filter selects subgroups (globs); "units" in the filter also lands the unit list.
                  "web" in the filter runs web_export.py instead: subgroups missing from Landing,
                  exported from the website (optionally only the P#### listed; "split" also
                  exports subgroups that need territory parts); "web packages" runs
                  web_packages.py: collect ready whole-subgroup packages, request missing ones;
                  "api P#### …" runs refresh.py --initial for the listed subgroups (#889), then
                  metadata.py for them; "initial [P#### …]" runs the whole initial load (_initial)
"""

from __future__ import annotations

from datetime import UTC, datetime
import time


def _inspect(p) -> int:
    from ingestion.to_landing.gus_bdl import inspect

    raw = [x for x in (p.filter or "").replace(",", " ").split() if x]
    if "initial" in raw:  # the initial-load run checks against the API inside landing.extract
        return 0
    tokens = [x for x in raw if x not in ("web", "split", "packages", "api")]
    return inspect.main([x for x in tokens if x])


API_RUNS_KEY = "_control/gus_bdl/api_runs.json"  # lakehouse bucket: when the last API step started


def _api_window_open() -> bool:
    """True (and the start recorded) when the last API step started 12 h or more ago: runs every
    6 h drift with queueing, so a clock-hour rule could put two API steps in one window."""
    import json

    import botocore.exceptions

    from ingestion.common.r2 import req, s3

    client, bucket = s3(), req("R2_LAKEHOUSE_BUCKET")
    now = datetime.now(UTC)
    try:
        last = datetime.fromisoformat(
            json.loads(client.get_object(Bucket=bucket, Key=API_RUNS_KEY)["Body"].read())["last_api_at"]
        )
        if (now - last).total_seconds() < 12 * 3600:
            return False
    except (botocore.exceptions.ClientError, KeyError, ValueError):
        pass
    client.put_object(Bucket=bucket, Key=API_RUNS_KEY, Body=json.dumps({"last_api_at": now.isoformat()}).encode())
    return True


def _initial(p, tokens: list[str]) -> int:
    """The BDL initial load in one run (owner decisions #889; scheduled every 6 h). With
    parallel_jobs n (owner, 23 Oct: parallel website sessions, start with 3, up to 10), job 0
    runs steps 1–4 and jobs 1…n-1 only step 4, each on its own share of the small subgroups:
    1. website packages of the big missing subgroups: collect ready ones, request missing ones;
    2. the listed cheapest subgroups through the API, until the key's 12 h limit;
    3. variables of every subgroup version landed without them (for Silver);
    4. the small missing subgroups from the website (whole table, else territory parts),
       with the rest of the time; parts land as they download, so the next run resumes.
    Each step stops at its own share of the budget; a step's crash is recorded and the others
    still run. Exit 0 unless every step crashed: an unfinished initial load is the normal state,
    and Bronze/Silver must still load what landed."""
    from ingestion.common.report import annotate
    from ingestion.to_landing.gus_bdl import metadata, refresh, web_export, web_packages
    from ingestion.to_landing.gus_bdl.api import BudgetSpent

    budget = min(p.budget_minutes, 330)
    shard = (p.job_index, p.parallel_jobs) if p.parallel_jobs > 1 else None
    first = p.job_index == 0  # steps 1–3 (packages, API, variables) run in job 0 only: one package list, one API key
    end = time.monotonic() + budget * 60
    left = lambda: max((end - time.monotonic()) / 60, 0)  # noqa: E731
    api_list = [t for t in tokens if t.startswith("P")]
    outcome: dict = {}

    def step(name, call) -> None:  # one step's crash is recorded; the next steps still run
        try:
            outcome[name] = call()
        except BudgetSpent:
            outcome[name] = "stopped: API limit"
        except Exception as exc:
            outcome[name] = f"crashed: {type(exc).__name__}: {str(exc)[:200]}"

    if first:
        step("packages", lambda: web_packages.main(budget_minutes=min(60.0, left() * 0.25)))
        # at most one API step per 12 h window of the key: the time of the last one is kept
        if api_list and left() > 30 and _api_window_open():
            api_minutes = min(150.0, left() * 0.5)
            # ≤3,000 requests, plus ~400 for the rest of the two runs in a 12 h window: under 5,000
            step(
                "api",
                lambda: refresh.main(
                    ["--initial", "--only", *api_list, "--budget-minutes", str(api_minutes), "--max-requests", "3000"]
                ),
            )
        step("variables", lambda: metadata.main(["--budget-minutes", str(min(20.0, max(left() * 0.1, 5)))]))
    if left() > 20:
        step(
            "website_small",
            lambda: web_export.main(
                limit=10_000,
                budget_minutes=left() - 14,
                split=True,
                small_only=True,
                exclude=set(api_list),
                shard=shard,
            ),
        )
    crashed = [k for k, v in outcome.items() if str(v).startswith("crashed")]
    summary = {"steps": outcome, "minutes_left": round(left())}
    annotate("error" if crashed else "notice", "gus_bdl initial load", summary)
    return 1 if crashed and len(crashed) == len(outcome) else 0


def _extract(p) -> int:
    from ingestion.to_landing.gus_bdl import metadata

    tokens = [x for x in (p.filter or "").replace(",", " ").split() if x]
    if "initial" in tokens:
        return _initial(p, tokens)
    if "api" in tokens:  # owner decision #889: the listed subgroups through the API, then their variables
        from ingestion.to_landing.gus_bdl import metadata, refresh

        only = [t for t in tokens if t.startswith("P")]
        if not only:
            raise SystemExit("'api' needs the subgroups to load (P####)")
        budget = min(p.budget_minutes, 330)
        started = time.monotonic()
        status = refresh.main(["--initial", "--only", *only, "--budget-minutes", str(budget * 0.85)])
        left = max(budget - (time.monotonic() - started) / 60, 10)
        meta = metadata.main(["--filter", ",".join(only), "--budget-minutes", str(left)])
        return status or meta
    if "web" in tokens and "packages" in tokens:
        from ingestion.to_landing.gus_bdl import web_packages

        if p.parallel_jobs > 1:
            raise SystemExit("the BDL website export runs as one job (parallel_jobs 1)")
        return web_packages.main(budget_minutes=min(p.budget_minutes, 330))
    if "web" in tokens:
        from ingestion.to_landing.gus_bdl import web_export

        if p.parallel_jobs > 1:  # one account, one browser: packages of parallel sessions would be ambiguous
            raise SystemExit("the BDL website export runs as one job (parallel_jobs 1)")
        only = [t for t in tokens if t.startswith("P")] or None
        return web_export.main(
            limit=10_000, budget_minutes=min(p.budget_minutes, 330), only=only, split="split" in tokens
        )
    args = [
        "--filter",
        ",".join(t for t in tokens if t != "units"),
        "--budget-minutes",
        str(min(p.budget_minutes, 330)),
    ]
    return metadata.main(args + (["--units"] if "units" in tokens else []))


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract}
