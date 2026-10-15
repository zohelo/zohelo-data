#!/usr/bin/env python3
"""The external scheduler clock: Cloudflare Worker `zohelo-scheduler` (#802).

GitHub's `schedule` trigger fires only a fraction of its slots. The Worker in
ops/scheduler_worker/ dispatches the Pipeline run "scheduler: dispatch due runs" on its own
Cron Trigger; the scheduler's catch-up window makes missed or repeated firings harmless.
The Worker's secret GITHUB_TOKEN (fine-grained, this repository, Actions read/write) is set
by the owner in the Cloudflare dashboard; this script only reports whether it is there.

Modes (one argument):
  status   the Worker's Cron Triggers and whether its token is set (default; read-only)
  deploy   deploy the Worker (code, variables, Cron Triggers; its secret is kept)
  pause    remove the Cron Triggers (the Worker stays deployed; GitHub's schedule remains;
           a later `deploy` turns them back on)
  resume   restore the Cron Triggers from wrangler.toml
"""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import tomllib

import requests

from ingestion.common.r2 import req
from ingestion.common.report import annotate

CONFIG = Path(__file__).resolve().parent / "scheduler_worker" / "wrangler.toml"
WRANGLER = ["npx", "--yes", "wrangler@4.146.0"]
API = "https://api.cloudflare.com/client/v4/accounts/{account}/workers/scripts/{script}/{part}"


def cloudflare(method: str, part: str, body: list | None = None) -> tuple[int, dict]:
    """Call the Workers API for this Worker's `schedules` or `secrets`."""
    worker = tomllib.loads(CONFIG.read_text())["name"]
    response = requests.request(
        method,
        API.format(account=req("CLOUDFLARE_ACCOUNT_ID"), script=worker, part=part),
        headers={"Authorization": f"Bearer {req('CLOUDFLARE_API_TOKEN')}"},
        json=body,
        timeout=30,
    )
    try:
        payload = response.json()
    except ValueError:
        payload = {"errors": [response.text[:300]]}
    return response.status_code, {"result": payload.get("result"), "errors": payload.get("errors")}


def deploy() -> list[str]:
    """Deploy code, variables and Cron Triggers with wrangler; the dashboard-set secret is kept."""
    try:
        result = subprocess.run(
            [*WRANGLER, "deploy", "--config", str(CONFIG)], capture_output=True, text=True, timeout=600
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("wrangler deploy timed out after 10 minutes") from exc
    if result.returncode != 0:
        raise RuntimeError(f"wrangler deploy failed: {(result.stderr or result.stdout)[-800:]}")
    return [line.strip() for line in result.stdout.splitlines() if "Version ID" in line or "schedule" in line]


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    mode = argv[0] if argv else "status"
    out: dict = {"mode": mode}
    try:
        if mode == "deploy":
            out["deployed"] = deploy()
        elif mode == "pause":
            out["put"] = cloudflare("PUT", "schedules", [])
        elif mode == "resume":
            crons = tomllib.loads(CONFIG.read_text())["triggers"]["crons"]
            out["put"] = cloudflare("PUT", "schedules", [{"cron": c} for c in crons])
        elif mode != "status":
            raise RuntimeError(f"unknown mode {mode!r} (status, deploy, pause, resume)")
        status, body = cloudflare("GET", "schedules")
        out["schedules"] = {"status": status, **body}
        secrets_status, secrets = cloudflare("GET", "secrets")
        names = [s.get("name") for s in secrets["result"] or []]
        out["token_set"] = "GITHUB_TOKEN" in names
        failed = status != 200 or secrets_status != 200 or ("put" in out and out["put"][0] != 200)
    except (RuntimeError, requests.RequestException) as exc:
        out["error"], failed = str(exc), True
    annotate("error" if failed else "notice", "scheduler trigger", out)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
