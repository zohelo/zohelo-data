#!/usr/bin/env python3
"""Credentials check (writes one scratch object, removes it again).

Proves that every organisation variable and secret this repository uses reaches the job and
works against its service: variables present; deploy token verify and Pages project list;
storage write keys put and delete zz_dev_credcheck/probe.txt in the Landing bucket; lakehouse
read keys list 02_bronze/; catalog read token GET /v1/config and /namespaces; catalog write
token GET /v1/config; GUS BDL key GET one variable; org app installation token GET the repository; the scheduler
Worker's own token code mints a token (run with node). Negative probes: read-only keys must
not write (a probe that succeeds is removed, then the row fails); the deploy token's R2 access is
shown, never fails. Port of the check in zohelo-bi.

Rows are (section, name, expected, actual). Variables are printed, secrets only as presence
and HTTP status codes; error rows quote the last error line (at most 120 characters, secret
values removed). One annotation per section and a job summary table; exit 1 when any actual
differs from its expected value.
"""

from __future__ import annotations

import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import requests

from ingestion.common.report import annotate, step_summary

TIMEOUT = 30
PROBE_KEY = "zz_dev_credcheck/probe.txt"
SCRATCH_NAMESPACE = "zz_dev_credcheck"
INFO = "informational"  # expected value of rows that are shown but never fail the run
VARIABLES = (
    "PLATFORM_ACCOUNT_ID",
    "STORAGE_ENDPOINT_URL",
    "STORAGE_LANDING_BUCKET_NAME",
    "STORAGE_LAKEHOUSE_BUCKET_NAME",
    "STORAGE_LAKEHOUSE_CATALOG_URL",
    "STORAGE_LAKEHOUSE_CATALOG_NAME",
    "ORG_BOT_CLIENT_ID",
)
SECRETS = (
    "PLATFORM_DEPLOY_TOKEN",
    "STORAGE_WRITE_ACCESS_KEY",
    "STORAGE_WRITE_SECRET_KEY",
    "STORAGE_LAKEHOUSE_READ_ACCESS_KEY",
    "STORAGE_LAKEHOUSE_READ_SECRET_KEY",
    "STORAGE_LAKEHOUSE_CATALOG_WRITE_TOKEN",
    "STORAGE_LAKEHOUSE_CATALOG_READ_TOKEN",
    "SOURCE_GUS_BDL_API_KEY",
    "ORG_BOT_PRIVATE_KEY",
    "APP_TOKEN",
)

Row = tuple[str, str, str, str]
rows: list[Row] = []


def env(name: str) -> str:
    return os.environ.get(name, "").strip()


def clean(text: str) -> str:
    """Last line of an error, without any secret value, at most 120 characters."""
    for name in SECRETS:
        if len(env(name)) >= 8:
            text = text.replace(env(name), "***")
    lines = [line for line in str(text).strip().splitlines() if line.strip()]
    return (lines[-1] if lines else "error")[:120]


def row(section: str, name: str, expected: str, actual: str) -> None:
    rows.append((section, name, expected, actual))


def attempt(section: str, name: str, expected: str, probe) -> None:
    """Run a probe returning the actual value; an exception becomes an error row."""
    try:
        actual = probe()
    except Exception as exc:
        actual = f"error: {clean(f'{type(exc).__name__}: {exc}')}"
    row(section, name, expected, actual)


def http(method: str, url: str, **kwargs: Any) -> requests.Response:
    return requests.request(method, url, timeout=TIMEOUT, **kwargs)


def bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def s3_client(access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=env("STORAGE_ENDPOINT_URL").rstrip("/"),
        region_name="auto",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4", retries={"max_attempts": 2, "mode": "standard"}),
    )


def writer():
    return s3_client(env("STORAGE_WRITE_ACCESS_KEY"), env("STORAGE_WRITE_SECRET_KEY"))


def reader():
    return s3_client(env("STORAGE_LAKEHOUSE_READ_ACCESS_KEY"), env("STORAGE_LAKEHOUSE_READ_SECRET_KEY"))


def catalog_base() -> str:
    return env("STORAGE_LAKEHOUSE_CATALOG_URL").rstrip("/")


def catalog_path(token: str) -> str:
    """`/v1[/<prefix>]` of the warehouse, from GET /v1/config."""
    response = http(
        "GET",
        f"{catalog_base()}/v1/config",
        params={"warehouse": env("STORAGE_LAKEHOUSE_CATALOG_NAME")},
        headers=bearer(token),
    )
    prefix = ""
    if response.ok:
        body = response.json()
        prefix = (body.get("overrides") or {}).get("prefix") or (body.get("defaults") or {}).get("prefix") or ""
    return f"/v1/{prefix}" if prefix else "/v1"


def variables_section() -> None:
    for name in VARIABLES:
        row("variables", name, "set", env(name) or "MISSING")
    for name in SECRETS:
        row("variables", name, "set", "set" if env(name) else "MISSING")


def deploy_section() -> None:
    account = f"https://api.cloudflare.com/client/v4/accounts/{env('PLATFORM_ACCOUNT_ID')}"
    headers = bearer(env("PLATFORM_DEPLOY_TOKEN"))

    def verify() -> str:
        response = http("GET", "https://api.cloudflare.com/client/v4/user/tokens/verify", headers=headers)
        status = (response.json().get("result") or {}).get("status", "") if response.ok else ""
        return f"{response.status_code} {status}".strip()

    def pages() -> str:
        response = http("GET", f"{account}/pages/projects", headers=headers)
        return str(response.status_code)

    attempt("deploy token", "token verify", "200 active", verify)
    attempt("deploy token", "list Pages projects", "200", pages)


def storage_section() -> None:
    landing, lakehouse = env("STORAGE_LANDING_BUCKET_NAME"), env("STORAGE_LAKEHOUSE_BUCKET_NAME")

    def put() -> str:
        writer().put_object(Bucket=landing, Key=PROBE_KEY, Body=b"check")
        return "ok"

    def delete() -> str:
        writer().delete_object(Bucket=landing, Key=PROBE_KEY)
        return "ok"

    def list_bronze() -> str:
        reader().list_objects_v2(Bucket=lakehouse, Prefix="02_bronze/", MaxKeys=1)
        return "ok"

    attempt("storage", "write keys: put probe in landing", "ok", put)
    attempt("storage", "write keys: delete probe", "ok", delete)
    attempt("storage", "read keys: list lakehouse 02_bronze/", "ok", list_bronze)


def catalog_section() -> None:
    base = env("STORAGE_LAKEHOUSE_CATALOG_URL").rstrip("/")
    warehouse = env("STORAGE_LAKEHOUSE_CATALOG_NAME")

    def config(token: str) -> requests.Response:
        return http("GET", f"{base}/v1/config", params={"warehouse": warehouse}, headers=bearer(token))

    def read_namespaces() -> str:
        token = env("STORAGE_LAKEHOUSE_CATALOG_READ_TOKEN")
        return str(http("GET", f"{base}{catalog_path(token)}/namespaces", headers=bearer(token)).status_code)

    attempt(
        "catalog",
        "read token: GET /v1/config",
        "200",
        lambda: str(config(env("STORAGE_LAKEHOUSE_CATALOG_READ_TOKEN")).status_code),
    )
    attempt("catalog", "read token: GET namespaces", "200", read_namespaces)
    attempt(
        "catalog",
        "write token: GET /v1/config",
        "200",
        lambda: str(config(env("STORAGE_LAKEHOUSE_CATALOG_WRITE_TOKEN")).status_code),
    )


def bdl_section() -> None:
    def variable() -> str:
        response = http(
            "GET",
            "https://bdl.stat.gov.pl/api/v1/variables",
            params={"format": "json", "page-size": 1},
            headers={"X-ClientId": env("SOURCE_GUS_BDL_API_KEY")},
        )
        return str(response.status_code)

    attempt("bdl", "GUS BDL API key: GET one variable", "200", variable)


WORKER = Path(__file__).resolve().parent / "scheduler_worker" / "src" / "index.js"
# Imports the Worker's token code exactly as zohelo-bi's check does and mints an installation token.
SNIPPET = """\
const { installationToken } = await import(process.argv[2]);
const env = {
  APP_CLIENT_ID: process.env.ORG_BOT_CLIENT_ID,
  APP_PRIVATE_KEY: process.env.ORG_BOT_PRIVATE_KEY,
  GITHUB_REPOSITORY: process.env.GITHUB_REPOSITORY,
};
try {
  await installationToken(env);
  console.log("ok");
} catch (e) {
  console.log("failed: " + String(e.message).slice(0, 200));
}
"""


def app_section() -> None:
    def org_app() -> str:
        response = http(
            "GET",
            "https://api.github.com/repos/zohelo/zohelo-data",
            headers={**bearer(env("APP_TOKEN")), "Accept": "application/vnd.github+json"},
        )
        return str(response.status_code)

    def worker_token() -> str:
        work = Path(env("RUNNER_TEMP") or tempfile.gettempdir())
        copy = work / "scheduler_worker_check.mjs"
        source = re.sub(r"^export default", "export const worker =", WORKER.read_text(), flags=re.M)
        copy.write_text(source + "\nexport { installationToken };\n")
        snippet = work / "scheduler_worker_check_run.mjs"
        snippet.write_text(SNIPPET)
        wanted = ("PATH", "HOME", "RUNNER_TEMP", "ORG_BOT_CLIENT_ID", "ORG_BOT_PRIVATE_KEY")
        environment = {k: os.environ[k] for k in wanted if k in os.environ}
        environment["GITHUB_REPOSITORY"] = "zohelo/zohelo-data"
        done = subprocess.run(
            ["node", str(snippet), copy.as_uri()],
            env=environment,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        return clean(done.stdout or done.stderr or f"node exit {done.returncode}")

    attempt("app", "org app", "200", org_app)
    attempt("app", "scheduler Worker token", "ok", worker_token)


def denied_section() -> None:
    """Read-only credentials must fail to write; a probe that succeeds is cleaned up, then reported."""
    landing, lakehouse = env("STORAGE_LANDING_BUCKET_NAME"), env("STORAGE_LAKEHOUSE_BUCKET_NAME")

    def s3_denied(call, undo) -> str:
        try:
            call()
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in ("AccessDenied", "Forbidden", "403") or status == 403:
                return "denied"
            return f"error: {clean(str(exc))}"
        try:
            undo()
            return "allowed (probe removed)"
        except Exception as exc:
            return f"allowed (probe NOT removed: {clean(str(exc))})"

    def put_lakehouse() -> str:
        return s3_denied(
            lambda: reader().put_object(Bucket=lakehouse, Key=PROBE_KEY, Body=b"check"),
            lambda: writer().delete_object(Bucket=lakehouse, Key=PROBE_KEY),
        )

    def list_landing() -> str:
        # a successful listing creates nothing, so there is nothing to undo
        return s3_denied(lambda: reader().list_objects_v2(Bucket=landing, MaxKeys=1), lambda: None)

    def create_namespace() -> str:
        read, write = env("STORAGE_LAKEHOUSE_CATALOG_READ_TOKEN"), env("STORAGE_LAKEHOUSE_CATALOG_WRITE_TOKEN")
        path = catalog_path(read)
        response = http(
            "POST", f"{catalog_base()}{path}/namespaces", json={"namespace": [SCRATCH_NAMESPACE]}, headers=bearer(read)
        )
        if response.status_code in (401, 403):
            return str(response.status_code)
        if response.ok:
            removed = http("DELETE", f"{catalog_base()}{path}/namespaces/{SCRATCH_NAMESPACE}", headers=bearer(write))
            return f"{response.status_code} (namespace removed: {removed.status_code})"
        return str(response.status_code)

    def r2_buckets() -> str:
        response = http(
            "GET",
            f"https://api.cloudflare.com/client/v4/accounts/{env('PLATFORM_ACCOUNT_ID')}/r2/buckets",
            headers=bearer(env("PLATFORM_DEPLOY_TOKEN")),
        )
        return "allowed" if response.ok else "denied"

    attempt("denied", "read keys: put in lakehouse", "denied", put_lakehouse)
    attempt("denied", "read keys: list landing", "denied", list_landing)
    attempt("denied", "catalog read token: create namespace", "403", create_namespace)
    attempt("denied", "deploy token: list R2 buckets (informational)", INFO, r2_buckets)


SECTIONS = (
    ("variables", variables_section),
    ("deploy token", deploy_section),
    ("storage", storage_section),
    ("catalog", catalog_section),
    ("bdl", bdl_section),
    ("app", app_section),
    ("denied", denied_section),
)


def report() -> int:
    failed = [r for r in rows if r[2] not in (INFO, r[3])]
    lines = ["## Credentials check", "", "| Section | Check | Expected | Actual | Result |", "|---|---|---|---|---|"]
    for section, name, expected, actual in rows:
        if expected == INFO:
            result = "informational"
        elif expected == actual:
            result = "as expected (denied)" if actual in ("denied", "403") else "as expected"
        else:
            result = "DIFFERS"
        lines.append(f"| {section} | {name} | {expected} | {actual.replace('|', '/')} | {result} |")
    step_summary("\n".join(lines))
    for section, _ in SECTIONS:
        part = [{"check": n, "expected": e, "actual": a} for s, n, e, a in rows if s == section]
        bad = [p for p in part if p["expected"] not in (INFO, p["actual"])]
        annotate("error" if bad else "notice", f"credentials: {section}", bad or part)
    print(f"{len(rows) - len(failed)} of {len(rows)} rows as expected")
    return 1 if failed else 0


def main() -> int:
    for title, section in SECTIONS:
        try:
            section()
        except Exception as exc:
            row(title, "section", "ok", f"error: {clean(f'{type(exc).__name__}: {exc}')}")
    return report()


if __name__ == "__main__":
    sys.exit(main())
