#!/usr/bin/env python3
"""Cloudflare side of the data portal (data.zohelo.com), idempotent; run by deploy-portal.yml.

- Pages project `zohelo-data` with the API's bindings: R2 buckets LANDING and LAKEHOUSE, the
  allowed emails and a fresh download-link signing key (rotated on every deploy, as before).
- Cloudflare Access app `zohelo-data-portal` over data.zohelo.com, zohelo-data.pages.dev and every
  preview subdomain: one-time PIN to the allowed emails (the shared policy "zohelo portal sign-in").
- With --switch-dns (main only): attach data.zohelo.com to the project and point its DNS record at
  it, replacing the old GitHub Pages CNAME. The old value is printed for a rollback.

Env: CLOUDFLARE_API_TOKEN, CLOUDFLARE_ACCOUNT_ID, PORTAL_SIGNIN_ALLOWED_EMAILS,
     STORAGE_LANDING_BUCKET_NAME, STORAGE_LAKEHOUSE_BUCKET_NAME.
"""

from __future__ import annotations

import argparse
import os
import secrets
import sys
from typing import Any

import requests

PROJECT = "zohelo-data"
DOMAIN = "data.zohelo.com"
ZONE = "zohelo.com"
APP_NAME = "zohelo-data-portal"
POLICY_NAME = "zohelo portal sign-in"
API = "https://api.cloudflare.com/client/v4"


def env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        sys.exit(f"missing env {name}")
    return value


TOKEN = env("CLOUDFLARE_API_TOKEN")
ACCOUNT = env("CLOUDFLARE_ACCOUNT_ID")
A = f"/accounts/{ACCOUNT}"


def cf(method: str, path: str, body: Any = None, allow_404: bool = False) -> Any:
    response = requests.request(method, API + path, json=body, timeout=60, headers={"Authorization": f"Bearer {TOKEN}"})
    if allow_404 and response.status_code == 404:
        return None
    payload = response.json()
    if not payload.get("success"):
        sys.exit(f"{method} {path}: {response.status_code} {payload.get('errors')}")
    return payload["result"]


def notice(title: str, message: str) -> None:
    print(f"::notice title={title}::{message}")


def pages_project(emails: list[str]) -> None:
    team_domain = cf("GET", f"{A}/access/organizations")["auth_domain"]
    runtime = {
        "compatibility_date": "2026-09-30",
        "r2_buckets": {
            "LANDING": {"name": env("STORAGE_LANDING_BUCKET_NAME")},
            "LAKEHOUSE": {"name": env("STORAGE_LAKEHOUSE_BUCKET_NAME")},
        },
        "env_vars": {
            "PORTAL_SIGNIN_ALLOWED_EMAILS": {"type": "secret_text", "value": ",".join(emails)},
            "DOWNLOAD_SIGNING_KEY": {"type": "secret_text", "value": secrets.token_hex(32)},
            # The API verifies Access tokens against this team's public keys.
            "ACCESS_TEAM_DOMAIN": {"type": "plain_text", "value": team_domain},
        },
    }
    if cf("GET", f"{A}/pages/projects/{PROJECT}", allow_404=True) is None:
        cf("POST", f"{A}/pages/projects", {"name": PROJECT, "production_branch": "main"})
        notice("pages project", f"created {PROJECT}")
    cf("PATCH", f"{A}/pages/projects/{PROJECT}", {"deployment_configs": {"production": runtime, "preview": runtime}})
    notice("pages project", f"{PROJECT}: R2 bindings and variables set")


def access(emails: list[str]) -> None:
    idps = cf("GET", f"{A}/access/identity_providers") or []
    otp = next((i for i in idps if i["type"] == "onetimepin"), None)
    if otp is None:
        otp = cf("POST", f"{A}/access/identity_providers", {"name": "One-time PIN", "type": "onetimepin", "config": {}})
    policy_body = {
        "name": POLICY_NAME,
        "decision": "allow",
        "include": [{"email": {"email": e}} for e in emails],
        "session_duration": "24h",
    }
    policy = next((p for p in cf("GET", f"{A}/access/policies") or [] if p["name"] == POLICY_NAME), None)
    policy = (
        cf("PUT", f"{A}/access/policies/{policy['id']}", policy_body)
        if policy
        else cf("POST", f"{A}/access/policies", policy_body)
    )
    hostnames = [DOMAIN, f"{PROJECT}.pages.dev", f"*.{PROJECT}.pages.dev"]
    app_body = {
        "name": APP_NAME,
        "type": "self_hosted",
        "domain": DOMAIN,
        "destinations": [{"type": "public", "uri": h} for h in hostnames],
        "session_duration": "24h",
        "allowed_idps": [otp["id"]],
        "auto_redirect_to_identity": True,
        "app_launcher_visible": False,
        "policies": [{"id": policy["id"], "precedence": 1}],
    }
    app = next((x for x in cf("GET", f"{A}/access/apps") or [] if x["name"] == APP_NAME), None)
    if app:
        cf("PUT", f"{A}/access/apps/{app['id']}", app_body)
    else:
        cf("POST", f"{A}/access/apps", app_body)
    notice("access", f"{APP_NAME} protects {', '.join(hostnames)} for {len(emails)} email(s)")


def switch_dns() -> None:
    domains = cf("GET", f"{A}/pages/projects/{PROJECT}/domains") or []
    if not any(d["name"] == DOMAIN for d in domains):
        cf("POST", f"{A}/pages/projects/{PROJECT}/domains", {"name": DOMAIN})
        notice("domain", f"{DOMAIN} attached to {PROJECT}")
    zone = (cf("GET", f"/zones?name={ZONE}") or [{}])[0].get("id")
    if not zone:
        sys.exit(f"zone {ZONE} not visible to the token")
    target = f"{PROJECT}.pages.dev"
    records = cf("GET", f"/zones/{zone}/dns_records?name={DOMAIN}") or []
    cname = next((r for r in records if r["type"] == "CNAME"), None)
    others = [r for r in records if r["type"] != "CNAME"]
    if others:
        sys.exit(f"{DOMAIN} has other DNS records ({', '.join(r['type'] for r in others)}); not touching them")
    if cname is None:
        cf("POST", f"/zones/{zone}/dns_records", {"type": "CNAME", "name": DOMAIN, "content": target, "proxied": True})
        notice("dns", f"{DOMAIN} CNAME {target} created")
    elif cname["content"] != target or not cname.get("proxied"):
        notice("dns rollback value", f"{DOMAIN} was CNAME {cname['content']} (proxied={cname.get('proxied')})")
        cf("PATCH", f"/zones/{zone}/dns_records/{cname['id']}", {"content": target, "proxied": True})
        notice("dns", f"{DOMAIN} now CNAME {target} (proxied)")
    attached = cf("GET", f"{A}/pages/projects/{PROJECT}/domains") or []
    status = next((d["status"] for d in attached if d["name"] == DOMAIN), "unknown")
    notice("domain", f"{DOMAIN}: {status}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--switch-dns", action="store_true", help="attach the custom domain and point DNS at Pages")
    args = parser.parse_args()
    emails = [e.strip() for e in env("PORTAL_SIGNIN_ALLOWED_EMAILS").split(",") if e.strip()]
    pages_project(emails)
    access(emails)
    if args.switch_dns:
        switch_dns()


if __name__ == "__main__":
    main()
