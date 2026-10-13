"""Polite client for the BDL API (https://bdl.stat.gov.pl/api/v1).

Uses the registered key (env GUS_BDL_API_KEY, header X-ClientId) and stays below its
limits (10/s, 500/15 min, 5,000/12 h, 50,000/7 days) with a local budget; a 429 waits for
the reset the server reports. One client per process; never parallel.
"""
from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
import os
import time
from typing import Any, Iterator
from zoneinfo import ZoneInfo

import requests

BASE = "https://bdl.stat.gov.pl/api/v1"
WARSAW = ZoneInfo("Europe/Warsaw")
WINDOWS = ((1, 8), (15 * 60, 450), (12 * 3600, 4800))   # (seconds, max requests), below the key's limits


class Bdl:
    def __init__(self, key: str | None = None):
        self.session = requests.Session()
        key = key if key is not None else os.environ.get("GUS_BDL_API_KEY", "")
        if key:
            self.session.headers["X-ClientId"] = key
        self.session.headers["Accept"] = "application/json"
        self.calls: deque[float] = deque()
        self.requests = 0

    def _wait(self) -> None:
        while True:
            now = time.monotonic()
            while self.calls and now - self.calls[0] > WINDOWS[-1][0]:
                self.calls.popleft()
            delay = 0.0
            for span, limit in WINDOWS:
                recent = [t for t in self.calls if now - t < span]
                if len(recent) >= limit:
                    delay = max(delay, span - (now - recent[0]) + 0.05)
            if delay <= 0:
                return
            time.sleep(min(delay, 60))

    def get(self, path: str, **params: Any) -> dict:
        params.setdefault("format", "json")
        for attempt in range(8):
            self._wait()
            self.calls.append(time.monotonic())
            self.requests += 1
            try:
                response = self.session.get(f"{BASE}{path}", params=params, timeout=60)
            except (requests.ConnectionError, requests.Timeout):
                time.sleep(2 ** attempt)
                continue
            if response.status_code == 429:
                reset = response.headers.get("X-Rate-Limit-Reset") or response.headers.get("Retry-After") or "60"
                time.sleep(min(max(float(reset) if reset.replace(".", "").isdigit() else 60, 5), 900))
                continue
            if response.status_code >= 500:
                time.sleep(2 ** attempt * 2)
                continue
            response.raise_for_status()
            return response.json()
        raise RuntimeError(f"BDL API kept failing: {path} {params}")

    def pages(self, path: str, **params: Any) -> Iterator[dict]:
        page = 0
        while True:
            body = self.get(path, page=page, **{"page-size": 100, **params})
            yield from body.get("results") or []
            if not (body.get("links") or {}).get("next"):
                return
            page += 1

    def subgroups(self) -> Iterator[dict]:
        """Every subject with variables (P####), walking categories → groups → subgroups."""
        stack = [s["id"] for s in self.pages("/subjects", lang="pl")]
        while stack:
            parent = stack.pop()
            for subject in self.pages("/subjects", lang="pl", **{"parent-id": parent}):
                if subject.get("hasVariables"):
                    yield subject
                if subject.get("children"):
                    stack.append(subject["id"])

    def last_update(self, subject_id: str, listed: dict | None = None) -> datetime | None:
        """The subgroup's lastUpdate in UTC (the API reports Warsaw local time without offset).

        Uses the value from the subject listing when it carries one (no extra request)."""
        value = (listed or {}).get("lastUpdate") or self.get(f"/subjects/{subject_id}", lang="pl").get("lastUpdate")
        if not value:
            return None
        parsed = datetime.fromisoformat(value)
        return (parsed if parsed.tzinfo else parsed.replace(tzinfo=WARSAW)).astimezone(timezone.utc)


def token(moment: datetime | None) -> str:
    """Layout timestamp YYYYMMDDTHHMMSSZ; an unknown version sorts first."""
    return moment.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ") if moment else "19700101T000000Z"
