"""Polite client for the BDL API (https://bdl.stat.gov.pl/api/v1).

Uses the registered key (env GUS_BDL_API_KEY, header X-ClientId) and stays below its
limits (10/s, 500/15 min, 5,000/12 h, 50,000/7 days) with a local budget; a 429 waits for
the reset the server reports. One client per process; never parallel.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterator
from datetime import UTC, datetime
import os
import time
from typing import Any
from zoneinfo import ZoneInfo

import requests

BASE = "https://bdl.stat.gov.pl/api/v1"
WARSAW = ZoneInfo("Europe/Warsaw")
WINDOWS = ((1, 8), (15 * 60, 450), (12 * 3600, 4800))  # (seconds, max requests), below the key's limits


class BudgetSpent(RuntimeError):
    """Waiting for the key's limit would pass the caller's deadline: stop, the next run resumes."""


class Bdl:
    _shared: Bdl | None = None

    @classmethod
    def shared(cls) -> Bdl:
        """The process's one client, so every module's requests count against the same limits."""
        if cls._shared is None:
            cls._shared = cls()
        return cls._shared

    def __init__(self, key: str | None = None):
        self.session = requests.Session()
        key = key if key is not None else os.environ.get("GUS_BDL_API_KEY", "")
        if key:
            self.session.headers["X-ClientId"] = key
        self.session.headers["Accept"] = "application/json"
        self.calls: deque[float] = deque()
        self.requests = 0
        self.deadline: float | None = None  # monotonic; a wait past it raises BudgetSpent
        self._subgroups: list[dict] | None = None

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
            if self.deadline is not None and now + delay > self.deadline:
                raise BudgetSpent(f"the API limit frees up in {delay / 60:.0f} min, after the run's deadline")
            time.sleep(min(delay, 60))

    def get(self, path: str, **params: Any) -> dict:
        return self.get_raw(path, **params)[2]

    def get_raw(self, path: str, **params: Any) -> tuple[str, bytes, dict]:
        """(final URL, exact response bytes, parsed JSON) of one request."""
        params.setdefault("format", "json")
        for attempt in range(8):
            self._wait()
            self.calls.append(time.monotonic())
            self.requests += 1
            try:
                response = self.session.get(f"{BASE}{path}", params=params, timeout=60)
            except (requests.ConnectionError, requests.Timeout):
                time.sleep(2**attempt)
                continue
            if response.status_code == 429:  # the server's count (other processes within 12 h included)
                reset = response.headers.get("X-Rate-Limit-Reset") or response.headers.get("Retry-After") or "60"
                pause = min(max(float(reset) if reset.replace(".", "").isdigit() else 60, 5), 900)
                if self.deadline is not None and time.monotonic() + pause > self.deadline:
                    raise BudgetSpent(f"the API key's limit is reached (429, reset in {pause:.0f} s)")
                time.sleep(pause)
                continue
            if response.status_code >= 500:
                time.sleep(2**attempt * 2)
                continue
            response.raise_for_status()
            return response.url, response.content, response.json()
        raise RuntimeError(f"BDL API kept failing: {path} {params}")

    def pages(self, path: str, **params: Any) -> Iterator[dict]:
        page = 0
        while True:
            body = self.get(path, page=page, **{"page-size": 100, **params})
            yield from body.get("results") or []
            if not (body.get("links") or {}).get("next"):
                return
            page += 1

    def subgroups(self) -> list[dict]:
        """Every subject with variables (P####), walking categories → groups → subgroups; walked
        once per client (~400 requests), so the steps of one run share the list."""
        return [s for s in self.all_subjects() if s.get("hasVariables")]

    def all_subjects(self) -> list[dict]:
        """Every subject — categories, groups and subgroups — walked once per client."""
        if self._subgroups is None:
            top = list(self.pages("/subjects", lang="pl"))
            found, stack = list(top), [s["id"] for s in top]
            while stack:
                parent = stack.pop()
                for subject in self.pages("/subjects", lang="pl", **{"parent-id": parent}):
                    found.append(subject)
                    if subject.get("children"):
                        stack.append(subject["id"])
            self._subgroups = found
        return list(self._subgroups)

    def last_update(self, subject_id: str, listed: dict | None = None) -> datetime | None:
        """The subgroup's lastUpdate in UTC (the API reports Warsaw local time without offset).

        Uses the value from the subject listing when it carries one (no extra request)."""
        value = (listed or {}).get("lastUpdate") or self.get(f"/subjects/{subject_id}", lang="pl").get("lastUpdate")
        if not value:
            return None
        parsed = datetime.fromisoformat(value)
        return (parsed if parsed.tzinfo else parsed.replace(tzinfo=WARSAW)).astimezone(UTC)


def token(moment: datetime | None) -> str:
    """Layout timestamp YYYYMMDDTHHMMSSZ; an unknown version sorts first."""
    return moment.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ") if moment else "19700101T000000Z"
