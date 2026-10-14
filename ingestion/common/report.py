"""Run reporting shared by every activity.

Results are published as GitHub Actions annotations (readable on the run page
and through the REST API) and, for humans, as the job step summary. GitHub keeps
only about 4 KB of an annotation message, so messages are capped here.
"""

from __future__ import annotations

import json
import os

ANNOTATION_LIMIT = 3900


def annotate(level: str, title: str, data) -> None:
    """Publish data as one annotation; level is notice, warning or error."""
    message = json.dumps(data, default=str, separators=(",", ":"))
    if os.environ.get("GITHUB_ACTIONS") != "true":
        print(f"[{level}] {title}: {message}", flush=True)
        return
    if len(message) > ANNOTATION_LIMIT:
        message = message[: ANNOTATION_LIMIT - 12] + "…[truncated]"
    message = message.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::{level} title={title}::{message}", flush=True)


def step_summary(markdown: str) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(markdown + "\n")
