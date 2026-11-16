"""RPWDL register files: Landing activities (register PLS-020).

The 18 ZIPs are declared in ingestion/sources/rpwdl.yaml and land through the shared bulk_file pattern. This module
only adds a pause between the downloads (the endpoint builds each ZIP on request; one request at a time, 5 s apart).

landing.inspect  declared files vs Landing (HEAD per file); no writes
landing.extract  every ZIP, written only when its SHA-256 changed
landing.verify   every receipt has its file (Landing or Archive)
"""

from __future__ import annotations

SOURCE = "rpwdl"
PAUSE_SECONDS = 5.0


def _run(mode: str, landing_root: str = "01_landing") -> int:
    from ingestion.to_landing.patterns import bulk_file

    pause = PAUSE_SECONDS if mode == "load" else 0.0
    return bulk_file.main(SOURCE, mode, landing_root=landing_root, pause_seconds=pause)


ACTIVITIES = {
    "landing.inspect": lambda p: _run("inspect"),
    "landing.extract": lambda p: _run("load"),
    "landing.verify": lambda p: _run("verify"),
}
