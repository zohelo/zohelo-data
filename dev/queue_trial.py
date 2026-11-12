"""Trial: jobs of one concurrency group with `queue: max` run one after another, none cancelled."""

from datetime import UTC, datetime
import sys
import time

start = datetime.now(UTC).isoformat(timespec="seconds")
time.sleep(int(sys.argv[1]) if len(sys.argv) > 1 else 40)
print(f"::notice title=queue trial::start={start} end={datetime.now(UTC).isoformat(timespec='seconds')}")
