"""Dev trial for #2546 (deleted before the merge): read-only. For each bulk_file of the given sources, the
HEAD and the GET probe side by side, then the real `landing: inspect` (lists Landing, writes nothing)."""

from pathlib import Path
import sys

import yaml

from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file


def main(sources: list[str]) -> int:
    status = 0
    for source in sources:
        cfg = yaml.safe_load(Path(f"ingestion/sources/{source}.yaml").read_text())
        rows = []
        for spec in cfg["extraction"]["bulk_file"]["files"]:
            url = spec.get("url")
            _, head = bulk_file.probe(url, "head")
            response, get = bulk_file.probe(url, "get")
            rows.append(
                {
                    "entity": spec["entity"],
                    "declared": spec.get("probe", "head"),
                    "head": head,
                    "get": get,
                    "get_headers": {
                        k: response.headers.get(k) for k in ("Content-Type", "Content-Length", "ETag", "Last-Modified")
                    }
                    if response is not None
                    else None,
                }
            )
        annotate("notice", f"dev probe_get {source}: probes", {"files": rows})
        status = max(status, bulk_file.main(source, "inspect"))
    return status


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:] or ["unesco_uis"]))
