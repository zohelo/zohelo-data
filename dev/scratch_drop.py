"""Drop every table left in the scratch namespace zz_dev_gold_stat (Gold trials of #2743/#2737), with pauses on
HTTP 429; then the namespace itself. Reports what it dropped. Deleted before merge."""

import time

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

NS = "zz_dev_gold_stat"


def main():
    cat = catalog()
    dropped, failed = 0, []
    for _ in range(5):
        left = list(cat.list_tables(NS))
        if not left:
            break
        for ident in left:
            for wait in (0, 10, 30, 60):
                time.sleep(wait)
                try:
                    cat.drop_table(ident)
                    dropped += 1
                    break
                except Exception as exc:  # 429: wait and retry
                    err = f"{ident[-1]}: {type(exc).__name__}"
            else:
                failed.append(err)
            time.sleep(1)
    left = [i[-1] for i in cat.list_tables(NS)]
    if not left:
        try:
            cat.drop_namespace(NS)
        except Exception as exc:  # reported
            failed.append(f"namespace: {type(exc).__name__}")
    annotate("notice" if not left else "warning", "scratch drop", {"dropped": dropped, "left": left, "failed": failed[:10]})


if __name__ == "__main__":
    main()
