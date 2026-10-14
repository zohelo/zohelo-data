"""Read-only: every namespace and table with location and size (finds duplicated tables)."""
import traceback
from urllib.parse import urlsplit

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

try:
    cat = catalog()
    rows = []
    for ns in cat.list_namespaces():
        for ident in cat.list_tables(ns):
            t = cat.load_table(ident)
            s = t.current_snapshot().summary if t.current_snapshot() else {}
            rows.append([".".join(ident), urlsplit(t.location()).path.strip("/")[:60],
                         round(int(s.get("total-files-size") or 0) / 1e9, 2)])
    by_ns = {}
    for name, _, gb in rows:
        ns = name.split(".")[0]
        by_ns[ns] = round(by_ns.get(ns, 0) + gb, 2)
    annotate("notice", "namespaces gb", by_ns)
    annotate("notice", "largest tables", sorted(rows, key=lambda r: -r[2])[:25])
except Exception:
    annotate("error", "namespaces probe", {"traceback": traceback.format_exc()[-1500:]})
    raise
