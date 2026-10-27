"""Trial: pyarrow 23.0.1 Arrow round trip through the R2 Data Catalog (#958).

Scans silver.nbp_fx_rates to Arrow, writes it to a scratch namespace, reads it back,
compares row counts, then drops the scratch table and namespace.
"""

from __future__ import annotations

import duckdb
import pandas
import pyarrow
import pyiceberg

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

NAMESPACE = "zz_dev_pyarrow"
TABLE = f"{NAMESPACE}.nbp_fx_rates"
LIMIT = 100_000


def main() -> None:
    annotate("notice", "versions", {
        "pyarrow": pyarrow.__version__, "pyiceberg": pyiceberg.__version__,
        "duckdb": duckdb.__version__, "pandas": pandas.__version__,
    })
    cat = catalog()
    source = cat.load_table("silver.nbp_fx_rates").scan(limit=LIMIT).to_arrow()
    cat.create_namespace_if_not_exists(NAMESPACE)
    try:
        table = cat.create_table(TABLE, schema=source.schema)
        table.append(source)
        back = cat.load_table(TABLE).scan().to_arrow()
        counted = duckdb.sql("select count(*) from back").fetchone()[0]
        rows = {"source": source.num_rows, "read_back": back.num_rows, "duckdb": counted,
                "pandas": len(back.to_pandas())}
        if len(set(rows.values())) != 1:
            annotate("error", "rows differ", rows)
            raise SystemExit(1)
        annotate("notice", "rows equal", rows)
    finally:
        if cat.table_exists(TABLE):
            cat.drop_table(TABLE)
        cat.drop_namespace(NAMESPACE)
        gone = NAMESPACE not in {ns[0] for ns in cat.list_namespaces()}
        annotate("notice" if gone else "error", "dropped zz_dev_pyarrow", {"namespace_gone": gone})
        if not gone:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
