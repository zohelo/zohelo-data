"""Dev probe for #491 (KPRM work list, PLG-034; test branch only, never on main).

Read-only: finds the CSV link on https://www.gov.pl/web/premier/wplip-rm (bulk_file url_from), downloads the file
once to a temporary directory (no R2 writes) and profiles it with DuckDB: header, row count, per column non-empty
count, distinct count and longest value, the values of the short closed lists, the shapes of numbers, dates and
the planned-adoption text. Never prints the person column (Osoba odpowiedzialna) or long text values.
"""

from __future__ import annotations

import json
from pathlib import Path
import re
import tempfile

import duckdb

from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

SPEC = bulk_file.declaration("kprm_work_list")[0]


def shape(column: str) -> str:
    return f"regexp_replace(regexp_replace(coalesce({column}, '<null>'), '[0-9]', 'N', 'g'), '\\p{{L}}+', 'L', 'g')"


def main() -> int:
    links = bulk_file.page_links(SPEC)
    annotate("notice", "kprm probe links", {"links": links, "all_register_files": len(links)})
    url = bulk_file.resolve_url(SPEC, links)
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "file.csv"
        digest, size, headers, final, _ = bulk_file.download(url, path, "raw")
        raw = path.read_bytes()
        first = raw.split(b"\n", 1)[0]
        annotate("notice", "kprm probe file", {
            "url": url, "final": final, "sha256": digest, "bytes": size,
            "headers": {k: headers.get(k) for k in ("Content-Type", "Last-Modified", "ETag", "Content-Disposition")},
            "bom": raw[:3] == b"\xef\xbb\xbf", "crlf": b"\r\n" in raw[:20000], "lines_lf": raw.count(b"\n"),
            "header_line": first.decode("utf-8-sig", "replace")[:1500],
        })
        con = duckdb.connect()
        con.execute(
            f"create table t as select * from read_csv('{path}', delim=';', quote='\"', escape='\"', header=true, "
            "all_varchar=true, strict_mode=true, null_padding=false)"
        )
        cols = [r[0] for r in con.execute("describe t").fetchall()]
        n = con.execute("select count(*) from t").fetchone()[0]
        prof = []
        for i, c in enumerate(cols):
            q = f'"{c}"'
            nn, nd, ml = con.execute(
                f"select count({q}) filter (where trim({q}) <> ''), count(distinct {q}), max(length({q})) from t"
            ).fetchone()
            prof.append([i, c, nn, nd, ml])
        annotate("notice", "kprm probe columns", {"rows": n, "columns": len(cols), "profile": prof})
        closed, shapes = {}, {}
        for i, c, nn, nd, ml in prof:
            q = f'"{c}"'
            if "Osoba" in c or (ml or 0) > 400:
                continue
            if nd <= 60:
                closed[c] = con.execute(f"select {q}, count(*) from t group by 1 order by 2 desc").fetchall()
            else:
                extra = f", min({q}), max({q})" if (ml or 0) <= 40 else ""
                shapes[c] = con.execute(
                    f"select left({shape(q)}, 60) s, count(*){extra} from t group by 1 order by 2 desc limit 12"
                ).fetchall()
        text = json.dumps(closed, ensure_ascii=False)
        for k in range(0, len(text), 3500):
            annotate("notice", f"kprm probe closed lists part {k // 3500 + 1}", {"json": text[k:k + 3500]})
        text = json.dumps(shapes, ensure_ascii=False, default=str)
        for k in range(0, len(text), 3500):
            annotate("notice", f"kprm probe shapes part {k // 3500 + 1}", {"json": text[k:k + 3500]})
        number = cols[0]
        annotate("notice", "kprm probe keys", {
            "distinct_numbers": con.execute(f'select count(distinct "{number}") from t').fetchone()[0],
            "prefixes": con.execute(
                f"select regexp_extract(\"{number}\", '^[^0-9]*'), count(*), min(\"{number}\"), max(\"{number}\") "
                "from t group by 1 order by 2 desc"
            ).fetchall(),
            "dup_numbers": con.execute(
                f'select "{number}", count(*) from t group by 1 having count(*) > 1 limit 10'
            ).fetchall(),
        })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
