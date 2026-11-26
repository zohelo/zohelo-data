"""Dev probe 2 of WIPO IP Statistics (GL-032, #1262): read-only. Is office '**' / origin '**' a total, is the
technology / class dimension a partition (sum against known office totals), which codes are not ISO2. Log only."""

from __future__ import annotations

import io
import json
import sys
import tempfile
import time
import zipfile
from pathlib import Path

import duckdb
import requests

from ingestion.to_landing.patterns.bulk_file import USER_AGENT

FILES = {
    "patent": "https://www.wipo.int/documents/d/ip-statistics/wipo-data-patent-indicators.zip",
    "design": "https://www.wipo.int/documents/d/ip-statistics/docs-en-wipo-data-design-indicators.zip",
    "trademark": "https://www.wipo.int/documents/d/ip-statistics/wipo-data-trademark-indicators.zip",
}


def log(title, data):
    print(f"### {title}: {json.dumps(data, default=str, ensure_ascii=False)}", flush=True)


def q(con, sql):
    return [[str(v) for v in r] for r in con.execute(sql).fetchall()]


def main(argv):
    del argv
    con = duckdb.connect()
    tmp = Path(tempfile.mkdtemp())
    for kind, url in FILES.items():
        time.sleep(10)
        body = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=(30, 300)).content
        with zipfile.ZipFile(io.BytesIO(body)) as z:
            for m in z.namelist():
                if not m.endswith(".csv"):
                    continue
                p = tmp / m
                p.write_bytes(z.read(m))
                t = f"{kind}_{m.split('_')[3]}_{m.split('_')[4]}"
                dim = "tec_id" if kind == "patent" else "class_id"
                con.execute(f"create table {t} as select year::int year, office, origin, {dim}::int k, count::bigint n "
                            f"from read_csv('{p}', all_varchar=true)")
                log(f"{t} rows", q(con, f"select count(*), min(year), max(year), min(n), max(n), count(*) filter (where n = 0) from {t}"))
                log(f"{t} dup keys", q(con, f"select count(*) from (select year, office, origin, k from {t} group by all having count(*) > 1)"))
                log(f"{t} office codes", q(con, f"select office, count(*), sum(n) from {t} group by 1 order by 1"))
                log(f"{t} origin non-iso-like", q(con, f"select origin, count(*), sum(n) from {t} where not regexp_full_match(origin, '[A-Z]{{2}}') group by 1"))
                log(f"{t} origins", q(con, f"select string_agg(distinct origin, ' ' order by origin) from {t}"))
                log(f"{t} office ** vs sum of other offices (n, star_ge_sum, star_lt_max_other, ratio_star_to_sum)", q(con, f"""
                    with a as (select year, origin, k, sum(n) filter (where office = '**') s, sum(n) filter (where office <> '**') o,
                                      max(n) filter (where office <> '**') mx from {t} group by all)
                    select count(*), count(*) filter (where s >= o), count(*) filter (where s < mx), round(sum(s) / sum(o), 3)
                    from a where s is not null and o is not null"""))
                log(f"{t} origin ** vs sum of other origins", q(con, f"""
                    with a as (select year, office, k, sum(n) filter (where origin = '**') s, sum(n) filter (where origin <> '**') o,
                                      max(n) filter (where origin <> '**') mx from {t} group by all)
                    select count(*), count(*) filter (where s >= o), count(*) filter (where s < mx), round(sum(s) / sum(o), 3)
                    from a where s is not null and o is not null"""))
                log(f"{t} k 0 share by year", q(con, f"select year, round(sum(n) filter (where k = 0) / sum(n), 3) from {t} group by 1 order by 1"))
                for off in ("US", "CN", "JP", "EP", "PL", "EM", "DE", "KR", "**"):
                    log(f"{t} office {off} by year (sum over origin and k; resident; k0)", q(con, f"""
                        select year, sum(n), sum(n) filter (where origin = '{off}'), sum(n) filter (where k = 0)
                        from {t} where office = '{off}' group by 1 order by 1"""))
                log(f"{t} origin PL by year (all; at PL; at EP/EM; at **)", q(con, f"""
                    select year, sum(n), sum(n) filter (where office = 'PL'), sum(n) filter (where office in ('EP', 'EM')),
                           sum(n) filter (where office = '**')
                    from {t} where origin = 'PL' group by 1 order by 1"""))
                log(f"{t} PL office by k 2022", q(con, f"select k, sum(n) from {t} where office = 'PL' and year = 2022 group by 1 order by 1"))
                log(f"{t} distinct (office,k) (origin,k) (office,k,yr) (origin,k,yr) (office,yr) (origin,yr) (office,origin,yr)", q(con, f"""
                    select (select count(*) from (select distinct office, k from {t})), (select count(*) from (select distinct origin, k from {t})),
                    (select count(*) from (select distinct office, k, year from {t})), (select count(*) from (select distinct origin, k, year from {t})),
                    (select count(*) from (select distinct office, year from {t})), (select count(*) from (select distinct origin, year from {t})),
                    (select count(*) from (select distinct office, origin, year from {t}))"""))
                con.execute(f"drop table {t}")
                p.unlink()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
