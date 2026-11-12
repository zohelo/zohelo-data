"""Read-only probe of the Companies House Free Company Data Product (CD-014): listing page, terms page, one
download of the one-file ZIP to the runner's disk, and a profile of its CSV with DuckDB. Nothing is written to R2.
One request at a time; the ZIP is downloaded once. Only aggregates are reported (no addresses, no names except
companies whose country of origin is Poland)."""

from __future__ import annotations

import html
import json
from pathlib import Path
import re
import resource
import shutil
import tempfile
import time
import zipfile

import duckdb
import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

PAGE = "https://download.companieshouse.gov.uk/en_output.html"
TERMS = "https://www.gov.uk/guidance/companies-house-data-products"
KEEP = [
    "CompanyName", "CompanyNumber", "RegAddress.PostTown", "RegAddress.Country", "CompanyCategory", "CompanyStatus",
    "CountryOfOrigin", "DissolutionDate", "IncorporationDate", "SICCode.SicText_1", "SICCode.SicText_2",
    "SICCode.SicText_3", "SICCode.SicText_4",
]
KNOWN = "ORLEN|PKO|KGHM|CD PROJEKT|PZU|ALLEGRO|LOT |COMARCH|ASSECO|PEKAO|MBANK|LPP|DINO|CCC|INPOST|BANK"


def plain(text: str) -> str:
    text = re.sub(r"(?is)<(script|style|noscript).*?</\1>", " ", text)
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"(?s)<[^>]+>", " ", text))).strip()


def get(url: str) -> requests.Response:
    time.sleep(1)
    return requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=60)


def rows(con, sql: str) -> list:
    return [list(r) for r in con.execute(sql).fetchall()]


def main() -> int:
    out: dict = {}
    robots = get("https://download.companieshouse.gov.uk/robots.txt")
    out["robots"] = [robots.status_code, robots.text[:300]]
    page = get(PAGE)
    links = bulk_file.links_matching(page.text, PAGE, r"BasicCompanyData.*\.zip$")
    one = [u for u in links if re.search(r"BasicCompanyDataAsOneFile-\d{4}-\d{2}-\d{2}\.zip$", u)]
    text = plain(page.text)
    out["page"] = {
        "status": page.status_code, "last_modified": page.headers.get("Last-Modified"), "links": links,
        "one_file": one, "text": text[:1500],
    }
    terms = get(TERMS)
    t = plain(terms.text)
    quotes = []
    for needle in ("We impose no rules", "Open Government Licence", "free of charge"):
        quotes += [t[max(0, m.start() - 200): m.start() + 420] for m in re.finditer(re.escape(needle), t)][:2]
    annotate("notice", "companies_house probe: pages", out)
    annotate("notice", "companies_house probe: terms", {"status": terms.status_code, "quotes": quotes})
    print(json.dumps(out), json.dumps(quotes), flush=True)
    if len(one) != 1:
        annotate("error", "companies_house probe", {"one_file_links": one})
        return 1
    with tempfile.TemporaryDirectory(prefix="zohelo-ch-probe-") as temporary:
        work = Path(temporary)
        raw = work / "file.zip"
        started = time.monotonic()
        digest, size, headers, final_url, detail = bulk_file.download(one[0], raw, "zip")
        dl = {
            "url": one[0], "final_url": final_url, "sha256": digest, "bytes": size, "members": detail.get("members"),
            "last_modified": headers.get("Last-Modified"), "etag": headers.get("ETag"),
            "content_type": headers.get("Content-Type"), "accept_ranges": headers.get("Accept-Ranges"),
            "seconds_download_and_crc": round(time.monotonic() - started),
        }
        csv_path = work / "data.csv"
        started = time.monotonic()
        with zipfile.ZipFile(raw) as archive:
            member = archive.infolist()[0]
            with archive.open(member) as src, csv_path.open("wb") as dst:
                shutil.copyfileobj(src, dst, 16 << 20)
        raw.unlink()
        dl["seconds_unzip"] = round(time.monotonic() - started)
        with csv_path.open("rb") as handle:
            first = handle.readline()
            second_len = len(handle.readline())
        dl["header_raw"] = first.decode("utf-8", "replace")
        dl["bom"] = first[:3] == b"\xef\xbb\xbf"
        dl["second_line_bytes"] = second_len
        lines = 0
        with csv_path.open("rb") as handle:
            while block := handle.read(64 << 20):
                lines += block.count(b"\n")
        dl["csv_newlines"] = lines
        annotate("notice", "companies_house probe: download", dl)
        print(json.dumps(dl), flush=True)

        con = duckdb.connect()
        con.execute("SET memory_limit = '8GB'")
        con.execute(f"SET temp_directory = '{work}'")
        read = {}
        for strict in ("true", "false"):
            try:
                con.execute(
                    f"CREATE OR REPLACE VIEW src AS SELECT * FROM read_csv('{csv_path}', delim=',', quote='\"', "
                    f"escape='\"', header=true, all_varchar=true, strict_mode={strict}, encoding='utf-8', "
                    "sample_size=-1)"
                )
                started = time.monotonic()
                read["rows"] = con.execute("SELECT count(*) FROM src").fetchone()[0]
                read["strict_mode"] = strict
                read["seconds_count"] = round(time.monotonic() - started)
                break
            except duckdb.Error as exc:
                read[f"error_strict_{strict}"] = str(exc)[:700]
        names = [r[0] for r in con.execute("DESCRIBE src").fetchall()]
        read["columns"] = names
        by_strip = {n.strip(): n for n in names}
        read["missing_kept"] = [k for k in KEEP if k not in by_strip]
        annotate("notice", "companies_house probe: csv", read)
        print(json.dumps(read), flush=True)

        def c(name: str) -> str:
            return '"' + by_strip[name].replace('"', '""') + '"'

        select = ", ".join(f"{c(k)} AS {re.sub(r'[^a-z0-9]+', '_', k.lower())}" for k in KEEP)
        started = time.monotonic()
        lean = work / "lean.parquet"
        con.execute(f"COPY (SELECT {select} FROM src) TO '{lean}' (FORMAT parquet, COMPRESSION zstd)")
        con.execute(f"CREATE VIEW b AS SELECT * FROM read_parquet('{lean}')")
        prof: dict = {"lean_parquet_bytes": lean.stat().st_size, "seconds_lean": round(time.monotonic() - started)}
        prof["rows"], prof["distinct_numbers"], prof["empty_numbers"], prof["trim_differs"] = con.execute(
            "SELECT count(*), count(DISTINCT companynumber), count(*) FILTER (WHERE coalesce(companynumber,'')=''), "
            "count(*) FILTER (WHERE companynumber <> trim(companynumber) OR companyname <> trim(companyname) "
            "OR countryoforigin <> trim(countryoforigin)) FROM b"
        ).fetchone()
        prof["number_shapes"] = rows(
            con, "SELECT regexp_replace(regexp_replace(companynumber,'[0-9]','9','g'),'9+','9'), length(companynumber)"
            ", count(*) FROM b GROUP BY 1,2 ORDER BY 3 DESC LIMIT 40"
        )
        prof["nulls"] = {
            k: con.execute(f"SELECT count(*) FILTER (WHERE {k} IS NULL), count(*) FILTER (WHERE {k} = '') FROM b").fetchone()
            for k in [re.sub(r"[^a-z0-9]+", "_", k.lower()) for k in KEEP]
        }
        annotate("notice", "companies_house probe: keys", prof)
        print(json.dumps(prof, default=str), flush=True)
        lists = {
            "category": rows(con, "SELECT companycategory, count(*) FROM b GROUP BY 1 ORDER BY 2 DESC"),
            "status": rows(con, "SELECT companystatus, count(*) FROM b GROUP BY 1 ORDER BY 2 DESC"),
        }
        annotate("notice", "companies_house probe: closed lists", lists)
        print(json.dumps(lists), flush=True)
        origin = {
            "distinct": con.execute("SELECT count(DISTINCT countryoforigin) FROM b").fetchone()[0],
            "top": rows(con, "SELECT countryoforigin, count(*) FROM b GROUP BY 1 ORDER BY 2 DESC LIMIT 60"),
            "like_pol": rows(
                con, "SELECT countryoforigin, count(*) FROM b WHERE countryoforigin ILIKE '%POL%' "
                "OR countryoforigin ILIKE '%PL%' GROUP BY 1 ORDER BY 2 DESC LIMIT 40"
            ),
        }
        annotate("notice", "companies_house probe: country of origin", origin)
        print(json.dumps(origin), flush=True)
        con.execute(
            "CREATE VIEW d AS SELECT *, try_strptime(incorporationdate, '%d/%m/%Y')::DATE AS inc, "
            "try_strptime(dissolutiondate, '%d/%m/%Y')::DATE AS dis FROM b"
        )
        dates = {
            "inc_shapes": rows(
                con, "SELECT regexp_replace(incorporationdate,'[0-9]','9','g'), count(*) FROM b GROUP BY 1 ORDER BY 2 DESC LIMIT 8"
            ),
            "dis_shapes": rows(
                con, "SELECT regexp_replace(dissolutiondate,'[0-9]','9','g'), count(*) FROM b GROUP BY 1 ORDER BY 2 DESC LIMIT 8"
            ),
            "inc_unparsed": con.execute(
                "SELECT count(*) FROM d WHERE inc IS NULL AND coalesce(incorporationdate,'') <> ''"
            ).fetchone()[0],
            "inc_min_max": con.execute("SELECT min(inc), max(inc) FROM d").fetchone(),
            "inc_last_days": rows(con, "SELECT inc, count(*) FROM d WHERE inc IS NOT NULL GROUP BY 1 ORDER BY 1 DESC LIMIT 12"),
            "inc_last_months": rows(
                con, "SELECT strftime(inc, '%Y-%m'), count(*) FROM d WHERE inc IS NOT NULL GROUP BY 1 ORDER BY 1 DESC LIMIT 14"
            ),
            "inc_distinct_months": con.execute("SELECT count(DISTINCT date_trunc('month', inc)) FROM d").fetchone()[0],
            "inc_before_1900": con.execute("SELECT count(*) FROM d WHERE inc < DATE '1900-01-01'").fetchone()[0],
            "counts_grain_rows": con.execute(
                "SELECT count(*) FROM (SELECT DISTINCT countryoforigin, companycategory, companystatus FROM b)"
            ).fetchone()[0],
            "sic_shapes": rows(
                con, "SELECT regexp_replace(regexp_replace(sic1,'[0-9]','9','g'),'[A-Za-z].*','A…'), count(*) FROM "
                "(SELECT siccode_sictext_1 AS sic1 FROM b) GROUP BY 1 ORDER BY 2 DESC LIMIT 8"
            ),
            "sic_non_code": rows(
                con, "SELECT siccode_sictext_1, count(*) FROM b WHERE NOT regexp_matches(coalesce(siccode_sictext_1,''),'^[0-9]{4,5} - ') "
                "GROUP BY 1 ORDER BY 2 DESC LIMIT 8"
            ),
            "address_country_top": rows(con, "SELECT regaddress_country, count(*) FROM b GROUP BY 1 ORDER BY 2 DESC LIMIT 15"),
        }
        annotate("notice", "companies_house probe: dates and codes", dates)
        print(json.dumps(dates, default=str), flush=True)
        con.execute("CREATE VIEW pl AS SELECT * FROM d WHERE upper(trim(countryoforigin)) = 'POLAND'")
        poland = {
            "rows": con.execute("SELECT count(*), count(DISTINCT companynumber) FROM pl").fetchone(),
            "category": rows(con, "SELECT companycategory, count(*) FROM pl GROUP BY 1 ORDER BY 2 DESC"),
            "status": rows(con, "SELECT companystatus, count(*) FROM pl GROUP BY 1 ORDER BY 2 DESC"),
            "number_shapes": rows(
                con, "SELECT regexp_replace(regexp_replace(companynumber,'[0-9]','9','g'),'9+','9'), count(*) FROM pl GROUP BY 1 ORDER BY 2 DESC"
            ),
            "address_country": rows(con, "SELECT regaddress_country, count(*) FROM pl GROUP BY 1 ORDER BY 2 DESC LIMIT 10"),
            "post_town_top": rows(con, "SELECT regaddress_posttown, count(*) FROM pl GROUP BY 1 ORDER BY 2 DESC LIMIT 10"),
            "sic_top": rows(con, "SELECT siccode_sictext_1, count(*) FROM pl GROUP BY 1 ORDER BY 2 DESC LIMIT 6"),
            "inc_min_max": con.execute("SELECT min(inc), max(inc), count(*) FILTER (WHERE inc IS NULL) FROM pl").fetchone(),
            "known": rows(
                con, "SELECT companynumber, companyname, companycategory, companystatus, incorporationdate FROM pl "
                f"WHERE regexp_matches(upper(companyname), '{KNOWN}') ORDER BY companyname LIMIT 25"
            ),
            "name_sample": rows(con, "SELECT companyname FROM pl ORDER BY companynumber LIMIT 25"),
            "polish_named_uk_origin": con.execute(
                "SELECT count(*) FROM b WHERE upper(trim(countryoforigin)) <> 'POLAND' AND upper(regaddress_country) = 'POLAND'"
            ).fetchone()[0],
        }
        annotate("notice", "companies_house probe: Poland origin", poland)
        print(json.dumps(poland, default=str), flush=True)
        annotate(
            "notice", "companies_house probe: resources",
            {"peak_rss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024, "csv_bytes": csv_path.stat().st_size},
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
