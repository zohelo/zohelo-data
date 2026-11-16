"""Read-only probe 3 for PL-ENV-033 (GUNB RWDZ): the 16 voivodeship files parsed as ';' CSV with DuckDB.

Probe 2 (run 43345366257) showed the separator is ';' (not '#'), a UTF-8 BOM, LF line ends, double quotes, and a
31-column header with 'cecha' and 'ulica' repeated. This probe reads every voivodeship file once (one request at a
time, pauses, user agent), with explicit column names, and prints only: header names, parse result, row counts,
key uniqueness, which columns differ inside repeated keys (counts only), and profiles of code/date/authority columns.
Values of columns that may name persons or addresses are never printed.
"""

import re
import tempfile
import time
import urllib.request
import zipfile
from pathlib import Path

import duckdb

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
B = "https://wyszukiwarka.gunb.gov.pl/pliki_pobranie/wynik_{}.zip"
FILES = ["opolskie", "dolnoslaskie", "kujawsko-pomorskie", "lodzkie", "lubelskie", "lubuskie", "malopolskie",
         "mazowieckie", "podkarpackie", "podlaskie", "pomorskie", "slaskie", "swietokrzyskie", "warminsko-mazurskie",
         "wielkopolskie", "zachodniopomorskie"]
HEADER = ("numer_urzad;numer_gunb;nazwa_organu;adres_organu;data_wplywu_wniosku;numer_decyzji_urzedu;"
          "data_wydania_decyzji;nazwa_inwestor;wojewodztwo;miasto;terc;cecha;cecha;ulica;ulica_dalej;nr_domu;"
          "rodzaj_inwestycji;kategoria;nazwa_zamierzenia_bud;nazwa_zam_budowlanego;kubatura;projektant_nazwisko;"
          "projektant_imie;projektant_numer_uprawnien;jednosta_numer_ew;obreb_numer;numer_dzialki;"
          "numer_arkusza_dzialki;jednostka_stara_numeracja_z_wniosku;stara_numeracja_obreb_z_wnioskiu;"
          "stara_numeracja_dzialka_z_wniosku").split(";")
NAMES = []
for h in HEADER:
    n, i = h, 2
    while n in NAMES:
        n, i = f"{h}_{i}", i + 1
    NAMES.append(n)
KEEP = ["numer_urzad", "numer_gunb", "nazwa_organu", "data_wplywu_wniosku", "numer_decyzji_urzedu",
        "data_wydania_decyzji", "wojewodztwo", "terc", "rodzaj_inwestycji", "kategoria", "kubatura"]
PLOT = ["jednosta_numer_ew", "obreb_numer", "numer_dzialki", "numer_arkusza_dzialki",
        "jednostka_stara_numeracja_z_wniosku", "stara_numeracja_obreb_z_wnioskiu", "stara_numeracja_dzialka_z_wniosku"]
PRINT_VALUES = ["wojewodztwo", "rodzaj_inwestycji", "kategoria"]
SHAPE = ["numer_gunb", "numer_urzad", "data_wplywu_wniosku", "data_wydania_decyzji", "terc", "kubatura",
         "numer_decyzji_urzedu"]


def shape_sql(c):
    return f"regexp_replace(regexp_replace(left(\"{c}\", 30), '[0-9]', '9', 'g'), '[^0-9 ./;:_-]', 'a', 'g')"


def fetch(url, path):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=600) as r, path.open("wb") as f:
        while b := r.read(1 << 20):
            f.write(b)


def main():
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    cols = "{" + ", ".join(f"'{n}': 'VARCHAR'" for n in NAMES) + "}"
    totals, summary = {}, {}
    for i, name in enumerate(FILES):
        time.sleep(3)
        with tempfile.TemporaryDirectory() as tmp:
            z = Path(tmp) / "f.zip"
            fetch(B.format(name), z)
            with zipfile.ZipFile(z) as zf:
                m = zf.infolist()
                zf.extract(m[0], tmp)
                csv = Path(tmp) / m[0].filename
            first = csv.open("rb").readline()
            header = first.decode("utf-8-sig").rstrip("\r\n").split(";")
            res = {"members": [x.filename for x in m], "csv_bytes": csv.stat().st_size, "header_ok": header == HEADER}
            if header != HEADER:
                res["header"] = header
            try:
                con.execute("DROP TABLE IF EXISTS t")
                con.execute(
                    f"CREATE TABLE t AS SELECT * FROM read_csv('{csv}', delim=';', quote='\"', escape='\"', header=false, "
                    f"skip=1, columns={cols}, strict_mode=true, null_padding=false, encoding='utf-8', parallel=false)"
                )
            except Exception as e:  # noqa: BLE001
                msg = str(e).split("\n")[0][:200]
                res["parse_error"] = re.sub(r"(?i)original line.*", "", msg)
                # retry permissive to count bad lines
                try:
                    con.execute("DROP TABLE IF EXISTS t")
                    con.execute(
                        f"CREATE TABLE t AS SELECT * FROM read_csv('{csv}', delim=';', quote='\"', escape='\"', header=false, "
                        f"skip=1, columns={cols}, strict_mode=false, ignore_errors=true, store_rejects=true, encoding='utf-8', parallel=false)"
                    )
                    res["rejects"] = con.execute("SELECT count(*), any_value(error_type) FROM reject_errors").fetchone()
                except Exception as e2:  # noqa: BLE001
                    res["parse_error2"] = str(e2).split("\n")[0][:200]
                    print("PROBE3", name, res, flush=True)
                    summary[name] = res
                    continue
            lines = sum(1 for _ in csv.open("rb")) - 1
            q = lambda s: con.execute(s).fetchall()  # noqa: E731
            res["rows"] = q("SELECT count(*) FROM t")[0][0]
            res["lf_lines"] = lines
            for k in ("numer_gunb", "numer_urzad"):
                res[k] = q(f"SELECT count(DISTINCT \"{k}\"), count(*) FILTER (WHERE \"{k}\" IS NULL) FROM t")[0]
            res["full_dupe_rows"] = q(f"SELECT count(*) - (SELECT count(*) FROM (SELECT DISTINCT * FROM t)) FROM t")[0][0]
            # repeated numer_gunb: per kept column, groups with more than one distinct value
            diff = ", ".join(f"count(*) FILTER (WHERE d_{j} > 1)" for j in range(len(KEEP)))
            inner = ", ".join(f"count(DISTINCT coalesce(\"{c}\", '~')) AS d_{j}" for j, c in enumerate(KEEP))
            pd = ", ".join(f"coalesce(\"{c}\", '~')" for c in PLOT)
            r = q(f"SELECT count(*), {diff}, count(*) FILTER (WHERE p > 1) FROM (SELECT {inner}, "
                  f"count(DISTINCT concat_ws('|', {pd})) AS p FROM t GROUP BY numer_gunb HAVING count(*) > 1)")[0]
            res["dup_key_groups"] = r[0]
            res["dup_groups_differing"] = {c: r[1 + j] for j, c in enumerate(KEEP) if r[1 + j]}
            res["dup_groups_differing_plots"] = r[-1]
            res["max_rows_per_key"] = q("SELECT max(c) FROM (SELECT count(*) c FROM t GROUP BY numer_gunb)")[0][0]
            r2 = q("SELECT count(*) FROM (SELECT numer_urzad, nazwa_organu FROM t GROUP BY 1,2 HAVING count(DISTINCT numer_gunb) > 1)")
            res["urzad_organ_with_several_gunb"] = r2[0][0]
            res["years_wniosek"] = q("SELECT left(data_wplywu_wniosku, 4) y, count(*) FROM t GROUP BY y ORDER BY y")
            res["years_decyzja"] = q("SELECT left(data_wydania_decyzji, 4) y, count(*) FROM t GROUP BY y ORDER BY y")
            if i < 3:
                for c in SHAPE:
                    res[f"shape_{c}"] = q(f"SELECT {shape_sql(c)} s, count(*) n FROM t GROUP BY s ORDER BY n DESC LIMIT 8")
                for c in PRINT_VALUES:
                    res[f"values_{c}"] = q(f"SELECT \"{c}\", count(*) n FROM t GROUP BY 1 ORDER BY n DESC LIMIT 40")
                res["terc_len"] = q("SELECT length(terc), count(*) FROM t GROUP BY 1 ORDER BY 2 DESC")
                res["organs"] = q("SELECT count(DISTINCT nazwa_organu) FROM t")[0][0]
                res["terc_woj_vs_file"] = q("SELECT left(terc, 2), count(*) FROM t GROUP BY 1 ORDER BY 2 DESC LIMIT 5")
            for c in PRINT_VALUES:
                for v, n in q(f"SELECT \"{c}\", count(*) FROM t GROUP BY 1"):
                    totals.setdefault(c, {}).setdefault(str(v), 0)
                    totals[c][str(v)] += n
            print("PROBE3", name, res, flush=True)
            summary[name] = [res["rows"], res["numer_gunb"], res["dup_key_groups"], res["full_dupe_rows"],
                             res.get("parse_error", "")[:60]]
    annotate("notice", "probe3 rows, (distinct gunb, null), dup key groups, full dupes", summary)
    for c, vals in totals.items():
        top = sorted(vals.items(), key=lambda kv: -kv[1])
        print("PROBE3 ALL", c, len(top), top[:80], flush=True)


if __name__ == "__main__":
    main()
