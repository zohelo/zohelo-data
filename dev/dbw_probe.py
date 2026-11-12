"""Read-only probe for the GUS DBW per-block Gold build stage: block ids against the inventory, the
dictionaries (periods, signs, dimensions, territorial positions) and a small data sample. Reads Iceberg
metadata, one dictionary file per block and about 100 small Silver files; writes nothing to R2.
Deleted before merge.  python -m dev.dbw_probe
"""

from __future__ import annotations

from collections import Counter, defaultdict
import random
import re

import duckdb

from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog, req
from ingestion.common.report import annotate

WOJ = [
    "DOLNOŚLĄSKIE", "KUJAWSKO-POMORSKIE", "LUBELSKIE", "LUBUSKIE", "ŁÓDZKIE", "MAŁOPOLSKIE", "MAZOWIECKIE",
    "OPOLSKIE", "PODKARPACKIE", "PODLASKIE", "POMORSKIE", "ŚLĄSKIE", "ŚWIĘTOKRZYSKIE", "WARMIŃSKO-MAZURSKIE",
    "WIELKOPOLSKIE", "ZACHODNIOPOMORSKIE",
]
STEM = re.compile(r"(_[0-9]{4})?_pl$")


def connect():
    con = duckdb.connect()
    con.execute("SET memory_limit = '8GB'; SET preserve_insertion_order = false; SET threads = 8")
    con.execute("INSTALL httpfs; LOAD httpfs")
    endpoint = req("STORAGE_ENDPOINT_URL").replace("https://", "").rstrip("/")
    con.execute(
        "CREATE SECRET r2 (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION 'auto', URL_STYLE 'path')",
        [req("STORAGE_WRITE_ACCESS_KEY"), req("STORAGE_WRITE_SECRET_KEY"), endpoint],
    )
    return con


def files(paths):
    return "read_parquet([" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "])"


def short(text, n=60):
    text = str(text)
    return text if len(text) <= n else text[: n - 1] + "…"


def main() -> None:
    cat, con = catalog(), connect()
    silver = et.state(cat.load_table(("silver", "dbw_observations")), "entity_id")
    dicts = et.state(cat.load_table(("bronze", "dbw_dictionary_items")), "entity_id")
    silver.pop(None, None)
    dicts.pop(None, None)
    stems = defaultdict(list)
    for e in silver:
        stems[STEM.sub("", e)].append(e)
    parts = Counter(len(e.split("_")) - 1 for e in silver)
    yearly = sum(1 for e in silver if re.search(r"_[0-9]{4}_pl$", e))
    stem_parts = Counter(len(s.split("_")) for s in stems)
    last = Counter(s.rsplit("_", 1)[-1] for s in stems)
    first4 = {"_".join(e.split("_")[:4]) for e in silver}
    five = sorted(s for s in stems if len(s.split("_")) == 5)
    # coverage: what the inventory counted
    cov = cat.load_table(("gold", "coverage")).scan(
        selected_fields=("source", "block_id", "status", "silver_rows")
    ).to_arrow()
    con.register("cov_all", cov)
    con.execute("create table cov as select * from cov_all where source = 'gus_dbw'")
    cov_rows = con.execute("select block_id, status, silver_rows from cov").fetchall()
    cov_stems = defaultdict(list)
    for block_id, status, rows in cov_rows:
        cov_stems[STEM.sub("", block_id)].append((block_id, status, rows))
    cov_status = Counter(status for _b, status, _r in cov_rows)
    only_silver = sorted(set(stems) - set(cov_stems))
    only_cov = sorted(set(cov_stems) - set(stems))
    cov_first4 = {"_".join(b.split("_")[:4]) for b, _s, _r in cov_rows}
    annotate("notice", "dbw probe 1 ids", {
        "silver_entities": len(silver), "silver_rows": sum(v["rows"] for v in silver.values()),
        "silver_mb": round(sum(v["bytes"] for v in silver.values()) / 1e6),
        "entities_with_several_files": sum(1 for v in silver.values() if len(v["paths"]) > 1),
        "dictionary_entities": len(dicts), "silver_without_dictionary": len(set(silver) - set(dicts)),
        "entity_parts": dict(parts), "entities_with_year": yearly, "stems": len(stems), "stem_parts": dict(stem_parts),
        "stem_last_part": dict(last.most_common(12)), "first4_distinct": len(first4),
        "five_part_stems": len(five), "five_part_sample": five[:6],
        "five_part_fifth": dict(Counter(s.split("_")[4] for s in five).most_common(8)),
        "files_per_stem_max": max(len(v) for v in stems.values()),
        "coverage_rows": len(cov_rows), "coverage_stems": len(cov_stems), "coverage_first4": len(cov_first4),
        "coverage_status": dict(cov_status),
        "coverage_stems_loaded": sum(1 for v in cov_stems.values() if any(s == "loaded" for _b, s, _r in v)),
        "coverage_stems_with_silver_rows": sum(1 for v in cov_stems.values() if any(r for _b, _s, r in v)),
        "stems_only_in_silver": len(only_silver), "only_silver_sample": only_silver[:8],
        "only_silver_last_part": dict(Counter(s.rsplit("_", 1)[-1] for s in only_silver).most_common(6)),
        "stems_only_in_coverage": len(only_cov), "only_coverage_sample": only_cov[:6],
        "only_coverage_status": dict(Counter(s for k in only_cov for _b, s, _r in cov_stems[k])),
        "silver_bytes_by_block_quantiles": [
            sorted(sum(silver[e]["bytes"] for e in v) for v in stems.values())[int(q * (len(stems) - 1))]
            for q in (0, 0.25, 0.5, 0.75, 0.9, 0.99, 1)
        ],
        "silver_rows_by_block_quantiles": [
            sorted(sum(silver[e]["rows"] for e in v) for v in stems.values())[int(q * (len(stems) - 1))]
            for q in (0, 0.25, 0.5, 0.75, 0.9, 0.99, 1)
        ],
    })

    # one dictionary per block: its newest file
    newest = {s: max(v) for s, v in stems.items() if max(v) in dicts}
    dict_paths = [p for e in newest.values() for p in dicts[e]["paths"]]
    con.execute(
        f"create table d as select entity_id, lower(nazwa_kolumny) as col, nazwa_slownika as dic, id_elementu as id, "
        f"opis from {files(dict_paths)}"
    )
    one = lambda q: con.execute(q).fetchall()  # noqa: E731
    cols = one(
        "select regexp_replace(col, '_[0-9]$', '_k') as c, dic, count(distinct entity_id) as f, count(*) as n "
        "from d group by 1, 2 order by 3 desc limit 40"
    )
    signs = {}
    for col in ("id_brak_wartosci", "id_tajnosci", "id_flaga"):
        signs[col] = [
            [i, short(o, 70), f]
            for i, o, f in one(
                f"select id, opis, count(distinct entity_id) from d where col = '{col}' group by 1, 2 order by 3 desc limit 30"
            )
        ]
    annotate("notice", "dbw probe 2 dictionary columns and signs", {
        "dictionary_files_read": len(dict_paths), "rows": one("select count(*) from d")[0][0],
        "columns": [[c, short(dc, 30), f, n] for c, dc, f, n in cols], **signs,
    })
    periods = one(
        "select id, opis, count(distinct entity_id), "
        "list(distinct regexp_extract(entity_id, '_([0-9]+)(_[0-9]{4})?_pl$', 1)) "
        "from d where col = 'id_okres' group by 1, 2 order by 3 desc"
    )
    packed = [f"{i}|{o}|{f}|{','.join(sorted(p))}" for i, o, f, p in periods]
    for n, start in enumerate(range(0, min(len(packed), 150), 50)):
        annotate("notice", f"dbw probe 3{'abc'[n]} periods", {"total": len(packed), "rows": packed[start : start + 50]})
    dates = one(
        "select id, opis, count(distinct entity_id) from d where col = 'id_daty' "
        "and not (regexp_matches(id, '^[0-9]{4}$') and opis = id) group by 1, 2 order by 3 desc limit 25"
    )
    pres = one("select count(distinct id), count(distinct opis) from d where col = 'id_sposob_prezentacji_miara'")[0]
    pres_top = one(
        "select id, opis, count(distinct entity_id) from d where col = 'id_sposob_prezentacji_miara' "
        "group by 1, 2 order by 3 desc limit 25"
    )
    multi = one(
        "select col, count(*) from (select entity_id, col, count(distinct id) as n from d "
        "where col like 'id_wymiar_%' or col in ('id_zmienna', 'id_przekroj', 'id_sposob_prezentacji_miara') "
        "group by 1, 2 having count(distinct id) > 1) group by 1 order by 2 desc"
    )
    id_labels = one(
        "select count(*) filter (where n > 1), count(*) from (select id, count(distinct upper(trim(opis))) as n "
        "from d where col like 'id_pozycja_%' group by 1)"
    )[0]
    annotate("notice", "dbw probe 4 dates, presentations, ids", {
        "id_daty_not_plain_year": [[i, short(o, 50), f] for i, o, f in dates],
        "id_daty_distinct": one("select count(distinct id) from d where col = 'id_daty'")[0][0],
        "presentation_ids": pres[0], "presentation_names": pres[1],
        "presentation_top": [[i, short(o, 45), f] for i, o, f in pres_top],
        "files_with_several_ids": multi,
        "position_ids_with_several_labels": id_labels[0], "position_ids": id_labels[1],
        "zmienna_sample": one("select id, opis from d where col = 'id_zmienna' limit 3"),
        "przekroj_sample": one("select id, opis from d where col = 'id_przekroj' limit 3"),
    })
    # territorial dimensions: positions that look like Polish areas
    woj = ", ".join("'" + w + "'" for w in WOJ)
    con.execute(
        f"""create table pos as
        select p.entity_id, cast(substr(p.col, 13) as integer) as k, w.id as dim_id, w.opis as dim_name, p.id as pos_id,
            upper(trim(p.opis)) as label,
            case
                when upper(trim(p.opis)) = 'POLSKA' then 'PL'
                when upper(trim(p.opis)) in ({woj}) then 'WOJ'
                when regexp_matches(upper(trim(p.opis)), '^MAKROREGION ') then 'MAKRO'
                when regexp_matches(upper(trim(p.opis)), '^REGION ') then 'REGION'
                when regexp_matches(upper(trim(p.opis)), '^PODREGION ') then 'PODREGION'
                when regexp_matches(upper(trim(p.opis)), '^POWIAT ') then 'POWIAT'
                when regexp_matches(upper(trim(p.opis)), '^(GMINA |M\\. |M\\.ST\\. |MIASTO )') then 'GMINA'
            end as class
        from d as p
        left join d as w on p.entity_id = w.entity_id and w.col = 'id_wymiar_' || substr(p.col, 13)
        where p.col like 'id_pozycja_%'"""
    )
    con.execute(
        "create table terr as select entity_id, k from pos group by 1, 2 "
        "having count(*) filter (where class in ('PL', 'WOJ', 'MAKRO', 'REGION', 'PODREGION', 'POWIAT')) > 0"
    )
    dims = one(
        """select p.dim_id, any_value(p.dim_name), count(distinct p.entity_id), count(distinct p.pos_id),
            count(distinct p.pos_id) filter (where p.class is not null),
            list(distinct p.label) filter (where p.class is null)[1:3]
        from pos as p join terr as t using (entity_id, k) group by 1 order by 3 desc limit 45"""
    )
    per_block = one(
        "select n, count(*) from (select s.entity_id, count(t.k) as n from (select distinct entity_id from d) as s "
        "left join terr as t using (entity_id) group by 1) group by 1 order by 1"
    )
    annotate("notice", "dbw probe 5 territorial dimensions", {
        "territorial_dimensions_per_block": per_block,
        "dimension_ids_with_area_positions": one("select count(distinct dim_id) from pos join terr using (entity_id, k)")[0][0],
        "dims": [[i, short(n, 40), f, p, a, [short(x, 25) for x in (o or [])]] for i, n, f, p, a, o in dims],
    })
    classes = one(
        """select coalesce(class, 'other'), count(distinct label), count(distinct pos_id), count(distinct entity_id),
            (list(distinct label order by label))[1:6]
        from pos join terr using (entity_id, k) group by 1 order by 2 desc"""
    )
    geo = cat.load_table(("gold", "map_geography")).scan(
        selected_fields=("source_system", "source_code", "geography_key", "matched")
    ).to_arrow()
    con.register("geo_all", geo)
    con.execute("create table geo as select * from geo_all where source_system = 'gus_dbw'")
    not_matched = one(
        """select class, label, count(distinct entity_id) from pos join terr using (entity_id, k)
        left join geo as g on g.source_code = label and g.matched
        where class in ('PL', 'WOJ', 'MAKRO', 'REGION', 'PODREGION') and g.source_code is null
        group by 1, 2 order by 3 desc limit 40"""
    )
    same_name = one(
        """select count(*) filter (where n > 1), count(*) from (select entity_id, k, label, count(distinct pos_id) as n
        from pos join terr using (entity_id, k) group by 1, 2, 3)"""
    )[0]
    other_top = one(
        """select label, count(distinct entity_id) from pos join terr using (entity_id, k)
        where class is null group by 1 order by 2 desc limit 40"""
    )
    annotate("notice", "dbw probe 6 territorial labels", {
        "classes": [[c, n, p, f, [short(x, 40) for x in s]] for c, n, p, f, s in classes],
        "map_rows": one("select count(*), count(*) filter (where matched) from geo")[0],
        "map_keys": one("select regexp_extract(geography_key, '^[A-Z_]+:?[A-Z]*', 0), count(*) from geo group by 1 order by 2 desc"),
        "nuts_labels_not_in_map": [[c, short(x, 50), f] for c, x, f in not_matched],
        "same_label_several_positions": same_name,
        "other_labels_top": [[short(x, 30), f] for x, f in other_top],
    })
    podregion = one(
        "select list(distinct label order by label) from pos join terr using (entity_id, k) "
        "where class in ('PODREGION', 'MAKRO', 'REGION')"
    )[0][0]
    annotate("notice", "dbw probe 7 nuts labels", {"labels": [short(x, 44) for x in podregion][:110]})

    # data sample: small Silver files of every periodicity, and the curated twin and powiat blocks
    rng = random.Random(7)
    by_last = defaultdict(list)
    for e in silver:
        if e in dicts and silver[e]["bytes"] < 3_000_000:
            by_last[STEM.sub("", e).rsplit("_", 1)[-1]].append(e)
    sample = []
    for _last_part, entities in sorted(by_last.items()):
        rng.shuffle(entities)
        sample += entities[:16]
    period_rows, missing, conf, flag, year_282 = defaultdict(Counter), Counter(), Counter(), Counter(), Counter()
    varying = Counter()
    for e in sample:
        labels = {
            (c, i): o
            for c, i, o in con.execute(
                f"select lower(nazwa_kolumny), id_elementu, opis from {files(dicts[e]['paths'])} "
                "where lower(nazwa_kolumny) in ('id_okres', 'id_brak_wartosci', 'id_tajnosci', 'id_flaga')"
            ).fetchall()
        }
        rows = con.execute(
            f"""select period_id, missing_reason_id, confidentiality_id, flag_id, count(*), count(value),
                count(*) filter (where value = 0), min(year), max(year),
                count(distinct indicator_id) + count(distinct section_id) + count(distinct dimension_1_id)
                    + count(distinct dimension_2_id) + count(distinct dimension_3_id)
            from {files(silver[e]['paths'])} group by 1, 2, 3, 4"""
        ).fetchall()
        periodicity = STEM.sub("", e).rsplit("_", 1)[-1]
        for period, miss, cf, fl, n, nv, nz, y0, y1, ids in rows:
            name = lambda c, i: f"{i}:{short(labels.get((c, str(i)), '?'), 46)}"  # noqa: E731
            period_rows[periodicity][name("id_okres", period)] += nv
            missing[(name("id_brak_wartosci", miss), "n")] += n
            missing[(name("id_brak_wartosci", miss), "valued")] += nv
            missing[(name("id_brak_wartosci", miss), "zero")] += nz
            conf[(name("id_tajnosci", cf), "n")] += n
            conf[(name("id_tajnosci", cf), "valued")] += nv
            conf[(name("id_tajnosci", cf), "zero")] += nz
            flag[(name("id_flaga", fl), "valued")] += nv
            if periodicity == "282":
                year_282[f"{y0}-{y1}"] += 1
            varying[ids > 5] += 1

    def table(counter):
        out = defaultdict(dict)
        for (name, kind), n in counter.items():
            out[name][kind] = n
        return dict(sorted(out.items(), key=lambda kv: -sum(kv[1].values()))[:14])

    annotate("notice", "dbw probe 8 data sample", {
        "files": len(sample),
        "periods_by_periodicity": {p: dict(c.most_common(9)) for p, c in period_rows.items()},
        "years_282": dict(year_282.most_common(5)),
        "groups_with_varying_ids": dict(varying),
    })
    annotate("notice", "dbw probe 9 signs in data", {
        "missing": table(missing), "confidentiality": table(conf), "flag": table(flag),
    })


if __name__ == "__main__":
    main()
