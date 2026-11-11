"""Read-only probe (deleted before merge): profile the Silver inputs of the Gold blocks for OECD, GLEIF,
MF white list, OpenData.org, GUGiK PRG and GUS TERYT; time PRG neighbour detection. No writes."""

from __future__ import annotations

import json
import time

import duckdb

from ingestion.common.r2 import req
from ingestion.common.report import annotate


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET memory_limit='10GB'; SET preserve_insertion_order=false;")
    for ext in ("httpfs", "iceberg", "spatial"):
        con.execute(f"INSTALL {ext}; LOAD {ext};")
    endpoint = req("STORAGE_ENDPOINT_URL").replace("https://", "").strip("/")
    con.execute(
        f"""CREATE SECRET r2_catalog (TYPE iceberg, TOKEN '{req("STORAGE_LAKEHOUSE_CATALOG_WRITE_TOKEN")}')"""
    )
    con.execute(
        f"""CREATE SECRET r2_files (TYPE s3, KEY_ID '{req("STORAGE_WRITE_ACCESS_KEY")}',
        SECRET '{req("STORAGE_WRITE_SECRET_KEY")}', ENDPOINT '{endpoint}', REGION 'auto', URL_STYLE 'path')"""
    )
    con.execute(
        f"""ATTACH '{req("STORAGE_LAKEHOUSE_CATALOG_NAME")}' AS r2 (TYPE iceberg, SECRET r2_catalog,
        ENDPOINT '{req("STORAGE_LAKEHOUSE_CATALOG_URL")}')"""
    )
    return con


def show(con, title: str, query: str, limit: int = 60) -> None:
    started = time.monotonic()
    try:
        cur = con.execute(query)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchmany(limit)
        out = {"s": round(time.monotonic() - started, 1), "cols": cols, "rows": [[str(v)[:60] for v in r] for r in rows]}
        level = "notice"
    except Exception as exc:  # report and continue
        out, level = {"error": f"{type(exc).__name__}: {str(exc)[:600]}"}, "warning"
    text = json.dumps(out, ensure_ascii=False)
    for i in range(0, len(text), 3500):
        annotate(level, f"{title} [{i // 3500 + 1}]", text[i : i + 3500])


def main() -> int:
    con = connect()
    for t in (
        "silver.oecd_cli", "silver.gleif_legal_entities", "silver.gleif_relationships",
        "silver.biala_lista_taxpayer_hashes", "silver.biala_lista_masks", "silver.opendata_organizations",
        "silver.opendata_locations", "silver.prg_units", "silver.teryt_terc", "silver.teryt_simc",
    ):
        show(con, f"rows {t}", f"select count(*) from r2.{t}")
    show(con, "oecd codes", """select measure, unit_measure, adjustment, transformation, methodology, freq,
        count(distinct ref_area) areas, count(*) n, min(time_period), max(time_period),
        bool_or(ref_area = 'POL') pol, list(distinct obs_status) st
        from r2.silver.oecd_cli group by all order by measure, freq""")
    show(con, "oecd areas", "select string_agg(distinct ref_area, ',' order by ref_area) from r2.silver.oecd_cli")
    show(con, "oecd LI areas", "select string_agg(distinct ref_area, ',' order by ref_area) from r2.silver.oecd_cli where measure = 'LI'")
    show(con, "oecd POL", """select measure, unit_measure, adjustment, transformation, freq, count(*), min(time_period),
        max(time_period), arg_max(obs_value, time_period) from r2.silver.oecd_cli where ref_area = 'POL' group by all""")
    show(con, "gleif status", """select entity_status, registration_status, entity_category, count(*) from
        r2.silver.gleif_legal_entities group by all order by 4 desc""")
    show(con, "gleif versions", """select source_version, count(*), count(distinct lei), min(registered_at), max(registration_updated_at)
        from r2.silver.gleif_legal_entities group by all""")
    show(con, "gleif jurisdiction top", """select legal_jurisdiction, count(*) from r2.silver.gleif_legal_entities
        group by all order by 2 desc""", 40)
    show(con, "gleif jurisdiction shapes", """select length(legal_jurisdiction) l, count(*), count(distinct legal_jurisdiction),
        any_value(legal_jurisdiction) from r2.silver.gleif_legal_entities group by all""")
    show(con, "gleif PL regions", """select legal_jurisdiction, legal_address_country, legal_address_region, count(*)
        from r2.silver.gleif_legal_entities where legal_jurisdiction = 'PL' or legal_address_country = 'PL'
        group by all order by 4 desc""", 50)
    show(con, "gleif PL forms", """select legal_form_code, any_value(legal_form_other), entity_status, count(*) from
        r2.silver.gleif_legal_entities where legal_jurisdiction = 'PL' group by legal_form_code, entity_status order by 4 desc""", 40)
    show(con, "gleif form count", """select count(distinct legal_form_code), count(distinct legal_jurisdiction),
        count(distinct (legal_jurisdiction, legal_form_code, entity_status, registration_status)) from r2.silver.gleif_legal_entities""")
    show(con, "gleif rr", """select relationship_type, relationship_status, count(*) from r2.silver.gleif_relationships group by all""")
    show(con, "biala lista", """select taxpayer_status, file_date, source_version, count(*), count(distinct hash_sha512)
        from r2.silver.biala_lista_taxpayer_hashes group by all""")
    show(con, "biala masks", "select file_date, source_version, count(*) from r2.silver.biala_lista_masks group by all")
    show(con, "opendata countries", """select address_country, count(*) from r2.silver.opendata_organizations
        group by all order by 2 desc""", 30)
    show(con, "opendata meta", """select data_source, record_type, source_version, count(*), count(distinct entity)
        from r2.silver.opendata_organizations group by all order by 4 desc""", 20)
    show(con, "opendata PL", """select address_state, count(*) from r2.silver.opendata_organizations
        where upper(address_country) in ('PL', 'POL', 'POLAND') group by all order by 2 desc""", 30)
    show(con, "opendata loc countries", """select address_country, count(*) from r2.silver.opendata_locations
        group by all order by 2 desc""", 15)
    show(con, "teryt levels", """select unit_level, gmina_type_code, count(*), any_value(unit_type_pl), min(catalogue_date)
        from r2.silver.teryt_terc group by all order by 1, 2""")
    show(con, "prg levels", """select unit_level, layer, geometry_type, count(*), count(distinct teryt_code),
        min(length(teryt_code)), max(length(teryt_code)), any_value(teryt_code), min(source_version), sum(area_ha)
        from r2.silver.prg_units group by all order by 1""")
    show(con, "prg bounds", """select unit_level, min(ST_XMin(g)), max(ST_XMax(g)), min(ST_YMin(g)), max(ST_YMax(g))
        from (select unit_level, ST_GeomFromWKB(geometry_wkb) g from r2.silver.prg_units
        where unit_level in ('kraj','wojewodztwo')) group by 1""")
    con.execute("""create table u as select unit_level, teryt_code, area_ha, ST_GeomFromWKB(geometry_wkb) g
        from r2.silver.prg_units where unit_level in ('wojewodztwo','powiat','gmina')""")
    show(con, "prg vs terc", """select u.unit_level, count(*) n, count(t.terc_code) matched,
        list(u.teryt_code) filter (where t.terc_code is null)[1:10] unmatched
        from u left join r2.silver.teryt_terc t on t.terc_code = u.teryt_code group by 1""")
    show(con, "terc vs prg", """select t.unit_level, t.gmina_type_code, count(*) n, count(u.teryt_code) in_prg
        from r2.silver.teryt_terc t left join u on t.terc_code = u.teryt_code group by all order by 1, 2""")
    for level in ("wojewodztwo", "powiat", "gmina"):
        show(con, f"neighbours {level}", f"""
            with p as (select teryt_code, g from u where unit_level = '{level}'),
            pairs as (
                select a.teryt_code a, b.teryt_code b,
                    ST_Length(ST_Transform(ST_Intersection(a.g, b.g), 'EPSG:4258', 'EPSG:2180', always_xy := true)) / 1000 km
                from p a join p b on a.teryt_code < b.teryt_code and ST_Intersects(a.g, b.g))
            select count(*) pairs, count(*) filter (where km > 0) rook, count(*) filter (where km = 0) point_only,
                min(km) filter (where km > 0), max(km), (select count(*) from p) units,
                (select count(*) from p where teryt_code not in (select a from pairs union select b from pairs)) isolated
            from pairs""")
    show(con, "centroid sample", """select teryt_code, ST_X(ST_Centroid(g)), ST_Y(ST_Centroid(g)),
        area_ha / 100, ST_Area(ST_Transform(g, 'EPSG:4258', 'EPSG:2180', always_xy := true)) / 1e6
        from u where unit_level = 'wojewodztwo' order by 1""")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
