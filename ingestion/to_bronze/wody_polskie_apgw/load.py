#!/usr/bin/env python3
"""Wody Polskie aPGW bronze.load: newest Landing copy of each plan-cycle geodatabase → Bronze attributes table.

A Landing file (ingestion/to_landing/wody_polskie_apgw/) is a ZIP holding one Esri File Geodatabase folder
(``Geobaza_2aPGW_ver_20230915.gdb``, 59 layers on 27 Nov 2026, Dev probe 45921328581). DuckDB spatial (GDAL's
OpenFileGDB driver, ``/vsizip/`` on the ZIP) lists the layers and their fields (``ST_Read_Meta``) and reads the
attributes of the LOADED layers (``ST_Read``); the geometry is never stored (it stays in Landing and the Archive):
the planning layers publish their area as an attribute (``POW_JCWP``, ``POWIERZCHN``, ``AREA``, km²), so nothing is
computed from it.

One Bronze row per feature x field of a loaded layer, values as published text (numbers as DuckDB prints the stored
double or integer, NULL kept as NULL so a feature without values still has its rows): layer, the feature's position in
the layer scan (``feature_number``, which ties the fields of one feature together), field name as published, value,
the geodatabase folder name (``gdb_name``, which carries the edition date) and provenance.

LOADED are the layers the Silver entity tables read: the surface water bodies (JCWP) of the lake, transitional,
coastal and reservoir layers (area) and their characterisation and status tables, the groundwater bodies (JCWPd) and
their characterisation, the river basin districts, water regions and RZGW areas. LISTED are the other layers of the
geodatabase, named and counted in the run's annotation but not loaded (measures, plans, protected areas, catchments,
relation tables; the river JCWP lines, whose attributes Charakterystyka_JCWP also holds). Rejected
(bronze.wody_polskie_apgw_rejected_files): a ZIP without exactly one ``.gdb`` folder, a layer that is neither LOADED nor
LISTED, a LOADED layer missing or with fields other than declared (missing or unknown), or an empty LOADED layer.
"""

from __future__ import annotations

from pathlib import Path
import tempfile
from typing import Any
import unicodedata
import zipfile

import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "wody_polskie_apgw"
TABLE = "wody_polskie_apgw_attributes"
REJECTED = "wody_polskie_apgw_rejected_files"
_CENTRES = ["CENTR_X_ETRS89", "CENTR_Y_ETRS89", "CENTR_X_PUWG1992", "CENTR_Y_PUWG1992"]
# Layer → its fields as published (Dev probe 45921328581: ST_Read_Meta of Geobaza_2aPGW_ver_20230915.gdb).
LOADED = {
    "Jednolite_Części_Wód_Powierzchniowych_Jeziornych": [
        "MS_KOD", "EU_KOD", "Nazwa_JCWP", *_CENTRES, "POW_JCWP", "Shape_Length", "Shape_Area", "METADATA",
    ],
    "Jednolite_Części_Wód_Powierzchniowych_Przejściowych": [
        "MS_KOD", "EU_KOD", "POW_JCWP", "Nazwa_JCWP", *_CENTRES, "Shape_Length", "Shape_Area", "METADATA",
    ],
    "Jednolite_Części_Wód_Powierzchniowych_Przybrzeżnych": [
        "MS_KOD", "EU_KOD", "POW_JCWP", "SHAPE_Length", "SHAPE_Area", "Nazwa_JCWP", *_CENTRES, "METADATA",
    ],
    "Jednolite_Części_Wód_Powierzchniowych_Zbiornikowych": [
        "MS_KOD", "EU_KOD", "POW_JCWP", "Nazwa_JCWP", *_CENTRES, "Shape_Length", "Shape_Area", "METADATA",
    ],
    "Charakterystyka_JCWP": [
        "kod_jcwp", "nazwa_jcwp", "jcwp_zmian", "ms_kod_apg", "typ_jcwp", "dlug_pow", "pow_zl", "obsz_dorz",
        "reg_wodny", "rzgw", "rdos", "wojewodz", "powiat", "gmina", "kod_jcwpd", "kod_ppk_s", "kod_ppk_n", "st_eko",
        "st_chem", "ocena_stan", "oc_ryz", "zl", "rk", "och_inspire", "oc_gat_gos", "bud_reg", "z_dzialan",
        "z_dz_drozn", "METADATA",
    ],
    "Status_JCWP": ["kod_jcwp", "status_os", "stat_os_uz", "stat_uz_1", "stat_uz_2", "stat_uz_3", "METADATA"],
    "Jednolite_części_wód_podziemnych": [
        "NR_JCWPD", "KOD_UE", "POWIERZCHN", "Shape_Length", "Shape_Area", "METADATA", "kod_jcwpd",
    ],
    "Charakterystyka_JCWPd": [
        "kod_jcwpd", "nr_jcwpd", "pow_jcwpd", "obsz_dorz", "reg_wodny", "rzgw", "obsz_bilan", "zl", "z_dzialan",
        "METADATA",
    ],
    "Obszary_dorzeczy": [
        "Shape_Length", "Shape_Area", "RBD_NAME", "RBD_NAME_NL", "RBD_EU_CD", "RBD_MS_CD", "CENTR_X_ETRS89",
        "CENTR_Y_ETRS89", "AREA", "INTERNATIONAL", "INTER_NAME", "COMPETENT_PRIME", "COMPETENT_OTHER", "METADATA",
        "URL", "CENTR_X_PUWG1992", "CENTR_Y_PUWG1992",
    ],
    "Regiony_wodne": [
        "RB_NAME", "Shape_Length", "Shape_Area", "RB_EU_CD", "RB_MS_CD", "CENTR_X_ETRS89", "CENTR_Y_ETRS89", "AREA",
        "RBD_MS_CD", "COMPETENT_PRIME", "METADATA", "URL", "CENTR_X_PUWG1992", "CENTR_Y_PUWG1992", "COMPETENT_OTHER",
    ],
    "Regionalne_Zarządy_Gospodarki_Wodnej": [
        "RZGW_NAME", "RZGW_EU_CD", "RZGW_MS_CD", "CENTR_X_ETRS89", "CENTR_Y_ETRS89", "AREA", "METADATA", "URL",
        "CENTR_X_PUWG1992", "CENTR_Y_PUWG1992", "GB_MS_CD", "O_dorzeczy", "Shape_Length", "Shape_Area",
    ],
}  # fmt: skip
_WITH_ACTIONS = "Wykaz_inwestycji_i_działań"
LISTED = {
    "Jednolite_Części_Wód_Powierzchniowych_Rzecznych",
    "Zlewnie_JCWP_rzecznych",
    "Zlewnie_bezpośrednie_JCWP_zbiornikowych",
    "Pozostale_obszary_JCWP",
    "Ekoregiony",
    "Zlewnie_bezpośrednie_JCWP_jeziornych",
    "Zlewnie_JCWP_przejściowych",
    "Zlewnie_JCWP_przybrzeżnych",
    "T2aPGW_relacja_aPGW",
    "Katalog_działań_krajowych_JCWP",
    "Katalog_Dzialan_Krajowych_JCWPd",
    "Wykaz_JCWP_wskazanych_do_odstępstw",
    "Wykaz_JCWPd_wskazanych_do_odstępstw",
    _WITH_ACTIONS,
    "Zestawy_działań_JCWP",
    "Zestawy_działań_JCWPd",
    "Wykaz_działań_dla_budowli",
    "Cele_środowiskowe_JCWPd",
    "Cele_środowiskowe_JCWP",
    "Presje_ryzyko_odstepstwa_JCWP",
    "Ocena_stanu_JCWP",
    "Ocena_stanu_JCWPd",
    "Presje_ryzyko_odstepstwa_JCWPd",
    "Obszary_chronione_wodozależne_pominiki_przyrody",
    "Obszary_chronione_wodozależne",
    "Obszary_chronione_JCWP_TW_troć",
    "Obszary_chronione_cieki_troć",
    "Obszary_chronione_jeziora_zbiorniki_troć",
    "Obszary_chronione_cieki_węgorz",
    "Obszary_chronione_jeziora_zbiorniki_węgorz",
    "Obszary_chronione_JCWP_TW_węgorz",
    "Relacja_widz_jcwpd",
    "Relacja_widz_jcwp_cw",
    "Relacja_widz_jcwp_lw",
    "Relacja_widz_jcwp_rw",
    "Relacja_widz_jcwp_rwr",
    "Relacja_widz_jcwp_tw",
    "Tabela_słownikowa_JCWP",
    "Rzeki_glowne",
    "Wartości_graniczne_SCW_SZCW_RWR",
    "Wartości_graniczne_SCW_SZCW_LW",
    "Wartości_graniczne_SCW_SZCW_RW",
    *(
        f"Jednolite_Części_Wód_Powierzchniowych_{kind}_{_WITH_ACTIONS}"
        for kind in ("Jeziornych", "Przejściowych", "Przybrzeżnych", "Rzecznych", "Zbiornikowych")
    ),
    f"Jednolite_części_wód_podziemnych_{_WITH_ACTIONS}",
}
COLUMNS = [
    ("entity", "VARCHAR"),
    ("gdb_name", "VARCHAR"),
    ("layer", "VARCHAR"),
    ("feature_number", "BIGINT"),
    ("field", "VARCHAR"),
    ("value", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)
LAST_LAYERS: dict[str, int] = {}  # layer → feature count of the last decoded file (for the run's annotation)


def nfc(text: str) -> str:
    return unicodedata.normalize("NFC", text)


def gdb_folder(names: list[str]) -> str:
    """The one ``*.gdb`` folder at the top of the ZIP."""
    folders = {n.split("/")[0] for n in names if n.split("/")[0].lower().endswith(".gdb")}
    others = {n.split("/")[0] for n in names} - folders
    if len(folders) != 1 or others:
        raise ValueError(f"expected one .gdb folder in the ZIP, found {sorted(folders)} and {sorted(others)[:5]}")
    return folders.pop()


def check_layout(layers: dict[str, list[str]]) -> None:
    """Raise ValueError when the geodatabase is not the declared layout (ingestion/to_bronze docstring)."""
    unknown = sorted(set(layers) - set(LOADED) - LISTED)
    if unknown:
        raise ValueError(f"unknown layers {unknown}")
    missing = sorted(set(LOADED) - set(layers))
    if missing:
        raise ValueError(f"declared layers missing {missing}")
    for name, fields in LOADED.items():
        found = layers[name]
        if sorted(found) != sorted(fields):
            raise ValueError(
                f"{name}: fields differ from the declared layout: missing {sorted(set(fields) - set(found))}, "
                f"unknown {sorted(set(found) - set(fields))}"
            )


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    import duckdb

    with tempfile.TemporaryDirectory(prefix="zohelo-apgw-") as temporary:
        path = Path(temporary) / "apgw.zip"
        path.write_bytes(body)
        try:
            names = zipfile.ZipFile(path).namelist()
        except zipfile.BadZipFile as exc:
            raise ValueError(f"not a ZIP: {exc}") from exc
        folder = gdb_folder(names)
        gdb = f"/vsizip/{path}/{folder}"
        con = duckdb.connect()
        con.execute("SET memory_limit = '6GB'")
        con.execute("INSTALL spatial; LOAD spatial;")
        try:
            found = con.execute("SELECT layers FROM ST_Read_Meta(?)", [gdb]).fetchall()
            meta = found[0] if len(found) == 1 else None
        except duckdb.Error as exc:
            raise ValueError(f"{folder}: not readable as a File Geodatabase: {str(exc)[:300]}") from exc
        if not meta or not meta[0]:
            raise ValueError(f"{folder}: no layers")
        layers = {nfc(layer["name"]): [nfc(f["name"]) for f in layer.get("fields") or []] for layer in meta[0]}
        raw_names = {nfc(layer["name"]): layer["name"] for layer in meta[0]}
        check_layout(layers)
        LAST_LAYERS.clear()
        LAST_LAYERS.update({nfc(x["name"]): int(x.get("feature_count") or 0) for x in meta[0]})
        parts = []
        for name, fields in LOADED.items():
            raw = {nfc(f): f for f in layers[name]}
            select = ", ".join(
                'CAST("' + raw[f].replace('"', '""') + f'" AS VARCHAR) AS f{i}' for i, f in enumerate(fields)
            )
            try:
                con.execute(
                    f"CREATE OR REPLACE TEMP TABLE t AS SELECT row_number() OVER () AS n, {select} "
                    "FROM ST_Read(?, layer = ?)",
                    [gdb, raw_names[name]],
                )
            except duckdb.Error as exc:
                raise ValueError(f"{name}: not readable: {str(exc)[:300]}") from exc
            rows = con.execute("SELECT count(*) FROM t").fetchall()[0][0]
            if rows == 0:
                raise ValueError(f"{name}: no features")
            LAST_LAYERS[name] = rows
            for i, f in enumerate(fields):
                parts.append(
                    con.sql(
                        f"SELECT ? AS layer, n AS feature_number, ? AS field, f{i} AS value FROM t ORDER BY n",
                        params=[name, f],
                    ).to_arrow_table()
                )
        con.close()
    table = pa.concat_tables(parts)
    base = bulk_bronze.provenance(receipt)
    n = table.num_rows
    columns = {
        "entity": pa.array([base["entity"]] * n, pa.string()),
        "gdb_name": pa.array([folder] * n, pa.string()),
        "layer": table.column("layer").cast(pa.string()),
        "feature_number": table.column("feature_number").cast(pa.int64()),
        "field": table.column("field").cast(pa.string()),
        "value": table.column("value").cast(pa.string()),
        "raw_sha256": pa.array([base["raw_sha256"]] * n, pa.string()),
        "source_version": pa.array([base["source_version"]] * n, pa.string()),
        "ingested_at": pa.array([base["ingested_at"]] * n, pa.timestamp("us", tz="UTC")),
        "source_row_number": pa.array(range(1, n + 1), pa.int64()),
    }
    return pa.Table.from_pydict(columns, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: one geodatabase per plan cycle, decoded in about a minute."""
    del budget_minutes
    from ingestion.common.report import annotate
    from ingestion.to_landing import wody_polskie_apgw as landing

    code = bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        COLUMNS,
        decode,
        "attributes",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": e} for e in sorted(landing.held(landing_root))],
    )
    if LAST_LAYERS:
        annotate(
            "notice",
            f"{SOURCE} Bronze layers",
            {
                "loaded": {k: v for k, v in LAST_LAYERS.items() if k in LOADED},
                "listed_not_loaded": {k: v for k, v in sorted(LAST_LAYERS.items()) if k not in LOADED},
            },
        )
    return code
