"""Gold PoC extraction stage: the label dictionaries of the activated DBW blocks' files
(bronze.dbw_dictionary_items, one file per bulk file), planned with pyiceberg and streamed by
DuckDB — a dbt SQL read of that table opens all 11,296 files. Keep BLOCKS in step with
macros/poc_blocks.sql (poc_dbw_blocks) and poc_src_dbw.py."""

BLOCKS = (
    "581_57_93_4",
    "581_16_2_1",
    # zohelo-bi first slice, every source per KPI (O31, #2592)
    "305_739_5_1",
    "305_1722_5_1",
    "305_1721_5_3",
    "376_2_243_1",
    "376_2_184_1",
    "376_16_182_1",
    "398_16_243_3",
    "398_16_184_3",
    "398_16_182_3",
    "875_143_95_1",
    "1514_1360_95_3",
    "1513_1360_95_3",
    "1512_1360_95_3",
    "1038_16_389_3",
    "1038_16_390_3",
    "1038_16_92_4",
    "1038_16_105_4",
    "1173_793_92_4",
    "814_2_7_1",
    "392_16_223_1",
    "109_849_7_1",
    "469_16_117_1",
)


def planned_files(table_name, row_filter):
    """Data files of the Iceberg table that may hold rows matching the filter (metadata only)."""
    import os

    from pyiceberg.catalog.rest import RestCatalog

    cat = RestCatalog(
        name="zohelo",
        warehouse=os.environ["STORAGE_LAKEHOUSE_CATALOG_NAME"],
        uri=os.environ["STORAGE_LAKEHOUSE_CATALOG_URL"],
        token=os.environ["STORAGE_LAKEHOUSE_CATALOG_WRITE_TOKEN"],
    )
    scan = cat.load_table(table_name).scan(row_filter=row_filter)
    tasks = list(scan.plan_files())
    if any(task.delete_files for task in tasks):  # the files are read directly: deletes would be ignored
        raise RuntimeError(f"{table_name}: delete files present; read through the catalog instead")
    return [task.file.file_path for task in tasks]


def model(dbt, con):
    import os

    dbt.config(materialized="table", alias="src_dbw_dictionary")
    dbt.source("gus_dbw", "dbw_dictionary_items")  # declares the input (changed-only build)
    prefix = " or ".join(f"starts_with(entity_id, '{b}_')" for b in BLOCKS)
    columns = "entity_id, nazwa_kolumny, nazwa_slownika, id_elementu, opis"
    if "STORAGE_LAKEHOUSE_CATALOG_URL" not in os.environ:  # local harness: the fake source table
        return con.query(f"select {columns} from r2.bronze.dbw_dictionary_items where {prefix}")
    from pyiceberg.expressions import Or, StartsWith

    filters = [StartsWith("entity_id", f"{b}_") for b in BLOCKS]
    files = planned_files(("bronze", "dbw_dictionary_items"), Or(*filters) if len(filters) > 1 else filters[0])
    if not files:
        return con.query(f"select {columns} from r2.bronze.dbw_dictionary_items where false")
    paths = ", ".join("'" + p.replace("'", "''") + "'" for p in files)
    return con.query(f"select {columns} from read_parquet([{paths}]) where {prefix}")
