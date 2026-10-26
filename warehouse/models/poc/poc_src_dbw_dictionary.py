"""Gold PoC extraction stage: the label dictionaries of the activated DBW blocks' files
(bronze.dbw_dictionary_items, one file per bulk file), planned with pyiceberg and streamed by
DuckDB — a dbt SQL read of that table opens all 11,296 files. Keep BLOCKS in step with
macros/poc_blocks.sql (poc_dbw_blocks) and poc_src_dbw.py."""

BLOCKS = ("581_57_93_4", "581_16_2_1")


def planned_files(table_name, row_filter):
    """Data files of the Iceberg table that may hold rows matching the filter (metadata only)."""
    import os

    from pyiceberg.catalog.rest import RestCatalog

    cat = RestCatalog(
        name="zohelo",
        warehouse=os.environ["R2_WAREHOUSE"],
        uri=os.environ["R2_CATALOG_URI"],
        token=os.environ["R2_DATA_CATALOG_TOKEN"],
    )
    scan = cat.load_table(table_name).scan(row_filter=row_filter)
    return [task.file.file_path for task in scan.plan_files()]


def model(dbt, con):
    import os

    dbt.config(materialized="table")
    prefix = " or ".join(f"starts_with(entity_id, '{b}_')" for b in BLOCKS)
    columns = "entity_id, nazwa_kolumny, nazwa_slownika, id_elementu, opis"
    if "R2_CATALOG_URI" not in os.environ:  # local harness: the fake source table
        return con.query(f"select {columns} from r2.bronze.dbw_dictionary_items where {prefix}")
    from pyiceberg.expressions import Or, StartsWith

    filters = [StartsWith("entity_id", f"{b}_") for b in BLOCKS]
    files = planned_files(("bronze", "dbw_dictionary_items"), Or(*filters) if len(filters) > 1 else filters[0])
    if not files:
        return con.query(f"select {columns} from r2.bronze.dbw_dictionary_items where false")
    paths = ", ".join(f"'{p}'" for p in files)
    return con.query(f"select {columns} from read_parquet([{paths}]) where {prefix}")
