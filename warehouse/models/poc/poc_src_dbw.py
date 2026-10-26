"""Gold PoC extraction stage: the activated GUS DBW blocks' rows (one Silver file per bulk
file, partitioned by entity_id), planned with pyiceberg and streamed by DuckDB; see
poc_src_eurostat.py. Keep the block list in step with macros/poc_blocks.sql (poc_dbw_blocks)."""

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
    tasks = list(scan.plan_files())
    if any(task.delete_files for task in tasks):  # the files are read directly: deletes would be ignored
        raise RuntimeError(f"{table_name}: delete files present; read through the catalog instead")
    return [task.file.file_path for task in tasks]


def model(dbt, con):
    import os

    dbt.config(materialized="table")
    dbt.source("silver_entities", "dbw_observations")  # declares the input (changed-only build)
    prefix = " or ".join(f"starts_with(entity_id, '{b}_')" for b in BLOCKS)
    where = f"({prefix}) and value is not null"
    if "R2_CATALOG_URI" not in os.environ:  # local harness: the fake source table
        return con.query(f"select * from r2.silver.dbw_observations where {where}")
    from pyiceberg.expressions import Or, StartsWith

    filters = [StartsWith("entity_id", f"{b}_") for b in BLOCKS]
    files = planned_files(("silver", "dbw_observations"), Or(*filters) if len(filters) > 1 else filters[0])
    if not files:
        return con.query("select * from r2.silver.dbw_observations where false")
    paths = ", ".join("'" + p.replace("'", "''") + "'" for p in files)
    return con.query(f"select * from read_parquet([{paths}]) where {where}")
