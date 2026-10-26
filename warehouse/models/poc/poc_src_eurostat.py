"""Gold PoC extraction stage: the activated Eurostat datasets' rows, read from only their
Silver files. silver.eurostat_observations holds one file per dataset (design #875); the
Iceberg metadata names those files, so the planner (pyiceberg) picks the activated ones and
DuckDB streams just them. The dbt-duckdb SQL path scanned every file (trial 1 and 2).
In v1 this is the per-block builder. Without a catalog (local harness) it reads the source table.

Keep the dataset list in step with macros/poc_blocks.sql (poc_eurostat_blocks)."""

DATASETS = {
    "DEMO_PJAN": "A",
    "DEMO_R_PJANAGGR3": "A",
    "DEMO_GIND": "A",
    "NAMA_10_GDP": "A",
    "NAMA_10_PC": "A",
    "UNE_RT_A": "A",
    "UNE_RT_M": "M",
    "PRC_HICP_MANR": "M",
    "LFSI_EMP_A": "A",
}
COLUMNS = (
    "dataset_id, freq_code, unit_code, geo_code, time_period, value, status_code, "
    "breakdown_dimensions, breakdown_codes, raw_sha256, source_version"
)


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
    freq_rule = " or ".join(f"(upper(dataset_id) = '{d}' and freq_code = '{f}')" for d, f in DATASETS.items())
    where = f"({freq_rule}) and value is not null"
    if "R2_CATALOG_URI" not in os.environ:  # local harness: the fake source table
        return con.query(f"select {COLUMNS} from r2.silver.eurostat_observations where {where}")
    from pyiceberg.expressions import In

    ids = [*DATASETS, *(d.lower() for d in DATASETS)]  # the id as Eurostat spells it, whichever case
    files = planned_files(("silver", "eurostat_observations"), In("dataset_id", ids))
    if not files:
        return con.query(f"select {COLUMNS} from r2.silver.eurostat_observations where false")
    paths = ", ".join(f"'{p}'" for p in files)
    return con.query(f"select {COLUMNS} from read_parquet([{paths}]) where {where}")
