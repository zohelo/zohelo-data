"""Gold PoC extraction stage: the activated GUS DBW blocks' rows (one Silver file per bulk
file, partitioned by entity_id), planned with pyiceberg and streamed by DuckDB; see
poc_src_eurostat.py. Keep the block list in step with macros/poc_blocks.sql (poc_dbw_blocks).
REGIONAL blocks (poc_dbw_block_rules: regional) hold every powiat or gmina; only their rows for Poland and the
voivodeships (territory in position 1, named in the block's own dictionary) are read, so a block of 0.8M rows
stays at the size the facts need."""

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
    # slice needs-block rows (O36, #2743)
    "1603_1339_116_4",
    "1618_1335_95_4",
    "1714_1575_243_1",
    "309_485_180_3",
    "507_870_93_1",
)
REGIONAL = ("1714_1575_243_1", "309_485_180_3", "507_870_93_1")
AREAS = (  # Poland and the 16 voivodeships as the DBW dictionaries name them (upper case)
    "POLSKA",
    "DOLNOŚLĄSKIE",
    "KUJAWSKO-POMORSKIE",
    "LUBELSKIE",
    "LUBUSKIE",
    "ŁÓDZKIE",
    "MAŁOPOLSKIE",
    "MAZOWIECKIE",
    "OPOLSKIE",
    "PODKARPACKIE",
    "PODLASKIE",
    "POMORSKIE",
    "ŚLĄSKIE",
    "ŚWIĘTOKRZYSKIE",
    "WARMIŃSKO-MAZURSKIE",
    "WIELKOPOLSKIE",
    "ZACHODNIOPOMORSKIE",
)


def regional_rule(dictionary_sql):
    """SQL condition keeping, in REGIONAL blocks, only rows whose position 1 is Poland or a voivodeship."""
    areas = ", ".join(f"'{a}'" for a in AREAS)
    blocks = " or ".join(f"starts_with(entity_id, '{b}_')" for b in REGIONAL)
    allowed = (
        f"select entity_id as e, try_cast(id_elementu as bigint) as pos from ({dictionary_sql}) "
        f"where lower(nazwa_kolumny) = 'id_pozycja_1' and upper(trim(opis)) in ({areas})"
    )
    return f"(not ({blocks}) or (entity_id, position_1_id) in (select e, pos from ({allowed})))"


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

    dbt.config(materialized="table", alias="src_dbw")
    dbt.source("silver_entities", "dbw_observations")  # declares the inputs (changed-only build)
    dbt.source("gus_dbw", "dbw_dictionary_items")  # the territory names of the REGIONAL blocks
    prefix = " or ".join(f"starts_with(entity_id, '{b}_')" for b in BLOCKS)
    where = f"({prefix}) and value is not null"
    if "STORAGE_LAKEHOUSE_CATALOG_URL" not in os.environ:  # local harness: the fake source tables
        rule = regional_rule("select * from r2.bronze.dbw_dictionary_items")
        return con.query(f"select * from r2.silver.dbw_observations where {where} and {rule}")
    from pyiceberg.expressions import Or, StartsWith

    regional = [StartsWith("entity_id", f"{b}_") for b in REGIONAL]
    dict_files = planned_files(("bronze", "dbw_dictionary_items"), Or(*regional) if len(regional) > 1 else regional[0])
    dict_paths = ", ".join("'" + p.replace("'", "''") + "'" for p in dict_files)
    if not dict_files:  # without the dictionaries the REGIONAL blocks would silently lose every row
        raise RuntimeError("bronze.dbw_dictionary_items has no files for the REGIONAL blocks " + ", ".join(REGIONAL))
    rule = regional_rule(f"select * from read_parquet([{dict_paths}])")
    filters = [StartsWith("entity_id", f"{b}_") for b in BLOCKS]
    files = planned_files(("silver", "dbw_observations"), Or(*filters) if len(filters) > 1 else filters[0])
    if not files:
        return con.query("select * from r2.silver.dbw_observations where false")
    paths = ", ".join("'" + p.replace("'", "''") + "'" for p in files)
    return con.query(f"select * from read_parquet([{paths}]) where {where} and {rule}")
