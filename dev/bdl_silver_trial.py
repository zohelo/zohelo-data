"""Trial of BDL Silver in zz_dbt_trial: build, measure the variable match and unit codes,
then drop the trial tables."""
import os
import time
import traceback

import duckdb

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import build


def connect():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")
    endpoint = os.environ["R2_S3_ENDPOINT"].replace("https://", "").strip("/")
    con.execute(f"""CREATE SECRET r2_catalog (TYPE iceberg, TOKEN '{os.environ["R2_DATA_CATALOG_TOKEN"]}')""")
    con.execute(f"""CREATE SECRET r2_files (TYPE s3, KEY_ID '{os.environ["CLOUDFLARE_R2_ACCESS_KEY_ID"]}',
                    SECRET '{os.environ["CLOUDFLARE_R2_SECRET_ACCESS_KEY"]}', ENDPOINT '{endpoint}',
                    REGION 'auto', URL_STYLE 'path')""")
    con.execute(f"""ATTACH '{os.environ["R2_WAREHOUSE"]}' AS r2 (TYPE iceberg, SECRET r2_catalog,
                    ENDPOINT '{os.environ["R2_CATALOG_URI"]}')""")
    return con


out = {}
try:
    started = time.time()
    out["exit"] = build.main("bdl_variables bdl_units bdl_observations", target="trial")
    out["seconds"] = round(time.time() - started)
    con = connect()
    t = "r2.zz_dbt_trial.bdl_observations"
    out["rows"], out["matched"], out["subgroups"], out["value_null"] = con.execute(
        f"select count(*), count(variable_id), count(distinct subgroup_id), count(*) filter (where value is null) from {t}").fetchone()
    out["unit_code_lengths"] = con.execute(f"select length(unit_code), count(*) from {t} group by 1 order by 2 desc limit 6").fetchall()
    out["unmatched_by_subgroup"] = con.execute(
        f"select subgroup_id, count(*) from {t} where variable_id is null group by 1 order by 2 desc limit 8").fetchall()
    out["unmatched_sample"] = [list(map(str, r)) for r in con.execute(
        f"select subgroup_id, dimension_values from {t} where variable_id is null limit 3").fetchall()]
    out["units"] = con.execute("select count(*), count(distinct level) from r2.zz_dbt_trial.bdl_units").fetchone()
    out["unit_in_api"] = con.execute(
        f"select count(*) filter (where u.unit_id is not null), count(*) from (select distinct unit_code from {t}) o "
        "left join r2.zz_dbt_trial.bdl_units u on u.unit_id = o.unit_code").fetchone()
    out["values_unparsed"] = [list(map(str, r)) for r in con.execute(
        f"select value_text, count(*) from {t} where value is null and trim(value_text) <> '' group by 1 order by 2 desc limit 6").fetchall()]
except Exception:
    out["error"] = traceback.format_exc()[-1500:]
cat = catalog()
for name in ("bdl_observations", "bdl_variables", "bdl_units"):
    try:
        cat.drop_table(("zz_dbt_trial", name))
    except Exception as exc:
        out[f"drop_{name}"] = str(exc)[:120]
annotate("notice", "bdl silver trial", out)
