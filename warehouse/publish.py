"""gold.publish: the sole writer of the published layer ``06_published/v1/`` (decision D5).

Reads Gold tables from the R2 Data Catalog and the KPI seeds in the repository and writes the
files described in docs/published.md (schemas in warehouse/published/schemas/): ``catalog/catalog.json``,
``catalog/<table>.parquet``, ``manifest.json``, ``kpi/<kpi_key>/latest.json`` / ``series.parquet``,
``feed/calendar.ics``, ``status.json`` and ``geo/country.geojson`` / ``geo/voivodeship.geojson``; and the topics
ranking (O32): ``06_published/v2/topics.json`` once warehouse.yaml ``topics.publish`` is true, until then only the
internal review copy ``06_published/v1/review/topics.json`` (data portal, after sign-in) and the run's job summary;
and contract v2 for zohelo-bi under ``06_published/v2/`` (warehouse/publish_v2.py) when warehouse.yaml
``published_v2`` is true.

Licence gate (O37, docs/published.md): no row of a source whose licence is unresolved or missing (source.csv
``licence_key`` → licence.csv) reaches any published file, v1 or v2.
"""

from __future__ import annotations

import csv
from datetime import UTC, date, datetime, timedelta
import io
import json
from pathlib import Path
import time
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.compute as pc

PROJECT = Path(__file__).resolve().parent
SCHEMAS = Path(__file__).resolve().parent / "published" / "schemas"
PREFIX = "06_published/v1/"
LEVELS = {"country": "country", "wojewodztwo": "voivodeship"}  # Gold geography_level → contract level
FREQ_RANK = {"D": 1, "B": 2, "W": 3, "M": 4, "Q": 5, "S": 6, "A": 7}
UNRESOLVED_LICENCE = "unresolved"  # licence.csv: terms not established (licence gate, O37)
# fx_rate is licence-blocked today (NBP unresolved, O37); the skip stays for when its terms are established, since
# kpi_latest would otherwise pick one arbitrary currency per source and geography.
SKIP = {"fx_rate": "one value per currency; latest.json has no currency field"}
# Sources that reach the published contract v2 only, never v1 (zohelo.com): v1 orders values by source_key, so a new
# source would change what zohelo.com shows first (review of #2758).
V2_ONLY_SOURCES = {"ecb"}
LATEST_LIMIT = 100_000  # bytes, docs/published.md size limits (D22)
TOPICS_KEY = "06_published/v2/topics.json"  # docs/published.md, "topics.json"
TOPICS_REVIEW_KEY = "06_published/v1/review/topics.json"  # internal review (docs/topics.md, "Internal review")
TOPICS_GZIP_LIMIT = 150_000  # bytes gzipped, docs/published.md size limits
TOPIC_PERIODS = ("day", "week", "month", "quarter", "year")
STATUS_LIMIT = 50_000  # bytes, docs/published.md size limits
SERIES_LIMIT = 20_000_000  # bytes, docs/published.md size limits
# Contract file catalog/<name>.parquet → Gold table (seeds kpi, kpi_measure, domain, unit, licence; models
# kpi_dimension, dim_source; freshness is not built yet).
CALENDAR_LINES = [
    "BEGIN:VCALENDAR",
    "VERSION:2.0",
    "PRODID:-//zohelo//gold.publish//EN",
    "CALSCALE:GREGORIAN",
    "X-WR-CALNAME:zohelo release calendar",
    "END:VCALENDAR",
]
CATALOG_TABLES = {
    "dim_kpi": "kpi",
    "kpi_measure": "kpi_measure",
    "kpi_dimension": "kpi_dimension",
    "dim_domain": "domain",
    "dim_unit": "unit",
    "dim_source": "dim_source",
    "dim_licence": "licence",
    "freshness": "freshness",
}
# silver.prg_units unit_level → geo/<level>.geojson level; simplification tolerance in degrees (about 350–550 m).
GEO = {"kraj": "country", "wojewodztwo": "voivodeship", "powiat": "powiat", "gmina": "gmina"}
GEO_TOLERANCE = 0.005
GEO_LIMITS = {  # bytes, docs/published.md size limits
    "country": 2_000_000,
    "voivodeship": 2_000_000,
    "powiat": 2_000_000,
    "gmina": 8_000_000,
}
# Gold geography_level → contract level of series.parquet (Polish NUTS 1 = makroregion, NUTS 2 = region).
SERIES_LEVELS = {
    "country": "country",
    "wojewodztwo": "voivodeship",
    "powiat": "powiat",
    "gmina": "gmina",
    "makroregion": "nuts1",
    "region": "nuts2",
    "nuts1": "nuts1",
    "nuts2": "nuts2",
}


def v1_rows(metrics: pa.Table) -> pa.Table:
    """The rows of gold.metrics_<kpi> that v1 publishes: every source except V2_ONLY_SOURCES; pure."""
    keep = pc.invert(pc.is_in(metrics.column("source_system"), value_set=pa.array(sorted(V2_ONLY_SOURCES))))
    return metrics.filter(keep)


# A concurrent silver_gold.build (another source's run; its concurrency group is not gold.publish's) swaps tables
# (dbt __dbt_tmp/__dbt_backup renames, seeds dropped and recreated), so a table can be missing for a few seconds:
# gold.news_outlet in prod run 43157796127 (#2793). A table not found is looked up again after these waits.
SWAP_RECHECK_SECONDS = (10, 20)


def gold(cat, namespace: str, name: str, waits: tuple[float, ...] = SWAP_RECHECK_SECONDS) -> pa.Table:
    """A catalog table as Arrow; NoSuchTableError only when it is still missing after the swap re-checks."""
    from pyiceberg.exceptions import NoSuchTableError

    for wait in (*waits, None):
        try:
            return cat.load_table((namespace, name)).scan().to_arrow()
        except NoSuchTableError:
            if wait is None:
                raise
            time.sleep(wait)
    raise AssertionError("unreachable")


def seed(name: str) -> list[dict[str, str]]:
    with (PROJECT / "seeds" / f"{name}.csv").open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def licensed_sources(sources: list[dict[str, str]], licences: list[dict[str, str]]) -> set[str]:
    """The source_keys whose rows may be published (licence gate, O37): the source's licence_key is a row of
    licence.csv and is not ``unresolved``; pure. A source missing from source.csv is never licensed."""
    known = {lic["licence_key"] for lic in licences} - {UNRESOLVED_LICENCE}
    return {s["source_key"] for s in sources if s.get("licence_key") in known}


def licence_gated(metrics: pa.Table, licensed: set[str], kpi_key: str = "") -> pa.Table:
    """The rows of a gold.metrics_<kpi_key> table whose source_system is licensed; pure. ValueError when a row has
    no source_system (a Gold defect, never counted as a licence removal)."""
    import pyarrow.compute as pc

    column = metrics.column("source_system")
    if column.null_count:
        raise ValueError(f"gold.metrics_{kpi_key}: {column.null_count} rows without source_system")
    return metrics.filter(pc.is_in(column, value_set=pa.array(sorted(licensed), pa.string())))


def licence_blocked(kpi_key: str, measures: list[dict[str, str]], licensed: set[str]) -> bool:
    """True when every source kpi_measure.csv maps for the KPI is unlicensed (decided by the seeds alone, never by a
    read): the KPI is out of the published layer and its files are removed; pure."""
    sources = {m["source_system"] for m in measures if m["kpi_key"] == kpi_key}
    return bool(sources) and not sources & licensed


def remove(client, key: str) -> None:
    """Delete one published object (a no-op when it does not exist)."""
    from ingestion.common.r2 import req

    client.delete_object(Bucket=req("STORAGE_LAKEHOUSE_BUCKET_NAME"), Key=key)


def iso(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def period_label(date_key: date, frequency: str) -> str:
    if frequency == "A":
        return f"{date_key.year}"
    if frequency == "S":
        return f"{date_key.year}-H{1 if date_key.month <= 6 else 2}"
    if frequency == "Q":
        return f"{date_key.year}-Q{(date_key.month - 1) // 3 + 1}"
    if frequency == "M":
        return f"{date_key.year}-{date_key.month:02d}"
    return date_key.strftime("%Y-%m-%d")


def put(client, key: str, body: bytes, content_type: str = "application/json") -> int:
    from ingestion.common.r2 import req

    client.put_object(
        Bucket=req("STORAGE_LAKEHOUSE_BUCKET_NAME"),
        Key=key,
        Body=body,
        ContentType=content_type,
        CacheControl="no-cache",
    )
    return len(body)


def compare_latest(latest: dict[str, Any], metrics: pa.Table) -> list[str]:
    """Values and previous values of latest that are not in gold.metrics_<kpi_key> for their source,
    geography and period; pure (no R2)."""
    gold_values: dict[tuple[str, str, str], set[float]] = {}
    for row in metrics.to_pylist():
        if row["geography_level"] not in LEVELS or row["value"] is None:
            continue
        key = (row["source_system"], row["geography_key"], period_label(row["date_key"], row["frequency"]))
        gold_values.setdefault(key, set()).add(float(row["value"]))
    problems = []
    for v in latest["values"]:
        pairs = [(v["period"], v["value"])]
        if "previous_period" in v:
            pairs.append((v["previous_period"], v.get("previous_value")))
        for period, value in pairs:
            if value not in gold_values.get((v["source_key"], v["geography_key"], period), set()):
                problems.append(
                    f"{latest['kpi_key']} {v['source_key']} {v['geography_key']} {period}: {value} not in Gold"
                )
    return problems


def get(client, key: str) -> bytes:
    from ingestion.common.r2 import req

    return client.get_object(Bucket=req("STORAGE_LAKEHOUSE_BUCKET_NAME"), Key=key)["Body"].read()


def kpi_latest(
    metrics: pa.Table,
    measures: list[dict],
    checked_at: dict[str, str],
    kpi_key: str,
    unit_key: str,
    generated_at: str,
) -> dict[str, Any]:
    """latest.json of one KPI from its gold.metrics_<kpi_key> rows; pure (no R2)."""
    measure_rows = pa.table(
        {
            "kpi_key": pa.array([m["kpi_key"] for m in measures], pa.string()),
            "source_system": pa.array([m["source_system"] for m in measures], pa.string()),
            "measure_key_pattern": pa.array([m["measure_key_pattern"] for m in measures], pa.string()),
            "priority": pa.array([int(m["priority"]) for m in measures], pa.int64()),
            "unit_key": pa.array([m.get("unit_key") or unit_key for m in measures], pa.string()),
        }
    )
    ranks = pa.table({"frequency": list(FREQ_RANK), "freq_rank": list(FREQ_RANK.values())})
    con = duckdb.connect()
    con.register("metrics", metrics)
    con.register("measures", measure_rows)
    con.register("ranks", ranks)
    rows = con.execute(
        """
        with kept as (
            select * from metrics
            where geography_level in (select unnest(?)) and value is not null and geography_key <> '_X'
        ),
        prioritised as (
            select k.*, coalesce((
                select min(m.priority) from measures m
                where m.kpi_key = ? and m.source_system = k.source_system
                    and k.measure_key like m.measure_key_pattern
            ), 999) as priority, coalesce((
                select arg_min(m.unit_key, m.priority) from measures m
                where m.kpi_key = ? and m.source_system = k.source_system
                    and k.measure_key like m.measure_key_pattern
            ), ?) as value_unit_key
            from kept k
        ),
        series as (
            select source_system, geography_key, measure_key, frequency,
                min(r.freq_rank) as freq_rank, min(priority) as priority
            from prioritised p join ranks r using (frequency)
            group by all
        ),
        chosen as (
            select * from series
            qualify row_number() over (
                partition by source_system, geography_key order by freq_rank, priority, measure_key
            ) = 1
        ),
        ranked as (
            select p.source_system, p.geography_key, p.geography_level, p.date_key, p.frequency,
                p.value, p.observation_status_key, p.value_unit_key,
                row_number() over (partition by p.source_system, p.geography_key order by p.date_key desc) as n
            from prioritised p join chosen c using (source_system, geography_key, measure_key, frequency)
        )
        select * from ranked where n <= 2
        order by source_system, geography_key, n
        """,
        [list(LEVELS), kpi_key, kpi_key, unit_key],
    ).fetchall()
    columns = [
        "source_system",
        "geography_key",
        "geography_level",
        "date_key",
        "frequency",
        "value",
        "status",
        "unit",
        "n",
    ]
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for row in (dict(zip(columns, r, strict=True)) for r in rows):
        key = (row["source_system"], row["geography_key"])
        if row["n"] == 1:
            latest[key] = {
                "source_key": row["source_system"],
                "geography_key": row["geography_key"],
                "geography_level": LEVELS[row["geography_level"]],
                "period": period_label(row["date_key"], row["frequency"]),
                "value": float(row["value"]),
                "unit_key": row["unit"],
                "flags": [] if row["status"] in (None, "ok") else [row["status"]],
                "checked_at": checked_at.get(row["source_system"], generated_at),
            }
        else:
            latest[key]["previous_period"] = period_label(row["date_key"], row["frequency"])
            latest[key]["previous_value"] = float(row["value"])
    level_order = {level: i for i, level in enumerate(LEVELS.values())}
    values = sorted(
        latest.values(), key=lambda v: (v["source_key"], level_order[v["geography_level"]], v["geography_key"])
    )
    return {"kpi_key": kpi_key, "unit_key": unit_key, "generated_at": generated_at, "values": values}


def parquet(table: pa.Table) -> bytes:
    import pyarrow.parquet as pq

    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="zstd")
    return buffer.getvalue()


def kpi_series(metrics: pa.Table, measures: list[dict], kpi_key: str, unit_key: str) -> pa.Table:
    """series.parquet of one KPI from its gold.metrics_<kpi_key> rows; pure (no R2).

    Each row's unit_key is its measure's unit from kpi_measure (as in kpi_latest), else the KPI's unit.
    """
    levels = pa.table({"gold_level": list(SERIES_LEVELS), "contract_level": list(SERIES_LEVELS.values())})
    own = [m for m in measures if m["kpi_key"] == kpi_key]
    measure_rows = pa.table(
        {
            "source_system": pa.array([m["source_system"] for m in own], pa.string()),
            "measure_key_pattern": pa.array([m["measure_key_pattern"] for m in own], pa.string()),
            "priority": pa.array([int(m["priority"]) for m in own], pa.int64()),
            "unit_key": pa.array([m.get("unit_key") or unit_key for m in own], pa.string()),
        }
    )
    con = duckdb.connect()
    con.register("metrics", metrics)
    con.register("levels", levels)
    con.register("measures", measure_rows)
    con.sql(
        """
        create temp table kept as
        select m.*, coalesce(l.contract_level, m.geography_level, '_Z') as series_level
        from metrics m left join levels l on m.geography_level = l.gold_level
        where m.value is not null and m.geography_key <> '_X'
            and (m.geography_key = '_Z' or l.contract_level is not null)
        """
    )
    keys = sorted(
        row[0]
        for row in con.sql(
            "select distinct unnest(json_keys(breakdown_json)) from kept where breakdown_json is not null"
        ).fetchall()
    )
    dims = []
    for key in keys:
        path = "$." + json.dumps(key)
        distinct = con.sql(
            "select count(distinct coalesce(json_extract_string(breakdown_json, ?), '_T')) from kept", params=[path]
        ).fetchall()[0][0]
        if distinct > 1:
            column = "dim_" + "".join(c if c in "abcdefghijklmnopqrstuvwxyz0123456789_" else "_" for c in key.lower())
            dims.append((path, column))
    dim_sql = "".join(
        f", coalesce(json_extract_string(breakdown_json, ${i + 3}), '_T') as {column}"
        for i, (_, column) in enumerate(dims)
    )
    return con.sql(
        f"""
        select $1 as kpi_key, source_system as source_key, measure_key, geography_key,
            series_level as geography_level,
            case frequency
                when 'A' then strftime(date_key, '%Y')
                when 'S' then strftime(date_key, '%Y') || '-H' || (case when month(date_key) <= 6 then 1 else 2 end)
                when 'Q' then strftime(date_key, '%Y') || '-Q' || quarter(date_key)
                when 'M' then strftime(date_key, '%Y-%m')
                else strftime(date_key, '%Y-%m-%d')
            end as period,
            cast(date_key as date) as period_start, frequency, cast(value as double) as value,
            coalesce((
                select arg_min(u.unit_key, u.priority) from measures u
                where u.source_system = kept.source_system and kept.measure_key like u.measure_key_pattern
            ), $2) as unit_key,
            case when observation_status_key is null or observation_status_key = 'ok' then ''
                else observation_status_key end as flags
            {dim_sql}
        from kept
        order by source_key, measure_key, geography_key, period_start
        """,
        params=[kpi_key, unit_key, *[path for path, _ in dims]],
    ).to_arrow_table()


def catalog_tables(cat, client, prefix: str, namespace: str) -> dict[str, Any]:
    """Write catalog/<name>.parquet for every CATALOG_TABLES entry whose Gold table exists."""
    from pyiceberg.exceptions import NoSuchTableError

    written: dict[str, int] = {}
    missing: list[str] = []
    for name, table_name in CATALOG_TABLES.items():
        try:
            table = gold(cat, namespace, table_name)
        except NoSuchTableError:
            missing.append(name)
            continue
        put(client, prefix + f"catalog/{name}.parquet", parquet(table), "application/vnd.apache.parquet")
        written[name] = table.num_rows
    return {"written": written, "missing": missing}


def kpi_entry(
    kpi: dict[str, str], series: pa.Table, domains: dict[str, dict], checked_at: dict[str, str], generated_at: str
) -> dict[str, Any]:
    """One catalog.json kpis[] entry from the KPI's seed row and its series.parquet table; pure (no R2)."""
    rows = series.select(["source_key", "geography_level", "period", "period_start", "frequency"]).to_pylist()
    default = [r for r in rows if r["frequency"] == kpi["frequency_default"]] or rows
    ordered = sorted(default, key=lambda r: r["period_start"])
    path: list[str] = []
    key: str | None = kpi["subgroup_key"]
    while key:
        path.insert(0, key)
        key = (domains.get(key) or {}).get("parent_key") or None
    sources = sorted({r["source_key"] for r in rows})
    dimensions = []
    for column in series.column_names:
        if column.startswith("dim_"):
            members = {v for v in series.column(column).to_pylist() if v not in (None, "_T")}
            dimensions.append({"dimension_key": column[4:], "member_count": len(members), "levels": []})
    stamps = [checked_at[s] for s in sources if s in checked_at]
    return {
        "kpi_key": kpi["kpi_key"],
        "name_pl": kpi["name_pl"],
        "name_en": kpi["name_en"],
        "unit_key": kpi["unit_key"],
        "frequency_default": kpi["frequency_default"],
        "subgroup_key": kpi["subgroup_key"],
        "domain_path": path,
        "source_keys": sources,
        "geo_levels": sorted({r["geography_level"] for r in rows if r["geography_level"] != "_Z"} or {"country"}),
        "first_period": ordered[0]["period"] if ordered else "",
        "last_period": ordered[-1]["period"] if ordered else "",
        "dimensions": dimensions,
        "loaded_at": max(stamps) if stamps else generated_at,
        "status": kpi["status"],
    }


def catalog_json(entries: list[dict[str, Any]], generated_at: str) -> dict[str, Any]:
    """catalog/catalog.json from the KPI entries and the seeds domain, unit, source, licence; pure (no R2)."""
    domains = [{**d, "parent_key": d["parent_key"] or None, "sort": int(d["sort"])} for d in seed("domain")]
    units = [{**u, "is_rate": u["is_rate"] == "true"} for u in seed("unit")]
    licences = [
        {**lic, "commercial_ok": lic["commercial_ok"] == "true", "share_alike": lic.get("share_alike") == "true"}
        for lic in seed("licence")
    ]
    return {
        "version": 1,
        "generated_at": generated_at,
        "kpis": entries,
        "domains": domains,
        "units": units,
        "sources": seed("source"),
        "licences": licences,
    }


def status_json(
    sources: list[dict[str, str]], checked_at: dict[str, str], next_at: dict[str, str], generated_at: str
) -> dict[str, Any]:
    """status.json: per source its last load, next expected run and lag in whole days (docs/published.md)."""
    generated = datetime.strptime(generated_at, "%Y-%m-%dT%H:%M:%SZ")
    entries = []
    for row in sources:
        key = row["source_key"]
        loaded_at = checked_at.get(key)
        lag = None
        if loaded_at is not None:
            lag = (generated - datetime.strptime(loaded_at, "%Y-%m-%dT%H:%M:%SZ")).days
        entries.append(
            {
                "source_key": key,
                "name_pl": row["name_pl"],
                "name_en": row["name_en"],
                "last_loaded_at": loaded_at,
                "next_expected_at": next_at.get(key),
                "lag_days": lag,
            }
        )
    return {"version": 1, "generated_at": generated_at, "sources": entries}


def ics_text(value: str) -> str:
    """RFC 5545 TEXT: a backslash before every backslash, semicolon and comma (backslashes first)."""
    return value.replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,")


def ics_time(stamp: str) -> str:
    """'2026-11-09T11:15:00Z' → '20261109T111500Z'."""
    return stamp.replace("-", "").replace(":", "")


def calendar_ics(rows: list[dict[str, str]] | None = None, stamp: str = "") -> str:
    """feed/calendar.ics: one VEVENT per release_calendar row (docs/published.md)."""
    events = []
    for row in sorted(rows or [], key=lambda r: (r["release_at"], r["source_key"], r["kpi_key"])):
        start = ics_time(row["release_at"])
        events += [
            "BEGIN:VEVENT",
            f"UID:{start}-{row['source_key']}-{row['kpi_key']}@zohelo.com",
            f"DTSTAMP:{ics_time(stamp)}",
            f"DTSTART:{start}",
            f"SUMMARY:{ics_text(row['title_pl'])}",
            f"URL:{row['url']}",
            "END:VEVENT",
        ]
    lines = CALENDAR_LINES[:-1] + events + CALENDAR_LINES[-1:]
    for line in lines:
        if len(line.encode("utf-8")) > 75:
            raise ValueError(f"calendar.ics line over 75 bytes: {line}")
    return "\r\n".join(lines) + "\r\n"


def calendar(client, prefix: str, stamp: str) -> int:
    """Write feed/calendar.ics under prefix; returns the number of events written."""
    rows = seed("release_calendar")
    put(
        client,
        prefix + "feed/calendar.ics",
        calendar_ics(rows, stamp).encode("utf-8"),
        content_type="text/calendar; charset=utf-8",
    )
    return len(rows)


def _round(coordinates):
    if isinstance(coordinates, (int, float)):
        return round(coordinates, 4)
    return [_round(c) for c in coordinates]


def features(shapes: list[dict], names: dict[str, dict], level: str) -> dict[str, Any]:
    """FeatureCollection of one level from PRG shapes and Gold names; pure (no R2)."""
    collection = []
    for shape in sorted(shapes, key=lambda s: s["geography_key"]):
        key = shape["geography_key"]
        if key not in names:
            raise ValueError(f"geo: {key} has no name in gold.dim_geography")
        name = names[key]
        properties = {
            "geography_key": key,
            "name_pl": name["name_pl"],
            "name_en": name["name_en"] or name["name_pl"],
            "level": level,
        }
        if level == "voivodeship":
            properties["parent_key"] = "PL"
        elif level != "country":
            if not name.get("parent_key"):
                raise ValueError(f"geo: {key} has no parent_key in gold.dim_geography")
            properties["parent_key"] = name["parent_key"]
        geometry = {"type": shape["geometry"]["type"], "coordinates": _round(shape["geometry"]["coordinates"])}
        collection.append({"type": "Feature", "properties": properties, "geometry": geometry})
    return {"type": "FeatureCollection", "features": collection}


def geo(cat, client, prefix: str) -> dict[str, int]:
    """Write geo/<level>.geojson (country, voivodeship, powiat, gmina) from silver.prg_units and gold.dim_geography."""
    from pyiceberg.expressions import In

    units = (
        cat.load_table(("silver", "prg_units"))
        .scan(row_filter=In("unit_level", list(GEO)), selected_fields=("unit_level", "teryt_code", "geometry_wkb"))
        .to_arrow()
    )
    names = {
        row["geography_key"]: row
        for row in cat.load_table(("gold", "dim_geography"))
        .scan(
            row_filter=In("level_name", ["country", "wojewodztwo", "powiat", "gmina"]),
            selected_fields=("geography_key", "name_pl", "name_en", "parent_key"),
        )
        .to_arrow()
        .to_pylist()
    }
    con = duckdb.connect()
    con.sql("INSTALL spatial; LOAD spatial;")
    con.register("units", units)
    rows = con.sql(
        """
        select unit_level,
            case when unit_level = 'kraj' then 'PL' else 'TERC:' || teryt_code end as geography_key,
            ST_AsGeoJSON(ST_SimplifyPreserveTopology(ST_GeomFromWKB(geometry_wkb), ?)) as geometry
        from units
        """,
        params=[GEO_TOLERANCE],
    ).fetchall()
    shapes: dict[str, list[dict]] = {level: [] for level in GEO.values()}
    seen: set[str] = set()
    for unit_level, key, geometry in rows:
        if key in seen:
            raise ValueError(f"geo: {key} occurs twice in silver.prg_units")
        seen.add(key)
        shapes[GEO[unit_level]].append({"geography_key": key, "geometry": json.loads(geometry)})
    counts = {level: len(items) for level, items in shapes.items()}
    expected = {"country": 1, "voivodeship": 16, "powiat": 380, "gmina": 2479}
    if counts != expected:
        raise ValueError(f"geo: expected {expected}, found {counts}")
    for level, items in shapes.items():
        body = json.dumps(features(items, names, level), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(body) > GEO_LIMITS[level]:
            raise ValueError(f"geo/{level}.geojson is {len(body)} bytes, over {GEO_LIMITS[level]}")
        put(client, prefix + f"geo/{level}.geojson", body, "application/geo+json")
    return counts


def checked(coverage: pa.Table) -> dict[str, str]:
    """Per source of gold.coverage, when it was last checked."""
    result: dict[str, str] = {}
    for row in coverage.select(["source", "checked_at"]).to_pylist():
        if row["checked_at"] is None:
            continue
        stamp = iso(row["checked_at"])
        result[row["source"]] = max(result.get(row["source"], stamp), stamp)
    return result


def schema_checked(obj: dict, schema: str) -> dict:
    """obj unchanged when it has every required key and JSON type of published/schemas/<schema>.schema.json;
    ValueError otherwise (docs/published.md, D27)."""
    from ops.repo_check import _schema_problems

    spec = json.loads((SCHEMAS / f"{schema}.schema.json").read_text(encoding="utf-8"))
    problems = _schema_problems(obj, spec, "$")
    if problems:
        raise ValueError(f"{schema}: {problems[:5]}")
    return obj


def manifest_json(client, prefix: str) -> int:
    """Write manifest.json under prefix after a fresh dbt parse; returns its number of metrics."""
    from warehouse import catalog, changed

    changed.manifest("prod")
    obj = schema_checked(catalog.manifest(), "manifest")
    body = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode()
    put(client, prefix + "manifest.json", body)
    return len(obj["metrics"])


def topics_json(con: duckdb.DuckDBPyConnection, cfg: dict[str, Any], generated_at: str) -> dict[str, Any]:
    """topics.json (docs/published.md) from the tables topic_rank, topic, topic_kpi, topic_matches, news_outlet,
    releases, release_calendar and kpi_v2_map of a DuckDB connection (Gold tables and seeds)."""
    as_of = con.execute("select coalesce(max(window_to), current_date) from topic_rank").fetchone()[0]
    kpis: dict[str, list[str]] = {}
    for topic_key, kpi_key in con.execute(
        "select topic_key, kpi_key from topic_kpi order by topic_key, role = 'main' desc, kpi_key"
    ).fetchall():
        kpis.setdefault(topic_key, []).append(kpi_key)
    releases = con.execute(
        """
        with announced as (
            select
                release_date as day, publisher as source_key, title as title_pl, title as title_en, trim(k) as kpi_key
            from releases, unnest(string_split(kpi_keys, ';')) as u (k)
            where kpi_keys != ''
            union all
            select cast(cast(r.release_at as timestamptz) as date), r.source_key, r.title_pl, r.title_en, m.v2_key
            from release_calendar as r inner join kpi_v2_map as m on r.kpi_key = m.concept_key
        )
        select t.topic_key, a.day, a.source_key, a.title_pl, a.title_en
        from announced as a inner join topic_kpi as t on a.kpi_key = t.kpi_key and t.role = 'main'
        where a.day >= ?
        qualify row_number() over (partition by t.topic_key order by a.day, a.source_key, a.title_pl) = 1
        """,
        [as_of],
    ).fetchall()
    next_release = {
        r[0]: {"date": r[1].isoformat(), "source_key": r[2], "title_pl": r[3], "title_en": r[4]} for r in releases
    }
    periods: dict[str, Any] = {}
    for period in TOPIC_PERIODS:
        days = int(cfg["periods"][period])
        window = {
            "from": (as_of - timedelta(days=days - 1)).isoformat(),
            "to": as_of.isoformat(),
        }
        ranked = con.execute(
            """
            select r.topic_key, t.name_pl, t.name_en, r.rank, r.score, r.trend, r.z_news, r.z_volume, r.z_attention
            from topic_rank as r inner join topic as t on r.topic_key = t.topic_key
            where r.period = ? and r.rank <= ? order by r.rank
            """,
            [period, int(cfg["max_topics"])],
        ).fetchall()
        topics = []
        for key, name_pl, name_en, rank, score, trend, z_news, z_volume, z_attention in ranked:
            headlines = con.execute(
                """
                select m.title, m.url, coalesce(o.name, m.outlet_key), m.lang, m.published_at
                from topic_matches as m left join news_outlet as o on m.outlet_key = o.outlet_key
                where m.topic_key = ? and m.day between ? and ? and m.url like 'https://%'
                order by m.published_at desc, m.headline_id limit ?
                """,
                [key, window["from"], window["to"], int(cfg["max_headlines"])],
            ).fetchall()
            topics.append(
                {
                    "topic_key": key,
                    "name_pl": name_pl,
                    "name_en": name_en,
                    "rank": int(rank),
                    "heat": round(float(score), 2),
                    "trend": trend,
                    "kpi_keys": kpis.get(key, []),
                    "headlines": [
                        {"title": t, "url": u, "outlet": o, "lang": lang, "published_at": iso(p)}
                        for t, u, o, lang, p in headlines
                    ],
                    "next_release": next_release.get(key),
                    "signals": {
                        name: None if z is None else round(float(z), 2)
                        for name, z in (("news", z_news), ("volume", z_volume), ("attention", z_attention))
                    },
                }
            )
        periods[period] = {"window": window, "topics": topics}
    return schema_checked({"version": 2, "generated_at": generated_at, "periods": periods}, "v2/topics")


def topics_review(doc: dict[str, Any]) -> str:
    """The job-summary review of topics.json during the internal period: top topics per period with headlines."""
    lines = [f"## Topics (internal review, {doc['generated_at']})", ""]
    for period, block in doc["periods"].items():
        lines += [f"### {period} ({block['window']['from']} … {block['window']['to']})", ""]
        lines += ["| # | topic | heat | trend | z news/volume/attention | headline |", "|---|---|---|---|---|---|"]
        for t in block["topics"]:
            z = "/".join(
                "–" if t["signals"][k] is None else str(t["signals"][k]) for k in ("news", "volume", "attention")
            )
            first = t["headlines"][0] if t["headlines"] else None
            title = first["title"].replace("|", "\\|") if first else ""
            headline = f"[{title}]({first['url']}) ({first['outlet']})" if first else ""
            lines.append(f"| {t['rank']} | {t['name_pl']} | {t['heat']} | {t['trend']} | {z} | {headline} |")
        lines.append("")
    return "\n".join(lines)


def topics(cat, client, generated_at: str) -> dict[str, Any]:
    """Write topics.json (publish on) or its internal review copy (publish off); returns sizes and top topics."""
    import gzip

    import yaml

    from ingestion.common.report import step_summary

    cfg = yaml.safe_load((PROJECT / "warehouse.yaml").read_text())["topics"]
    con = duckdb.connect()
    for name in ("topic_rank", "topic_matches"):
        con.register(name, gold(cat, "gold", name))
    for name in ("topic", "topic_kpi", "news_outlet", "release_calendar", "kpi_v2_map", "release_kpi"):
        con.register(name, gold(cat, "gold", name))
    con.register("releases", gold(cat, "silver", "releases"))
    doc = topics_json(con, cfg, generated_at)
    body = json.dumps(doc, ensure_ascii=False, separators=(",", ":")).encode()
    zipped = len(gzip.compress(body))
    if zipped > TOPICS_GZIP_LIMIT:
        raise ValueError(f"topics.json is {zipped} bytes gzipped, over {TOPICS_GZIP_LIMIT}")
    key = TOPICS_KEY if cfg.get("publish") else TOPICS_REVIEW_KEY
    put(client, key, body)
    step_summary(topics_review(doc))
    top = {p: [t["topic_key"] for t in b["topics"][:3]] for p, b in doc["periods"].items()}
    return {"key": key, "published": bool(cfg.get("publish")), "bytes": len(body), "gzip_bytes": zipped, "top": top}


def main(prefix: str = PREFIX, namespace: str = "gold") -> dict[str, Any]:
    """Write kpi/<kpi_key>/latest.json for every published KPI under prefix."""
    from pyiceberg.exceptions import NoSuchTableError

    from ingestion.common.r2 import catalog, s3
    from ingestion.common.report import annotate

    generated_at = iso(datetime.now(UTC))
    cat = catalog()
    client = s3()
    measures = seed("kpi_measure")
    checked_at = checked(gold(cat, "gold", "coverage"))
    written: dict[str, int] = {}
    missing: list[str] = []
    series: dict[str, int] = {}
    entries: list[dict[str, Any]] = []
    problems: list[str] = []
    domains = {d["domain_key"]: d for d in seed("domain")}
    tables: dict[str, pa.Table] = {}  # gold.metrics_<kpi_key> read below (licence-gated), reused by contract v2
    licensed = licensed_sources(seed("source"), seed("licence"))
    licence_removed: dict[str, int] = {}  # KPI → rows left out by the licence gate
    blocked: list[str] = []  # published KPIs whose every source is unlicensed: no files (earlier ones removed)
    for kpi in seed("kpi"):
        kpi_key = kpi["kpi_key"]
        if kpi["status"] != "published":
            continue
        if licence_blocked(kpi_key, measures, licensed):
            blocked.append(kpi_key)  # files removed at the end, after every write and read-back succeeded
            continue
        try:
            metrics = gold(cat, namespace, f"metrics_{kpi_key}")
        except NoSuchTableError:
            missing.append(kpi_key)
            continue
        gated = licence_gated(metrics, licensed, kpi_key)
        if gated.num_rows < metrics.num_rows:
            licence_removed[kpi_key] = metrics.num_rows - gated.num_rows
        metrics = gated
        tables[kpi_key] = metrics
        metrics = v1_rows(metrics)
        table = kpi_series(metrics, measures, kpi_key, kpi["unit_key"])
        data = parquet(table)
        if len(data) > SERIES_LIMIT:
            raise ValueError(f"kpi/{kpi_key}/series.parquet is {len(data)} bytes, over {SERIES_LIMIT}")
        put(client, prefix + f"kpi/{kpi_key}/series.parquet", data, "application/vnd.apache.parquet")
        series[kpi_key] = table.num_rows
        entries.append(kpi_entry(kpi, table, domains, checked_at, generated_at))
        if kpi_key in SKIP:
            continue
        latest = schema_checked(
            kpi_latest(metrics, measures, checked_at, kpi_key, kpi["unit_key"], generated_at), "kpi_latest"
        )
        body = json.dumps(latest, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(body) > LATEST_LIMIT:
            raise ValueError(f"kpi/{kpi_key}/latest.json is {len(body)} bytes, over {LATEST_LIMIT}")
        put(client, prefix + f"kpi/{kpi_key}/latest.json", body)
        problems += compare_latest(json.loads(get(client, prefix + f"kpi/{kpi_key}/latest.json")), metrics)
        written[kpi_key] = len(latest["values"])
    if problems:
        annotate(
            "error", "gold.publish: latest.json differs from Gold", {"count": len(problems), "first": problems[:20]}
        )
        raise ValueError(f"{len(problems)} latest.json values differ from Gold")
    result = {"written": written, "skipped": sorted(SKIP), "missing": missing, "differences": 0}
    result["licence_gate"] = {"blocked": blocked, "rows_removed": licence_removed}
    result["series"] = series
    result["catalog_tables"] = catalog_tables(cat, client, prefix, namespace)
    v1_catalog = catalog_json(entries, generated_at)
    body = json.dumps(v1_catalog, ensure_ascii=False, separators=(",", ":")).encode()
    result["catalog_json"] = {"kpis": len(entries), "bytes": put(client, prefix + "catalog/catalog.json", body)}
    from ingestion.common import scheduler
    import run

    next_at = scheduler.next_runs(run.load_config(), datetime.now(UTC))
    status = schema_checked(status_json(seed("source"), checked_at, next_at, generated_at), "status")
    body = json.dumps(status, ensure_ascii=False, separators=(",", ":")).encode()
    if len(body) > STATUS_LIMIT:
        raise ValueError(f"status.json is {len(body)} bytes, over {STATUS_LIMIT}")
    result["status"] = {"sources": len(status["sources"]), "bytes": put(client, prefix + "status.json", body)}
    result["manifest"] = manifest_json(client, prefix)
    result["calendar"] = calendar(client, prefix, generated_at)
    result["geo"] = geo(cat, client, prefix)
    if prefix == PREFIX:  # topics: the production run only (a trial prefix writes v1 files alone)
        result["topics"] = topics(cat, client, generated_at)
    for kpi_key in blocked:  # last v1 step, after every write and read-back; seed-decided, never by a failed read
        for name in ("series.parquet", "latest.json"):
            remove(client, prefix + f"kpi/{kpi_key}/{name}")
    annotate("notice", "gold.publish: latest", result)
    if prefix == PREFIX and published_v2():  # contract v2: the production run only, after every v1 file
        from warehouse import publish_v2

        try:
            result["v2"] = publish_v2.write(cat, client, tables, checked_at, v1_catalog, status, generated_at)
        except ValueError as error:
            annotate("error", "gold.publish: v2 not written in full", {"error": str(error)[:3000]})
            raise
        annotate("notice", "gold.publish: v2", result["v2"])
    return result


def published_v2() -> bool:
    """warehouse.yaml ``published_v2``: whether gold.publish writes contract v2 (docs/published.md)."""
    import yaml

    return bool(yaml.safe_load((PROJECT / "warehouse.yaml").read_text()).get("published_v2", False))
