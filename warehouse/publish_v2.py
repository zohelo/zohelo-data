"""gold.publish, contract v2: the files of ``06_published/v2/`` that zohelo-bi reads (docs/published.md,
"Published layer contract v2").

``catalog.json``, ``latest.json``, ``kpi/<kpi_key>/series.json``, ``calendar.json``, ``briefs.json`` (release
briefs: the facts of each new KPI period) and ``status.json`` (the v1 file, copied), written by
``warehouse/publish.py`` after the v1 files when warehouse.yaml ``published_v2`` is true (``topics.json`` has
its own writer there). The v2 KPIs are the v2 keys of ``seeds/kpi_v2_map.csv`` with a concept in ``seeds/kpi.csv``
and Gold series from a ready, licensed source, whatever the concept's status (owner decision O37: v2 is the review
stage, no publishing decision; the status is shown in catalog.json); each ready source of the map is its own series
(O31). Sources whose licence is unresolved or missing never reach v2 (licence gate, O37). Geography keys come
from ``seeds/geography_v2.csv``, decimals from ``seeds/unit_decimals.csv``, curated members from
``seeds/kpi_member.csv``. Every JSON file is checked against ``published/schemas/v2/`` and the cross-file rules of
``ops.repo_check.published_v2_problems`` before it is written, and every number is read back against Gold.
"""

from __future__ import annotations

from collections import defaultdict
from contextlib import suppress
from datetime import UTC, date, datetime, timedelta
import gzip
import json
import math
import re
from typing import Any

import duckdb
import pyarrow as pa

from warehouse.publish import get, licensed_sources, period_label, put, remove, schema_checked, seed

V2_PREFIX = "06_published/v2/"
GZIP_LIMITS = {  # docs/published.md
    "catalog.json": 1_000_000,
    "latest.json": 300_000,
    "series.json": 1_000_000,
    "briefs.json": 100_000,
}
RAW_LIMITS = {"calendar.json": 50_000, "status.json": 50_000}
SHOWN = ("M", "Q", "A")  # frequencies a v2 KPI or series can have, finest first
DAILY = ("D", "B")  # published as the monthly mean (frequency M)
# gold.dim_observation_status → v2 flag letter (SDMX/Eurostat letters); other statuses carry no flag.
FLAGS = {
    "provisional": "p",
    "estimated": "e",
    "break": "b",
    "forecast": "f",
    "low_reliability": "u",
    "definition_differs": "d",
    "not_significant": "n",
    "confidential": "c",
}
MEAN_DIGITS = 6  # monthly means of daily values are rounded to this many decimals
RELEASE_LOOKBACK_DAYS = 14  # briefs.json released_at: a release at most this many days before the detection


def dumps(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def v2_kpis(
    kpis: list[dict[str, str]], v2map: list[dict[str, str]], licensed: set[str] | None = None
) -> list[dict[str, Any]]:
    """The v2 KPI candidates: one entry per v2 key with one concept and at least one ready source that is licensed
    (``licensed``: source_keys of publish.licensed_sources; None = the seeds' own), with those sources and the
    measure patterns of each; the concept's status (draft or published) does not gate v2 (O37). Whether a candidate
    has Gold series is decided by ``build``. Pure apart from reading the source and licence seeds when ``licensed``
    is None. ValueError when a v2 key has several concepts or differing frequency, aggregation or polarity across
    its rows."""
    if licensed is None:
        licensed = licensed_sources(seed("source"), seed("licence"))
    by_concept = {k["kpi_key"]: k for k in kpis}
    rows_by_key: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in v2map:
        rows_by_key[row["v2_key"]].append(row)
    entries = []
    for key, rows in sorted(rows_by_key.items()):
        concepts = {r["concept_key"] for r in rows if r["concept_key"]}
        if len(concepts) > 1:
            raise ValueError(f"kpi_v2_map: {key} has several concepts {sorted(concepts)}")
        if not concepts:
            continue
        concept = concepts.pop()
        kpi = by_concept[concept]
        fixed = {}
        for field in ("frequency", "aggregation", "polarity"):
            values = {r[field] for r in rows}
            if len(values) != 1:
                raise ValueError(f"kpi_v2_map: {key} has several {field} values {sorted(values)}")
            fixed[field] = values.pop()
        if fixed["frequency"] not in SHOWN:
            raise ValueError(f"kpi_v2_map: {key} frequency {fixed['frequency']} is not one of {SHOWN}")
        ready = [
            r for r in rows if r["status"] == "ready" and r["concept_key"] == concept and r["source_key"] in licensed
        ]
        if not ready:  # needs-block / needs-source only, or no licensed source: nothing to publish
            continue
        sources = {r["source_key"]: [p for p in r["measure_key_pattern"].split(";") if p] for r in ready}
        # A non-geographic row (Gold `_Z`, e.g. the NBP rates against the zloty) belongs to the one geography the
        # map names for its source, when it names exactly one.
        unplaced = {
            r["source_key"]: r["geographies"] for r in ready if r["geographies"] and ";" not in r["geographies"]
        }
        entries.append(
            {"kpi_key": key, "concept": concept, "kpi": kpi, "sources": sources, "unplaced": unplaced, **fixed}
        )
    return entries


def geography_map(rows: list[dict[str, str]]) -> dict[str, str]:
    """Gold geography_key → v2 geography key, from seeds/geography_v2.csv; pure."""
    result = {}
    for row in rows:
        for gold_key in row["gold_keys"].split(";"):
            result[gold_key] = row["geography_key"]
    return result


def dimension_key(source_system: str, raw: str, aliases: dict[tuple[str, str], str]) -> str:
    """A breakdown key of breakdown_json as a v2 dimension key: dimension_alias.csv, else lower case with every
    other character than [a-z0-9_] as '_' (as v1 series.parquet)."""
    alias = aliases.get((source_system, raw))
    if alias:
        return alias
    return "".join(c if c in "abcdefghijklmnopqrstuvwxyz0123456789_" else "_" for c in raw.lower())


def _measure_lookup(measures: list[dict[str, str]], concept: str, unit_key: str):
    """(source, measure_key) → (priority, unit_key) of the best kpi_measure.csv row of the concept matching the
    measure (999 and the KPI's unit when none), as v1 kpi_latest."""
    own = [m for m in measures if m["kpi_key"] == concept]
    cache: dict[tuple[str, str], tuple[int, str]] = {}

    def lookup(source: str, measure_key: str) -> tuple[int, str]:
        if (source, measure_key) not in cache:
            best: tuple[int, str] | None = None
            for m in own:
                if m["source_system"] == source and _like(measure_key, m["measure_key_pattern"]):
                    candidate = (int(m["priority"]), m.get("unit_key") or unit_key)
                    best = candidate if best is None or candidate[0] < best[0] else best
            cache[(source, measure_key)] = best or (999, unit_key)
        return cache[(source, measure_key)]

    return lookup


def _like(value: str, pattern: str) -> bool:
    """SQL LIKE (``%`` any characters, ``_`` one character, case-sensitive) as DuckDB evaluates it."""
    regex = "".join(".*" if c == "%" else "." if c == "_" else re.escape(c) for c in pattern)
    return re.fullmatch(regex, value, flags=re.DOTALL) is not None


def gold_rows(metrics: pa.Table, entry: dict[str, Any], geo: dict[str, str]) -> list[dict[str, Any]]:
    """The rows of gold.metrics_<concept> that can reach v2: a value, a v2 geography, a ready source of the map
    and a measure matching that source's patterns, at a v2 frequency (M, Q, A, or daily for the monthly mean).
    Rows of every frequency are read: a source without the shown frequency gets a series of its own (O31)."""
    patterns = [(s, p) for s, ps in entry["sources"].items() for p in ps]
    frequencies = [*SHOWN, *DAILY]
    con = duckdb.connect()
    con.register("metrics", metrics)
    con.register(
        "patterns",
        pa.table({"source": [s for s, _ in patterns], "pattern": [p for _, p in patterns]}, schema=_PATTERNS),
    )
    con.register("geo", pa.table({"gold_key": list(geo), "v2_key": list(geo.values())}, schema=_GEO))
    unplaced = entry.get("unplaced", {})
    con.register("unplaced", pa.table({"source": list(unplaced), "v2_key": list(unplaced.values())}, schema=_UNPLACED))
    rows = con.execute(
        """
        select distinct m.source_system, m.measure_key, m.geography_key, coalesce(g.v2_key, u.v2_key) as v2_key,
            m.date_key, m.frequency, cast(m.value as double) as value, m.observation_status_key, m.breakdown_json
        from metrics m
        left join geo g on m.geography_key = g.gold_key
        left join unplaced u on m.geography_key = '_Z' and m.source_system = u.source
        where m.value is not null and m.frequency in (select unnest(?))
            and coalesce(g.v2_key, u.v2_key) in (select v2_key from geo)
            and exists (select 1 from patterns p where p.source = m.source_system and m.measure_key like p.pattern)
        order by all
        """,
        [frequencies],
    ).fetchall()
    columns = ["source", "measure", "gold_geo", "geography", "date_key", "frequency", "value", "status", "breakdown"]
    return [dict(zip(columns, r, strict=True)) for r in rows]


_PATTERNS = pa.schema([("source", pa.string()), ("pattern", pa.string())])
_GEO = pa.schema([("gold_key", pa.string()), ("v2_key", pa.string())])
_UNPLACED = pa.schema([("source", pa.string()), ("v2_key", pa.string())])


def _breakdown(row: dict[str, Any], aliases: dict[tuple[str, str], str]) -> dict[str, str]:
    raw = json.loads(row["breakdown"]) if row["breakdown"] else {}
    return {dimension_key(row["source"], k, aliases): str(v) for k, v in raw.items() if v is not None}


def _series_frequency(row: dict[str, Any]) -> str:
    """The v2 frequency a gold row feeds: its own, daily rows as M (the monthly mean)."""
    return "M" if row["frequency"] in DAILY else row["frequency"]


def kpi_cells(
    rows: list[dict[str, Any]],
    curated: dict[str, set[str]],
    aliases: dict[tuple[str, str], str],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Cells (source, geography, members, measure, period → value and flags) of one v2 KPI from its gold rows, and
    the KPI's dimension keys; pure.

    Gold metrics hold total series, so a breakdown key is a dimension only where it splits the rows of one measure
    at one frequency (daily counting as M; more than one value, a missing key counting as '_T'), e.g. the currency
    of the NBP rates; a key that differs only between frequencies (seasonally adjusted quarterly, unadjusted annual
    rows) is not a dimension, so rows of another frequency never change the shown series. Where kpi_member.csv
    lists members of a dimension, rows of other members are dropped. A dimension left with one value is not a
    dimension (members {}). Daily rows become the monthly mean per series."""
    parsed = [(row, _breakdown(row, aliases), _series_frequency(row)) for row in rows]
    keys_by_measure: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row, breakdown, frequency in parsed:
        keys_by_measure[(row["measure"], frequency)].update(breakdown)
    values_by_measure: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for row, breakdown, frequency in parsed:
        for key in keys_by_measure[(row["measure"], frequency)]:
            values_by_measure[(row["measure"], frequency, key)].add(breakdown.get(key, "_T"))
    split = {(m, f, k) for (m, f, k), values in values_by_measure.items() if len(values) > 1}
    kept = []
    for row, breakdown, frequency in parsed:
        members = {k: breakdown.get(k, "_T") for (m, f, k) in split if (m, f) == (row["measure"], frequency)}
        if any(k in breakdown and breakdown[k] not in allowed for k, allowed in curated.items()):
            continue
        if any(k in members and members[k] not in allowed for k, allowed in curated.items()):
            continue
        kept.append((row, members))
    distinct: dict[str, set[str]] = defaultdict(set)
    for _, members in kept:
        for k, v in members.items():
            distinct[k].add(v)
    dims = sorted(k for k, values in distinct.items() if len(values) > 1)
    grouped: dict[tuple, list[tuple[float, str | None]]] = defaultdict(list)
    for row, members in kept:
        shown = {k: v for k, v in members.items() if k in dims and v != "_T"}
        frequency = _series_frequency(row)
        key = (
            row["source"],
            row["geography"],
            tuple(sorted(shown.items())),
            row["measure"],
            period_label(row["date_key"], frequency),
            row["frequency"] in DAILY,
        )
        grouped[key].append((row["value"], FLAGS.get(row["status"] or "")))
    cells = []
    for (source, geography, members, measure, period, daily), items in grouped.items():
        flags = {f for _, f in items if f}
        values = [v for v, _ in items]
        cells.append(
            {
                "source": source,
                "geography": geography,
                "members": members,
                "measure": measure,
                "frequency": "M" if daily else period_frequency(period),
                "period": period,
                "values": [round(sum(values) / len(values), MEAN_DIGITS)] if daily else values,
                "flags": "".join(sorted(flags)),
            }
        )
    return cells, dims


def period_frequency(period: str) -> str:
    """'2025' → A, '2025-Q3' → Q, '2025-09' → M."""
    return "A" if len(period) == 4 else "Q" if "Q" in period else "M"


def series_json(
    entry: dict[str, Any],
    cells: list[dict[str, Any]],
    measures: list[dict[str, str]],
    geo_order: dict[str, int],
) -> tuple[dict[str, Any], list[str]]:
    """kpi/<kpi_key>/series.json of one v2 KPI from its cells, and the problems found (a period with two different
    values in one series); pure. One series per geography × source × members: the KPI's shown frequency when that
    series has it, else the finest other frequency it has (then the series carries its own ``frequency`` and follows
    ``periods_by_frequency``); within that frequency the measure with the best kpi_measure.csv priority, then the
    lowest measure key; its unit is the measure's unit, else the KPI's."""
    unit_key = entry["kpi"]["unit_key"]
    shown_frequency = entry["frequency"]
    lookup = _measure_lookup(measures, entry["concept"], unit_key)
    order = source_order(entry, measures)
    rank = {f: (f != shown_frequency, SHOWN.index(f)) for f in SHOWN}
    frequency_of: dict[tuple, str] = {}
    for c in cells:
        series_key = (c["source"], c["geography"], c["members"])
        current = frequency_of.get(series_key)
        if current is None or rank[c["frequency"]] < rank[current]:
            frequency_of[series_key] = c["frequency"]
    chosen: dict[tuple, str] = {}
    for c in cells:
        series_key = (c["source"], c["geography"], c["members"])
        if c["frequency"] != frequency_of[series_key]:
            continue
        current = chosen.get(series_key)
        candidate = (lookup(c["source"], c["measure"])[0], c["measure"])
        if current is None or candidate < (lookup(c["source"], current)[0], current):
            chosen[series_key] = c["measure"]
    problems = []
    data: dict[tuple, dict[str, tuple[float, str]]] = defaultdict(dict)
    for c in cells:
        series_key = (c["source"], c["geography"], c["members"])
        if c["frequency"] != frequency_of[series_key] or chosen[series_key] != c["measure"]:
            continue
        distinct = sorted(set(c["values"]))
        if len(distinct) > 1:
            problems.append(
                f"{entry['kpi_key']} {c['source']} {c['geography']} {dict(c['members'])} {c['period']}: "
                f"several values {distinct[:3]}"
            )
            continue
        data[series_key][c["period"]] = (distinct[0], c["flags"])
    axes: dict[str, set[str]] = defaultdict(set)
    for series_key, values in data.items():
        axes[frequency_of[series_key]].update(values)
    axis = {f: sorted(periods) for f, periods in axes.items()}
    series = []
    for series_key in sorted(data, key=lambda k: (geo_order[k[1]], order.index(k[0]), k[2])):
        source, geography, members = series_key
        frequency = frequency_of[series_key]
        periods = axis[frequency]
        values = data[series_key]
        entry_series: dict[str, Any] = {
            "geography": geography,
            "source_key": source,
            "unit_key": lookup(source, chosen[series_key])[1],
            "members": dict(members),
        }
        if frequency != shown_frequency:
            entry_series["frequency"] = frequency
        entry_series["values"] = [values[p][0] if p in values else None for p in periods]
        flags = {p: values[p][1] for p in periods if p in values and values[p][1]}
        if flags:
            entry_series["flags"] = flags
        series.append(entry_series)
    doc: dict[str, Any] = {
        "kpi_key": entry["kpi_key"],
        "frequency": shown_frequency,
        "unit_key": unit_key,
        "periods": axis.get(shown_frequency, []),
    }
    others = {f: axis[f] for f in SHOWN if f in axis and f != shown_frequency}
    if others:
        doc["periods_by_frequency"] = others
    doc["series"] = series
    return doc, problems


def series_periods(doc: dict[str, Any], series: dict[str, Any]) -> list[str]:
    """The periods a series' values follow: ``periods_by_frequency`` of its own ``frequency``, else ``periods``."""
    if "frequency" in series:
        return doc.get("periods_by_frequency", {}).get(series["frequency"], [])
    return doc["periods"]


def source_order(entry: dict[str, Any], measures: list[dict[str, str]]) -> list[str]:
    """The v2 KPI's ready sources, preferred first: best kpi_measure.csv priority of the concept, then map order."""
    priority = {}
    for m in measures:
        if m["kpi_key"] == entry["concept"]:
            priority[m["source_system"]] = min(priority.get(m["source_system"], 999), int(m["priority"]))
    sources = list(entry["sources"])
    return sorted(sources, key=lambda s: (priority.get(s, 999), sources.index(s)))


def year_ago(period: str, frequency: str) -> str | None:
    """The same period a year earlier ('2025-09' → '2024-09', '2025-Q3' → '2024-Q3'); None for annual."""
    if frequency == "A":
        return None
    return f"{int(period[:4]) - 1}{period[4:]}"


def latest_values(
    series_doc: dict[str, Any],
    sources: list[str],
    checked_at: dict[str, str],
    generated_at: str,
    daily: frozenset[tuple[str, str]] = frozenset(),
) -> list[dict[str, Any]]:
    """latest.json values[] of one v2 KPI: per geography (totals only) the preferred source with data, its newest
    period and value, the period before with data, the value a year earlier (M and Q) and flags; pure. Only series
    of the shown frequency count (a series with its own ``frequency`` never reaches latest.json): a geography
    without a shown-frequency value of its preferred source falls back to the next source that has one, else has no
    entry. A series fed by daily rows (``daily``: source × geography) skips the month of generated_at, whose mean
    is still incomplete; series.json keeps it."""
    periods = series_doc["periods"]
    current = generated_at[:7]

    def filled_of(s: dict[str, Any]) -> list[int]:
        partial = (s["source_key"], s["geography"]) in daily
        return [i for i, v in enumerate(s["values"]) if v is not None and not (partial and periods[i] >= current)]

    index = {p: i for i, p in enumerate(periods)}
    totals = [s for s in series_doc["series"] if not s["members"] and "frequency" not in s]
    result = []
    for geography in dict.fromkeys(s["geography"] for s in totals):
        candidates = [s for s in totals if s["geography"] == geography and filled_of(s)]
        if not candidates:
            continue
        shown = min(candidates, key=lambda s: sources.index(s["source_key"]))
        filled = filled_of(shown)
        last = filled[-1]
        previous = filled[-2] if len(filled) > 1 else None
        ago = year_ago(periods[last], series_doc["frequency"])
        flags = (shown.get("flags") or {}).get(periods[last], "")
        result.append(
            {
                "kpi_key": series_doc["kpi_key"],
                "geography": geography,
                "source_key": shown["source_key"],
                "unit_key": shown["unit_key"],
                "period": periods[last],
                "value": shown["values"][last],
                "previous_period": periods[previous] if previous is not None else None,
                "previous_value": shown["values"][previous] if previous is not None else None,
                "year_ago_value": shown["values"][index[ago]] if ago in index else None,
                "flags": list(flags),
                "checked_at": checked_at.get(shown["source_key"], generated_at),
            }
        )
    return result


def releases(
    calendar_rows: list[dict[str, str]],
    silver_rows: list[dict[str, Any]],
    v2_of_concept: dict[str, str],
    v2_keys: set[str],
    today: date,
) -> list[dict[str, Any]]:
    """calendar.json releases[]: every release_calendar.csv row dated today or later (kpi_keys = the v2 key of
    its concept when that KPI is in v2) and every silver.releases row dated today or later that publishes a v2
    KPI; rows of one date, source, title and URL are merged; pure."""
    merged: dict[tuple, set[str]] = {}
    for row in calendar_rows:
        day = row["release_at"][:10]
        key = v2_of_concept.get(row["kpi_key"])
        merged.setdefault((day, row["source_key"], row["title_pl"], row["title_en"], row["url"] or None), set())
        if key in v2_keys:
            merged[(day, row["source_key"], row["title_pl"], row["title_en"], row["url"] or None)].add(key)
    for row in silver_rows:
        keys = {k.strip() for k in (row["kpi_keys"] or "").split(";")} & v2_keys
        if not keys:
            continue
        day = row["release_date"].isoformat() if isinstance(row["release_date"], date) else str(row["release_date"])
        merged.setdefault((day, row["publisher"], row["title"], row["title"], row["url"] or None), set()).update(keys)
    return [
        {"date": d, "source_key": s, "kpi_keys": sorted(k), "title_pl": tp, "title_en": te, "url": u}
        for (d, s, tp, te, u), k in sorted(merged.items(), key=lambda i: (i[0][0], i[0][1], i[0][2]))
        if d >= today.isoformat()
    ]


def past_releases(
    calendar_rows: list[dict[str, str]], silver_rows: list[dict[str, Any]], v2_of_concept: dict[str, str]
) -> list[tuple[str, str, str]]:
    """(date, publisher, v2 key) of every known release of a v2 KPI, past and future: the release_calendar.csv rows
    (concept → v2 key) and the silver.releases rows that are not tentative (their kpi_keys are v2 keys); the
    publisher is the source or publisher key before its first '_' (`gus` for `gus_bdl`); pure."""
    result = []
    for row in calendar_rows:
        if row["kpi_key"] in v2_of_concept:
            result.append((row["release_at"][:10], publisher(row["source_key"]), v2_of_concept[row["kpi_key"]]))
    for row in silver_rows:
        if row.get("tentative"):
            continue
        day = row["release_date"].isoformat() if isinstance(row["release_date"], date) else str(row["release_date"])
        for key in {k.strip() for k in (row["kpi_keys"] or "").split(";") if k.strip()}:
            result.append((day[:10], publisher(row["publisher"]), key))
    return sorted(set(result))


def publisher(key: str) -> str:
    """The publisher of a source or release key: its part before the first '_' (`gus_dbw` → `gus`)."""
    return key.split("_")[0]


def released_on(known: list[tuple[str, str, str]], kpi_key: str, source_key: str, detected: date) -> str | None:
    """The newest known release of the KPI by the series' publisher dated on the detection day or up to
    RELEASE_LOOKBACK_DAYS before it; None when none."""
    first = (detected - timedelta(days=RELEASE_LOOKBACK_DAYS)).isoformat()
    days = [
        d
        for d, by, key in known
        if key == kpi_key and by == publisher(source_key) and first <= d <= detected.isoformat()
    ]
    return max(days) if days else None


def briefs_json(
    values: list[dict[str, Any]],
    series_docs: dict[str, dict[str, Any]],
    kpis: dict[str, dict[str, Any]],
    previous: dict[str, dict[str, Any] | None],
    known_releases: list[tuple[str, str, str]],
    calendar: list[dict[str, Any]],
    config: dict[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    """briefs.json: the facts of a rule-written release brief for every latest.json value of the configured
    geographies whose period is new; pure (docs/published.md, "briefs.json").

    New: the period is later than the one of the previously published latest.json (``previous["latest.json"]``)
    for the same KPI and geography, of the same frequency and from the same source. ``detections`` (kept in
    briefs.json) remembers every period detected in the last window_days: a period with a detection keeps its
    detected_at, and a new period at or below a detected one (a transient step back, P → P-1 → P) reuses that
    detection's detected_at. A KPI or geography that was not in the previous latest.json gets no brief (first
    publication is not a release). A brief stays while detected_at is at most window_days old; its numbers are
    recomputed from the current files every run (revisions included). next_release is the KPI's first calendar.json
    date after released_at."""
    from ops.repo_check import brief_change, brief_change_unit, brief_extreme, period_ordinal

    def utc(stamp: str) -> datetime:
        return datetime.strptime(stamp[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)

    oldest = utc(generated_at) - timedelta(days=int(config["window_days"]))
    geographies = list(config["geographies"])
    before = {(v["kpi_key"], v["geography"]): v for v in (previous.get("latest.json") or {}).get("values", [])}
    detections = {
        (d["kpi_key"], d["geography"], d["period"]): d
        for d in (previous.get("briefs.json") or {}).get("detections", [])
        if utc(d["detected_at"]) >= oldest
    }
    dates: dict[str, list[str]] = defaultdict(list)
    for release in calendar:
        for key in release["kpi_keys"]:
            dates[key].append(release["date"])
    briefs = []
    for v in values:
        if v["geography"] not in geographies or v["kpi_key"] not in kpis:
            continue
        kpi = kpis[v["kpi_key"]]
        frequency = period_frequency(v["period"])
        at_or_above = sorted(
            (period_ordinal(p), d)
            for (k, g, p), d in detections.items()
            if (k, g) == (v["kpi_key"], v["geography"])
            and period_frequency(p) == frequency
            and period_ordinal(p) >= period_ordinal(v["period"])
        )
        old = detections.get((v["kpi_key"], v["geography"], v["period"]))
        last = before.get((v["kpi_key"], v["geography"]))
        if old is None:
            if (
                last is None
                or last["source_key"] != v["source_key"]
                or period_frequency(last["period"]) != frequency
                or period_ordinal(v["period"]) <= period_ordinal(last["period"])
            ):
                continue
            old = at_or_above[0][1] if at_or_above else None
        detected_at = old["detected_at"] if old is not None else generated_at
        doc = series_docs[v["kpi_key"]]
        shown = next(
            s
            for s in doc["series"]
            if s["geography"] == v["geography"]
            and s["source_key"] == v["source_key"]
            and not s["members"]
            and "frequency" not in s
        )
        unit = brief_change_unit(kpi["aggregation"], v["unit_key"], shown["values"])
        ago = year_ago(v["period"], kpi["frequency"]) if v["year_ago_value"] is not None else None
        released = released_on(known_releases, v["kpi_key"], v["source_key"], date.fromisoformat(detected_at[:10]))
        source = "calendar" if released else "detected"
        if released is None and old is not None and old.get("released_at_source") == "calendar":
            released, source = old["released_at"], "calendar"  # the calendar row has left silver.releases since
        released = released or detected_at[:10]
        later = sorted(d for d in dates.get(v["kpi_key"], []) if d > released)
        detections[(v["kpi_key"], v["geography"], v["period"])] = {
            "kpi_key": v["kpi_key"],
            "geography": v["geography"],
            "period": v["period"],
            "detected_at": detected_at,
            "released_at": released,
            "released_at_source": source,
        }
        briefs.append(
            {
                "kpi_key": v["kpi_key"],
                "geography": v["geography"],
                "source_key": v["source_key"],
                "unit_key": v["unit_key"],
                "period": v["period"],
                "value": v["value"],
                "previous_period": v["previous_period"],
                "previous_value": v["previous_value"],
                "change": brief_change(v["value"], v["previous_value"], unit),
                "change_unit": unit,
                "year_ago_period": ago,
                "year_ago_value": v["year_ago_value"],
                "year_ago_change": brief_change(v["value"], v["year_ago_value"], unit),
                "extreme": brief_extreme(
                    doc["periods"],
                    shown["values"],
                    doc["periods"].index(v["period"]),
                    int(config["extreme_min_periods"]),
                ),
                "flags": v["flags"],
                "released_at": released,
                "released_at_source": source,
                "detected_at": detected_at,
                "next_release": later[0] if later else None,
            }
        )
    briefs.sort(key=lambda b: (b["kpi_key"], geographies.index(b["geography"])))
    briefs.sort(key=lambda b: (b["detected_at"], b["released_at"]), reverse=True)
    return {
        "version": 2,
        "generated_at": generated_at,
        "window_days": int(config["window_days"]),
        "extreme_min_periods": int(config["extreme_min_periods"]),
        "geographies": geographies,
        "briefs": briefs,
        "detections": [detections[k] for k in sorted(detections)],
    }


def usable_previous(previous: dict[str, dict[str, Any] | None]) -> dict[str, dict[str, Any]]:
    """The previously published latest.json and briefs.json that have the shape briefs_json reads (a document
    that does not is treated as absent); pure."""
    stamp = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}")
    needs = {
        "latest.json": ("values", ("kpi_key", "geography", "source_key", "period")),
        "briefs.json": ("detections", ("kpi_key", "geography", "period", "detected_at", "released_at")),
    }
    result = {}
    for name, (field, keys) in needs.items():
        doc = previous.get(name)
        rows = doc.get(field) if isinstance(doc, dict) else None
        if not isinstance(rows, list):
            continue
        if all(isinstance(r, dict) and all(isinstance(r.get(k), str) for k in keys) for r in rows) and all(
            stamp.match(r["detected_at"]) for r in rows if "detected_at" in r
        ):
            result[name] = doc
    return result


def briefs_config() -> dict[str, Any]:
    """warehouse.yaml ``briefs`` (window_days, extreme_min_periods, geographies)."""
    import yaml

    from warehouse.publish import PROJECT

    return yaml.safe_load((PROJECT / "warehouse.yaml").read_text())["briefs"]


def _path(domains: dict[str, dict[str, str]], key: str) -> dict[str, str]:
    levels: dict[str, str] = {}
    current: str | None = key
    while current:
        row = domains.get(current) or {}
        levels.setdefault(row.get("level", ""), current)
        current = row.get("parent_key") or None
    return levels


def catalog_entry(
    entry: dict[str, Any],
    series_doc: dict[str, Any],
    dims: list[str],
    sources: list[str],
    domains: dict[str, dict[str, str]],
    geo_order: dict[str, int],
    checked_at: dict[str, str],
    next_release: str | None,
    generated_at: str,
) -> dict[str, Any]:
    """One catalog.json kpis[] entry; pure."""
    kpi = entry["kpi"]
    levels = _path(domains, kpi["subgroup_key"])
    with_data = {s["source_key"] for s in series_doc["series"]}
    source_keys = [s for s in sources if s in with_data]
    stamps = [checked_at[s] for s in source_keys if s in checked_at]
    return {
        "kpi_key": entry["kpi_key"],
        "name_pl": kpi["name_pl"],
        "name_en": kpi["name_en"],
        "definition_pl": kpi.get("definition_pl") or "",
        "definition_en": kpi.get("definition_en") or "",
        "unit_key": kpi["unit_key"],
        "frequency": entry["frequency"],
        "domain_key": levels.get("domain", ""),
        "group_key": levels.get("group") or levels.get("domain", ""),
        "source_keys": source_keys,
        "geographies": sorted({s["geography"] for s in series_doc["series"]}, key=geo_order.__getitem__),
        "dimension_keys": dims,
        "first_period": series_doc["periods"][0],
        "last_period": series_doc["periods"][-1],
        "aggregation": entry["aggregation"],
        "polarity": entry["polarity"],
        "loaded_at": max(stamps) if stamps else generated_at,
        "next_release": next_release,
        "status": kpi["status"],
    }


def dimensions_json(
    used: dict[str, dict[str, set[str]]],
    dimension_rows: list[dict[str, str]],
    member_rows: list[dict[str, str]],
    sex_rows: list[dict[str, str]],
) -> list[dict[str, Any]]:
    """catalog.json dimensions[] from the members used per dimension and KPI ({dimension: {kpi_key: members}}):
    names from dimension.csv, member names, order and parent from kpi_member.csv, else sex.csv for sex, else the
    member key itself; pure."""
    names = {d["dimension_key"]: d for d in dimension_rows}
    curated = {(m["v2_key"], m["dimension_key"], m["member_key"]): m for m in member_rows}
    sexes = {s["sex_key"]: s for s in sex_rows}
    result = []
    for dim in sorted(used):
        members: dict[str, dict[str, Any]] = {}
        for kpi_key in sorted(used[dim]):
            for key in sorted(used[dim][kpi_key]):
                if key in members:
                    continue
                row = curated.get((kpi_key, dim, key)) or (sexes.get(key) if dim == "sex" else None)
                member: dict[str, Any] = {
                    "key": key,
                    "name_pl": (row or {}).get("name_pl") or key,
                    "name_en": (row or {}).get("name_en") or key,
                    "sort": int((row or {}).get("sort") or 0),
                }
                if (row or {}).get("parent_key"):
                    member["parent_key"] = row["parent_key"]
                members[key] = member
        listed = sorted(members.values(), key=lambda m: (m["sort"] == 0, m["sort"], m["key"]))
        for i, member in enumerate(listed, start=1):
            member["sort"] = member["sort"] or i
        name = names.get(dim) or {}
        result.append(
            {
                "dimension_key": dim,
                "name_pl": name.get("name_pl") or dim,
                "name_en": name.get("name_en") or dim,
                "members": listed,
            }
        )
    return result


def catalog_json(
    kpis: list[dict[str, Any]],
    dimensions: list[dict[str, Any]],
    geography_rows: list[dict[str, str]],
    v1_catalog: dict[str, Any],
    decimals: dict[str, int],
    generated_at: str,
) -> dict[str, Any]:
    """catalog.json; domains, sources and licences as v1 (``v1_catalog`` = publish.catalog_json), units as v1 plus
    decimals (ValueError for a unit without decimals), geographies for every key a KPI uses; pure."""
    used = {g for k in kpis for g in k["geographies"]}
    geographies = [
        {
            "key": g["geography_key"],
            "name_pl": g["name_pl"],
            "name_en": g["name_en"],
            "level": g["level"],
            "eu": g["eu"] == "true",
        }
        for g in sorted(geography_rows, key=lambda g: int(g["sort"]))
        if g["geography_key"] in used
    ]
    missing = [u["unit_key"] for u in v1_catalog["units"] if u["unit_key"] not in decimals]
    if missing:
        raise ValueError(f"unit_decimals.csv: no decimals for {missing}")
    return {
        "version": 2,
        "generated_at": generated_at,
        "kpis": kpis,
        "dimensions": dimensions,
        "geographies": geographies,
        "units": [{**u, "decimals": decimals[u["unit_key"]]} for u in v1_catalog["units"]],
        "domains": v1_catalog["domains"],
        "sources": v1_catalog["sources"],
        "licences": v1_catalog["licences"],
    }


def gold_index(
    rows: list[dict[str, Any]], aliases: dict[tuple[str, str], str]
) -> dict[tuple[str, str, str], list[tuple[dict[str, str], float]]]:
    """Read-back index straight from the gold rows: (source, v2 geography, period) → (breakdown, value) pairs, daily
    rows as the mean of each month per measure, geography and breakdown."""
    index: dict[tuple[str, str, str], list[tuple[dict[str, str], float]]] = defaultdict(list)
    days: dict[tuple, list[float]] = defaultdict(list)
    for row in rows:
        breakdown = _breakdown(row, aliases)
        if row["frequency"] in DAILY:
            month = row["date_key"].strftime("%Y-%m")
            frozen = tuple(sorted(breakdown.items()))
            days[(row["source"], row["geography"], month, row["measure"], row["gold_geo"], frozen)].append(row["value"])
            continue
        period = period_label(row["date_key"], row["frequency"])
        index[(row["source"], row["geography"], period)].append((breakdown, row["value"]))
    for (source, geography, month, _, _, frozen), values in days.items():
        index[(source, geography, month)].append((dict(frozen), sum(values) / len(values)))
    return index


def compare_series(
    doc: dict[str, Any], index: dict[tuple[str, str, str], list], curated: dict[str, set[str]]
) -> list[str]:
    """Numbers of series.json that are not in Gold for their source, geography, period and members (each series on
    its own periods: ``periods``, or ``periods_by_frequency`` of its ``frequency``); pure."""
    problems = []
    for s in doc["series"]:
        periods = series_periods(doc, s)
        if len(periods) != len(s["values"]):
            problems.append(
                f"{doc['kpi_key']} {s['source_key']} {s['geography']} {s['members']}: {len(s['values'])} values for "
                f"{len(periods)} periods of frequency {s.get('frequency', doc['frequency'])}"
            )
            continue
        for period, value in zip(periods, s["values"], strict=True):
            if value is not None and not _in_gold(
                index, s["source_key"], s["geography"], period, s["members"], value, curated
            ):
                problems.append(
                    f"{doc['kpi_key']} {s['source_key']} {s['geography']} {s['members']} {period}: {value} not in Gold"
                )
    return problems


def compare_latest(
    latest: dict[str, Any], indexes: dict[str, dict], curated: dict[str, dict[str, set[str]]], frequency: dict[str, str]
) -> list[str]:
    """Values, previous values and year-ago values of latest.json that are not in Gold; pure."""
    problems = []
    for v in latest["values"]:
        index = indexes[v["kpi_key"]]
        pairs = [(v["period"], v["value"]), (v["previous_period"], v["previous_value"])]
        pairs.append((year_ago(v["period"], frequency[v["kpi_key"]]), v["year_ago_value"]))
        for period, value in pairs:
            if value is None:
                continue
            if not _in_gold(index, v["source_key"], v["geography"], period, {}, value, curated[v["kpi_key"]]):
                problems.append(
                    f"latest {v['kpi_key']} {v['source_key']} {v['geography']} {period}: {value} not in Gold"
                )
    return problems


def _in_gold(index, source, geography, period, members, value, curated) -> bool:
    for breakdown, gold_value in index.get((source, geography, period), []):
        if any(breakdown.get(k, "_T") != m for k, m in members.items()):
            continue
        if any(k in breakdown and breakdown[k] not in allowed for k, allowed in curated.items()):
            continue
        if math.isclose(gold_value, value, rel_tol=1e-9, abs_tol=10**-MEAN_DIGITS):
            return True
    return False


def _sized(name: str, body: bytes) -> dict[str, int]:
    limit_key = "series.json" if name.endswith("/series.json") else name
    if limit_key in GZIP_LIMITS:
        zipped = len(gzip.compress(body))
        if zipped > GZIP_LIMITS[limit_key]:
            raise ValueError(f"v2/{name} is {zipped} bytes gzipped, over {GZIP_LIMITS[limit_key]}")
        return {"bytes": len(body), "gzip_bytes": zipped}
    if len(body) > RAW_LIMITS[limit_key]:
        raise ValueError(f"v2/{name} is {len(body)} bytes, over {RAW_LIMITS[limit_key]}")
    return {"bytes": len(body)}


def build(
    metrics: dict[str, pa.Table],
    checked_at: dict[str, str],
    silver_releases: list[dict[str, Any]],
    v1_catalog: dict[str, Any],
    status: dict[str, Any],
    generated_at: str,
    previous: dict[str, dict[str, Any] | None] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Every v2 file (path under the v2 prefix → document) from the gold.metrics_<concept> tables of the v2 KPIs
    (``metrics``, by concept; a concept missing there is reported as missing) and the seeds, plus the read-back
    context; no R2. ``previous`` holds the previously published ``latest.json`` and ``briefs.json`` (None when
    absent), from which briefs.json tells which periods are new. ValueError when a series has a period with two
    different values or a file breaks its schema, the cross-file rules or its size limit."""
    from ops.repo_check import published_v2_problems

    measures = seed("kpi_measure")
    geo_rows = seed("geography_v2")
    geo = geography_map(geo_rows)
    geo_order = {g["geography_key"]: int(g["sort"]) for g in geo_rows}
    aliases = {(a["source_system"], a["source_dimension"]): a["dimension_key"] for a in seed("dimension_alias")}
    member_rows = seed("kpi_member")
    domains = {d["domain_key"]: d for d in seed("domain")}
    entries = v2_kpis(seed("kpi"), seed("kpi_v2_map"))
    v2_of_concept = {e["concept"]: e["kpi_key"] for e in entries}
    files: dict[str, dict[str, Any]] = {}
    missing, empty, problems = [], [], []
    built = []
    used: dict[str, dict[str, set[str]]] = defaultdict(dict)
    context: dict[str, Any] = {
        "indexes": {},
        "curated": {},
        "frequency": {},
        "daily": {},
        "missing": missing,
        "empty": empty,
    }
    for entry in entries:
        if entry["concept"] not in metrics:
            missing.append(entry["kpi_key"])
            continue
        curated: dict[str, set[str]] = defaultdict(set)
        for m in member_rows:
            if m["v2_key"] == entry["kpi_key"]:
                curated[m["dimension_key"]].add(m["member_key"])
        rows = gold_rows(metrics[entry["concept"]], entry, geo)
        cells, dims = kpi_cells(rows, curated, aliases)
        doc, found = series_json(entry, cells, measures, geo_order)
        problems += found
        if not doc["periods"]:
            empty.append(entry["kpi_key"])
            continue
        files[f"kpi/{entry['kpi_key']}/series.json"] = schema_checked(doc, "v2/series")
        dims = [d for d in dims if any(d in s["members"] for s in doc["series"])]
        for dim in dims:
            used[dim][entry["kpi_key"]] = {s["members"][dim] for s in doc["series"] if dim in s["members"]}
        context["indexes"][entry["kpi_key"]] = gold_index(rows, aliases)
        context["curated"][entry["kpi_key"]] = dict(curated)
        context["frequency"][entry["kpi_key"]] = entry["frequency"]
        context["daily"][entry["kpi_key"]] = frozenset(
            (r["source"], r["geography"]) for r in rows if r["frequency"] in DAILY
        )
        built.append((entry, doc, dims))
    if problems:
        raise ValueError(f"{len(problems)} series cells have several values in Gold: {problems[:10]}")
    keys = {e["kpi_key"] for e, _, _ in built}
    today = date.fromisoformat(generated_at[:10])
    calendar_rows = seed("release_calendar")
    calendar = releases(calendar_rows, silver_releases, v2_of_concept, keys, today)
    next_release = {}
    for release in calendar:
        for key in release["kpi_keys"]:
            next_release.setdefault(key, release["date"])
    kpis = []
    values = []
    for entry, doc, dims in built:
        sources = source_order(entry, measures)
        kpis.append(
            catalog_entry(
                entry,
                doc,
                dims,
                sources,
                domains,
                geo_order,
                checked_at,
                next_release.get(entry["kpi_key"]),
                generated_at,
            )
        )
        values += latest_values(doc, sources, checked_at, generated_at, context["daily"][entry["kpi_key"]])
    decimals = {u["unit_key"]: int(u["decimals"]) for u in seed("unit_decimals")}
    dimensions = dimensions_json(used, seed("dimension"), member_rows, seed("sex"))
    files["catalog.json"] = schema_checked(
        catalog_json(kpis, dimensions, geo_rows, v1_catalog, decimals, generated_at), "v2/catalog"
    )
    files["latest.json"] = schema_checked({"version": 2, "generated_at": generated_at, "values": values}, "v2/latest")
    files["calendar.json"] = schema_checked(
        {"version": 2, "generated_at": generated_at, "releases": calendar}, "v2/calendar"
    )
    files["status.json"] = schema_checked(status, "v2/status")
    files["briefs.json"] = schema_checked(
        briefs_json(
            values,
            {e["kpi_key"]: doc for e, doc, _ in built},
            {k["kpi_key"]: k for k in kpis},
            usable_previous(previous or {}),
            past_releases(calendar_rows, silver_releases, v2_of_concept),
            calendar,
            briefs_config(),
            generated_at,
        ),
        "v2/briefs",
    )
    cross = published_v2_problems(files)
    if cross:
        raise ValueError(f"v2 cross-file problems: {cross[:10]}")
    for name, doc in files.items():
        _sized(name, dumps(doc))
    return files, context


def read_back(files: dict[str, dict[str, Any]], context: dict[str, Any]) -> list[str]:
    """Every number of the written series.json and latest.json files that is not in Gold; pure."""
    problems = []
    for name, doc in files.items():
        if name.endswith("/series.json"):
            problems += compare_series(doc, context["indexes"][doc["kpi_key"]], context["curated"][doc["kpi_key"]])
    problems += compare_latest(files["latest.json"], context["indexes"], context["curated"], context["frequency"])
    if "briefs.json" in files:  # value, previous and year-ago value of every brief
        briefs = {"values": files["briefs.json"]["briefs"]}
        found = compare_latest(briefs, context["indexes"], context["curated"], context["frequency"])
        problems += [f"briefs.json: {p}" for p in found]
    return problems


def write(
    cat,
    client,
    metrics: dict[str, pa.Table],
    checked_at: dict[str, str],
    v1_catalog: dict[str, Any],
    status: dict[str, Any],
    generated_at: str,
    prefix: str = V2_PREFIX,
) -> dict[str, Any]:
    """Write every v2 file under prefix, read each back and compare its numbers with Gold; returns sizes and counts.

    ``metrics`` holds the gold.metrics_<concept> tables gold.publish has already read (by concept); the others are
    read here. silver.releases is optional (calendar.json then has the release_calendar.csv rows only)."""
    from pyiceberg.exceptions import NoSuchTableError

    from warehouse.publish import gold

    tables = dict(metrics)
    candidates = v2_kpis(seed("kpi"), seed("kpi_v2_map"))
    for entry in candidates:
        if entry["concept"] not in tables:
            with suppress(NoSuchTableError):
                tables[entry["concept"]] = gold(cat, "gold", f"metrics_{entry['concept']}")
    try:
        silver_releases = gold(cat, "silver", "releases").to_pylist()
    except NoSuchTableError:
        silver_releases = []
    previous = {name: _published(client, prefix + name) for name in ("latest.json", "briefs.json")}
    unusable = sorted(n for n, doc in previous.items() if doc is not None and n not in usable_previous(previous))
    if unusable:
        from ingestion.common.report import annotate

        annotate("warning", "gold.publish: previous v2 file without the expected keys, treated as absent", unusable)
    files, context = build(tables, checked_at, silver_releases, v1_catalog, status, generated_at, previous)
    stale, lost = stale_plan(kpi_objects(client, prefix), {e["kpi_key"] for e in candidates}, files)
    if lost:  # a KPI still in scope lost its series in this run (missing table, read failure): keep what is live
        raise ValueError(
            f"v2 KPIs in scope but not built this run, while published: {lost}; nothing written "
            "(fix the Gold table, or take the KPI out of scope in kpi_v2_map.csv)"
        )
    sizes = {}
    # series first, catalog after them; briefs.json before latest.json, whose previous copy tells what is new
    for name in sorted(files, key=lambda n: (not n.startswith("kpi/"), n)):
        sizes[name] = _sized(name, dumps(files[name]))
        put(client, prefix + name, dumps(files[name]))
    written = {name: json.loads(get(client, prefix + name)) for name in files}
    problems = read_back(written, context)
    if problems:
        raise ValueError(f"{len(problems)} v2 numbers differ from Gold: {problems[:20]}")
    for name in stale:  # after a clean read-back: files of KPIs out of scope (seeds, licence gate) only
        remove(client, prefix + name)
    return {
        "prefix": prefix,
        "kpis": sorted(n.split("/")[1] for n in files if n.startswith("kpi/")),
        "removed": stale,
        "missing": context["missing"],
        "empty": context["empty"],
        "latest_values": len(files["latest.json"]["values"]),
        "releases": len(files["calendar.json"]["releases"]),
        "briefs": [f"{b['kpi_key']} {b['geography']} {b['period']}" for b in files["briefs.json"]["briefs"]][:40],
        "bytes": {n: s.get("gzip_bytes", s["bytes"]) for n, s in sizes.items()},
        "differences": 0,
    }


def kpi_objects(client, prefix: str) -> set[str]:
    """Paths (relative to prefix) of the ``kpi/<kpi_key>/…`` objects published under prefix."""
    from ingestion.common.r2 import req

    found = set()
    pages = client.get_paginator("list_objects_v2").paginate(
        Bucket=req("STORAGE_LAKEHOUSE_BUCKET_NAME"), Prefix=prefix + "kpi/"
    )
    for page in pages:
        found |= {str(item["Key"])[len(prefix) :] for item in page.get("Contents", [])}
    return found


def stale_plan(existing: set[str], candidates: set[str], files: dict[str, Any]) -> tuple[list[str], list[str]]:
    """Which published ``kpi/<kpi_key>/…`` paths this run removes, and which KPIs it lost; pure.

    A KPI that is no longer a v2 candidate (``candidates``: the v2_kpis keys, decided by the seeds and the licence
    gate alone) is out of scope: its files are removed. A candidate whose ``series.json`` is published but was not
    built this run (missing table, read failure, no row at the shown frequency) is lost: the run must fail before
    writing, so a transient failure never takes a live KPI down. Other leftover files of a built KPI are removed."""
    stale, lost = [], set()
    for path in sorted(existing):
        if path in files:
            continue
        key = path.split("/")[1]
        if key in candidates and f"kpi/{key}/series.json" not in files:
            lost.add(key)
        else:
            stale.append(path)
    return stale, sorted(lost)


def _published(client, key: str) -> dict[str, Any] | None:
    """The JSON document published at key, None when there is none yet or it is not readable JSON (with a warning
    annotation); other storage errors (access denied, …) fail."""
    from botocore.exceptions import ClientError

    from ingestion.common.report import annotate

    try:
        body = get(client, key)
    except ClientError as exc:
        if str(exc.response.get("Error", {}).get("Code")) in ("404", "NoSuchKey", "NotFound"):
            return None
        raise
    try:
        return json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        annotate(
            "warning", "gold.publish: previous v2 file not readable, treated as absent", {"key": key, "error": str(exc)}
        )
        return None
