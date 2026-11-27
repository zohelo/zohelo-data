#!/usr/bin/env python3
"""Bilingual measure names through the Message Batches API (owner request of 27 Nov 2026, decision O47).

Run by the workflow ``batch-labels.yml``, not by Ops: that workflow alone has the ``ANTHROPIC_API_KEY`` secret
and installs ``anthropic`` (pinned with hashes in ``ops/measure_names.requirements.txt``, not in requirements.txt).

``submit``: reads ``gold.dim_measure`` read-only (the read credentials, ``READ_ONLY`` attach), keeps the measures
with status ``activated`` or ``waiting`` (``--scope all``: every measure), sends them in chunks of 40 measures of
one source per request, and writes the batch id and the manifest (custom_id → measure keys, measure key → source)
to ``--out``. Writes nothing to R2.

``collect``: reads that manifest, stops with exit code 3 while the batch has not ended, otherwise validates the
results (every sent measure_key answered exactly once, a tool call with the expected fields, names of at most
90 characters, ``unclear`` a boolean) and writes ``warehouse/seed_rows/measure_name/<source_system>.csv``
(measure_key,name_pl,name_en,unclear; D36 folder pattern). Rows already in a file and not in this batch stay.
A report for the job summary and the pull request body goes to ``--out``. Exit code 0 when every measure passed,
1 when some did not (their rows are left out and listed), 2 for bad arguments, a missing secret or any
unexpected error (an API error, a bad manifest): the workflow then opens no pull request, whatever was written.

Args: ``submit --scope activated|all --model <id> --out DIR``; ``collect --manifest FILE --out DIR``.
"""

from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import sys
import traceback

from ingestion.common.report import annotate, step_summary

CHUNK = 40
MAX_NAME = 90
MAX_TOKENS = 8192
STATUSES = ("activated", "waiting")  # --scope activated (the catalogue's live and next measures)
COLUMNS = ("measure_key", "source_system", "block_id", "block_name", "measure_name", "unit_key", "status")
SEED_FOLDER = Path("warehouse/seed_rows/measure_name")
SEED_HEADER = ["measure_key", "name_pl", "name_en", "unclear"]
TOOL = "record_measure_names"

SYSTEM = f"""You name statistical measures for zohelo, a Polish public-data catalogue (GUS, Eurostat, NBP, \
ministries, international organisations). Each measure is one series definition of one source: its source \
system, the dataset or block it belongs to, the source's own name or code for it, and its unit.

For every measure write:
- name_pl: a short, clear Polish name a non-specialist understands, in sentence case, with no source codes, \
no unit and no geography or period (those are shown separately). Keep the source's statistical meaning exactly: \
do not widen or narrow it, and keep breakdown words (sex, age, sector, price base, seasonal adjustment) that \
distinguish it from its neighbours in the same block.
- name_en: the same name in English.
- unclear: true when the given text does not let you tell what the measure counts (an opaque code, a truncated \
or ambiguous label); still give your best name. Otherwise false.
Both names at most {MAX_NAME} characters. Answer every measure_key you are given exactly once, by calling the \
tool {TOOL} once."""

TOOL_SPEC = {
    "name": TOOL,
    "description": "Record the Polish and English names of the given measures.",
    "input_schema": {
        "type": "object",
        "properties": {
            "measures": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "measure_key": {"type": "string"},
                        "name_pl": {"type": "string", "maxLength": MAX_NAME},
                        "name_en": {"type": "string", "maxLength": MAX_NAME},
                        "unclear": {"type": "boolean"},
                    },
                    "required": ["measure_key", "name_pl", "name_en", "unclear"],
                },
            }
        },
        "required": ["measures"],
    },
}


def api_key() -> str:
    """The Anthropic API key, or exit 2 with a message saying who adds it."""
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not key:
        annotate(
            "error",
            "ANTHROPIC_API_KEY missing",
            "The repository secret ANTHROPIC_API_KEY is not set. The owner adds it (Settings → Secrets and "
            "variables → Actions); batch-labels.yml cannot run without it.",
        )
        sys.exit(2)
    return key


def client():
    """An Anthropic client; ``anthropic`` is installed only by batch-labels.yml."""
    key = api_key()
    import anthropic

    return anthropic.Anthropic(api_key=key, max_retries=5)


def read_measures() -> list[dict]:
    """Every row of gold.dim_measure, read-only through DuckDB's Iceberg attach (read credentials)."""
    import duckdb

    from ingestion.common import r2

    con = duckdb.connect(":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")
    endpoint = r2.req("STORAGE_ENDPOINT_URL").replace("https://", "").strip("/")
    con.execute("CREATE SECRET r2_catalog (TYPE iceberg, TOKEN ?)", [r2.req("STORAGE_LAKEHOUSE_CATALOG_READ_TOKEN")])
    con.execute(
        "CREATE SECRET r2_files (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION 'auto', URL_STYLE 'path')",
        [r2.req("STORAGE_LAKEHOUSE_READ_ACCESS_KEY"), r2.req("STORAGE_LAKEHOUSE_READ_SECRET_KEY"), endpoint],
    )
    con.execute(
        "ATTACH ? AS r2 (TYPE iceberg, SECRET r2_catalog, ENDPOINT ?, READ_ONLY)",
        [r2.req("STORAGE_LAKEHOUSE_CATALOG_NAME"), r2.req("STORAGE_LAKEHOUSE_CATALOG_URL")],
    )
    have = [r[0] for r in con.execute("describe r2.gold.dim_measure").fetchall()]
    missing = {"measure_key", "source_system", "status"} - set(have)
    if missing:
        raise RuntimeError(f"gold.dim_measure has no column {sorted(missing)}")
    want = [c for c in COLUMNS if c in have]
    rows = con.execute(f"select {', '.join(want)} from r2.gold.dim_measure order by measure_key").fetchall()
    return [dict(zip(want, row, strict=True)) for row in rows]


def select(rows: list[dict], scope: str) -> list[dict]:
    """The measures of the scope, ordered by source, block and key so a chunk holds neighbours."""
    kept = rows if scope == "all" else [r for r in rows if str(r.get("status")) in STATUSES]
    return sorted(kept, key=lambda r: (str(r["source_system"]), str(r.get("block_id") or ""), str(r["measure_key"])))


def chunks(rows: list[dict], size: int = CHUNK) -> list[list[dict]]:
    """Chunks of at most ``size`` measures, never mixing two sources."""
    out: list[list[dict]] = []
    for row in rows:
        if not out or len(out[-1]) >= size or out[-1][0]["source_system"] != row["source_system"]:
            out.append([])
        out[-1].append(row)
    return out


def build_requests(rows: list[dict], model: str) -> tuple[list[dict], dict]:
    """The batch requests and the manifest: custom_id → measure keys, measure key → source system."""
    requests, keys, owner = [], {}, {}
    for i, chunk in enumerate(chunks(rows), start=1):
        custom_id = f"c{i:05d}"
        items = [
            {c: (None if r.get(c) is None else str(r[c])) for c in COLUMNS if c in r and c != "status"} for r in chunk
        ]
        keys[custom_id] = [str(r["measure_key"]) for r in chunk]
        owner.update({str(r["measure_key"]): str(r["source_system"]) for r in chunk})
        requests.append(
            {
                "custom_id": custom_id,
                "params": {
                    "model": model,
                    "max_tokens": MAX_TOKENS,
                    "system": SYSTEM,
                    "tools": [TOOL_SPEC],
                    "tool_choice": {"type": "tool", "name": TOOL},
                    "messages": [
                        {
                            "role": "user",
                            "content": "Measures (JSON lines):\n"
                            + "\n".join(json.dumps(x, ensure_ascii=False) for x in items),
                        }
                    ],
                },
            }
        )
    return requests, {"chunks": keys, "sources": owner}


def submit(args) -> int:
    """Send the batch; write the manifest and the job summary."""
    api = client()  # fails first when the key is missing, before reading R2
    rows = select(read_measures(), args.scope)
    if not rows:
        annotate("error", "measure names", {"scope": args.scope, "measures": 0})
        return 1
    requests, manifest = build_requests(rows, args.model)
    batch = api.messages.batches.create(requests=requests)
    manifest |= {
        "batch_id": batch.id,
        "model": args.model,
        "scope": args.scope,
        "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "measures": len(rows),
        "requests": len(requests),
    }
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    by_source: dict[str, int] = {}
    for r in rows:
        by_source[str(r["source_system"])] = by_source.get(str(r["source_system"]), 0) + 1
    annotate(
        "notice",
        "measure names batch",
        {k: manifest[k] for k in ("batch_id", "model", "scope", "measures", "requests")},
    )
    step_summary(
        f"## Measure names batch submitted\n\nBatch id: `{batch.id}`\n\nModel `{args.model}`, scope `{args.scope}`: "
        f"{len(rows)} measures of {len(by_source)} sources in {len(requests)} requests (chunks of {CHUNK}).\n\n"
        "Run this workflow again with step `collect` once the batch has ended (usually within an hour, at most 24 h). "
        "The manifest is the artifact `measure-names-" + batch.id + "`.\n"
    )
    return 0


def tool_input(message) -> dict | None:
    """The input of the tool call in a result message, None when there is none."""
    for block in getattr(message, "content", None) or []:
        if getattr(block, "type", None) == "tool_use" and getattr(block, "name", None) == TOOL:
            return block.input if isinstance(block.input, dict) else None
    return None


def check_result(custom_id: str, sent: list[str], answer: dict | None) -> tuple[dict[str, dict], list[str]]:
    """Valid rows by measure key and the problems of one request's answer; pure."""
    if answer is None or not isinstance(answer.get("measures"), list):
        return {}, [f"{custom_id}: no {TOOL} call with a measures list"]
    rows, problems, seen = {}, [], set()
    for item in answer["measures"]:
        key = item.get("measure_key") if isinstance(item, dict) else None
        if key not in sent:
            problems.append(f"{custom_id}: answered a measure that was not sent: {str(key)[:120]}")
            continue
        if key in seen:
            problems.append(f"{key}: answered twice")
            rows.pop(key, None)
            continue
        seen.add(key)
        names = [item.get("name_pl"), item.get("name_en")]
        if not all(isinstance(n, str) and n.strip() for n in names):
            problems.append(f"{key}: empty or missing name")
        elif any(len(n.strip()) > MAX_NAME for n in names):
            problems.append(f"{key}: name longer than {MAX_NAME} characters")
        elif not isinstance(item.get("unclear"), bool):
            problems.append(f"{key}: unclear is not true or false")
        else:
            rows[key] = {"name_pl": names[0].strip(), "name_en": names[1].strip(), "unclear": item["unclear"]}
    problems += [f"{key}: not answered" for key in sent if key not in seen]
    return rows, problems


def write_seed_rows(
    rows: dict[str, dict], owner: dict[str, str], folder: Path, declared: set[str]
) -> tuple[dict[str, int], list[str]]:
    """Merge the rows into ``<folder>/<source_system>.csv`` (existing rows of other keys stay); rows of a source
    without a declaration are not written (repo_check accepts only declared sources' files)."""
    by_source: dict[str, dict[str, dict]] = {}
    skipped = sorted({owner[k] for k in rows if owner[k] not in declared})
    for key, row in rows.items():
        if owner[key] in declared:
            by_source.setdefault(owner[key], {})[key] = row
    folder.mkdir(parents=True, exist_ok=True)
    written = {}
    for source, new in sorted(by_source.items()):
        path = folder / f"{source}.csv"
        merged: dict[str, dict] = {}
        if path.exists():
            with path.open(encoding="utf-8", newline="") as handle:
                merged = {r["measure_key"]: r for r in csv.DictReader(handle)}
        for key, row in new.items():
            merged[key] = {"measure_key": key, **row, "unclear": "true" if row["unclear"] else "false"}
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=SEED_HEADER, lineterminator="\n")
            writer.writeheader()
            writer.writerows(merged[k] for k in sorted(merged))
        written[source] = len(new)
    return written, [f"source {s} has no declaration in ingestion/sources/: its rows are not written" for s in skipped]


def report(
    manifest: dict, written: dict[str, int], unclear: list[tuple[str, str, str]], problems: list[str], limit: int = 150
) -> str:
    """Markdown for the job summary and the pull request body."""
    lines = [
        f"Batch `{manifest['batch_id']}`, model `{manifest['model']}`, scope `{manifest['scope']}`, "
        f"submitted {manifest['created_at']}: {manifest['measures']} measures in {manifest['requests']} requests.",
        "",
        f"**Written:** {sum(written.values())} rows in {len(written)} files of `{SEED_FOLDER}/`.",
        f"**Unclear:** {len(unclear)} (named, marked `unclear=true`).",
        f"**Problems:** {len(problems)} (those measures are not written).",
    ]
    if written:
        lines += ["", "| source_system | rows |", "|---|---|"] + [f"| {s} | {n} |" for s, n in sorted(written.items())]
    if unclear:
        lines += ["", "### Unclear", "", "| measure_key | name_pl | name_en |", "|---|---|---|"]
        lines += [f"| `{k}` | {pl.replace('|', '/')} | {en.replace('|', '/')} |" for k, pl, en in unclear[:limit]]
        if len(unclear) > limit:
            lines.append(f"\n…and {len(unclear) - limit} more (`unclear=true` in the files).")
    if problems:
        lines += ["", "### Problems", ""] + [f"- {p}" for p in problems[:limit]]
        if len(problems) > limit:
            lines.append(f"- …and {len(problems) - limit} more (job log).")
    return "\n".join(lines) + "\n"


def collect(args) -> int:
    """Validate the ended batch and write the seed rows."""
    from warehouse import declared

    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    api = client()
    batch = api.messages.batches.retrieve(manifest["batch_id"])
    if batch.processing_status != "ended":
        counts = batch.request_counts
        annotate(
            "error",
            "batch not ended",
            f"Batch {batch.id} is {batch.processing_status}: {counts.processing} of {manifest['requests']} requests "
            f"still processing. Run step collect again later (a batch ends within 24 h of {manifest['created_at']}).",
        )
        return 3
    rows, problems = {}, []
    answered = set()
    for item in api.messages.batches.results(batch.id):
        sent = manifest["chunks"].get(item.custom_id)
        if sent is None:
            problems.append(f"{item.custom_id}: not in the manifest")
            continue
        answered.add(item.custom_id)
        if item.result.type != "succeeded":
            error = getattr(getattr(item.result, "error", None), "error", None)
            problems.append(
                f"{item.custom_id}: {item.result.type} {getattr(error, 'message', '')}".strip()
                + f" ({len(sent)} measures)"
            )
            continue
        if item.result.message.stop_reason == "max_tokens":
            problems.append(f"{item.custom_id}: answer cut at max_tokens")
        good, bad = check_result(item.custom_id, sent, tool_input(item.result.message))
        rows |= good
        problems += bad
    problems += [
        f"{cid}: no result ({len(keys)} measures)" for cid, keys in manifest["chunks"].items() if cid not in answered
    ]
    written, skipped = write_seed_rows(rows, manifest["sources"], Path(args.seed_folder), set(declared.sources()))
    problems += skipped
    unclear = sorted((k, r["name_pl"], r["name_en"]) for k, r in rows.items() if r["unclear"])
    text = report(manifest, written, unclear, problems)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "report.md").write_text(text, encoding="utf-8")
    step_summary("## Measure names collected\n\n" + text)
    annotate(
        "warning" if problems else "notice",
        "measure names collected",
        {
            "batch_id": batch.id,
            "rows": sum(written.values()),
            "files": len(written),
            "unclear": len(unclear),
            "problems": len(problems),
        },
    )
    return 1 if problems else 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    steps = parser.add_subparsers(dest="step", required=True)
    s = steps.add_parser("submit")
    s.add_argument("--scope", choices=["activated", "all"], default="activated")
    s.add_argument("--model", required=True)
    s.add_argument("--out", required=True)
    c = steps.add_parser("collect")
    c.add_argument("--manifest", required=True)
    c.add_argument("--out", required=True)
    c.add_argument("--seed-folder", default=str(SEED_FOLDER))
    args = parser.parse_args(argv)
    if args.step == "submit":
        return submit(args)
    try:
        return collect(args)
    except Exception as exc:  # exit 1 ("some measures left out") opens a pull request; an unplanned error must not
        traceback.print_exc()
        annotate("error", "collect failed", f"{type(exc).__name__}: {exc}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
