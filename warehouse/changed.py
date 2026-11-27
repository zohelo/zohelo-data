"""Which models the scheduled build must rebuild: only those whose inputs or code changed.

Owner policy #822: scheduled runs are incremental only. The daily build used to rewrite
every model in full (silver.bdl_observations alone ~229 M rows) although most Bronze
inputs change weekly at most. This plan is stateless: it reads Iceberg metadata only
(no data scan) and dbt's manifest from `dbt parse`.

After a successful build every model it built carries two table properties:
``zohelo.built_from_ms`` (when that build started) and ``zohelo.build_fingerprint``
(its SQL and config, the project's macros/dimensions/settings and requirements). The per-source
macro files (``macros/sources/``, docs/source-registration.md) count only for the models that use
them, so a new source's file rebuilds the Gold models that read it and what is built on them, not
every model of every source. A model is rebuilt when

- its table is missing or has no properties (never built by this rule, or the last build failed),
- its fingerprint differs (the code changed),
- an upstream model is rebuilt in this run, or
- an input changed after its build started: a Bronze source's current snapshot, or an
  upstream table's ``built_from_ms`` (its snapshot time if it has none, e.g. the
  Eurostat and BDL observations, written per entity by their loaders, #875).

An incremental model (materialized='incremental') is rebuilt
incrementally, except when its own code changed, it has no build properties or no table: then it is
listed in ``full_refresh`` and the build passes it as the dbt var ``full_refresh_models``
(the model's config turns that into a full refresh of that model only).

Per-block incremental models (#3204 PR B, D39; incremental_strategy ``gold_blocks``, macros/poc_incremental.sql): the
five facts and poc_dim_series replace only the Gold blocks listed in ``changed_blocks`` (the blocks rebuilt in this
run, and those changed after the model's last build) and delete the rows of blocks no longer registered. Their
fingerprint leaves out the block list and the per-source macro files (the "core"), so a new source is a block
change, not "code changed"; the block list they were built with is the property ``zohelo.build_blocks``, and a
different list ("blocks changed") rebuilds them incrementally. Once more than half of a table's counted rows are
position deletes ("position deletes"), it is rebuilt in full. Compaction is off (#875), so every replacement adds a
data file and delete files, and a small block replaced every run never reaches that share: the live data and delete
files after their last full build are the property ``zohelo.base_files``, and once more than FILE_GROWTH_LIMIT files
were added since ("file growth"), the table is rebuilt in full too. A failed run or test does not clear the
properties of an incremental table (it is the same table, unlike a swapped one): the next plan sees "input changed"
for the blocks newer than its last recorded build and replaces each of them again (``since``).

A model whose source table does not exist yet (a new source before its first Bronze load), directly or
through other models, is left out and listed under ``waiting``. A Gold block (a model tagged ``gold_block``,
listed by ``poc_block_models()``, macros/source_registration.sql) stops that: it waits itself, but the models that
union the blocks (facts, dimensions, metrics) are planned as usual and read only the blocks whose table
exists (``poc_observations()``), so a new source's block contributes zero rows until its first run instead
of holding back the whole Gold layer. Those blocks are listed again under ``empty_blocks``, but only when
their own table does not exist either. A model also does not wait for a parent it declares optional (config
meta ``optional_parents``, read through ``poc_optional_ref()``: the Silver tables the shared geography map reads
only for a source's own codes), so one source without Bronze cannot hold back the map and every block that
reads it (#2903).

A waiting model that already has a table is not a new source: an input it was built from is gone, or a new
input was added to it before that input's first load. It is listed under ``stalled`` with the missing source
tables, and the build stops before dbt runs (warehouse/build.py): such a model would otherwise stay frozen
without a word, and a stalled Gold block must never be passed on as an empty one.

Block-scoped metrics (#3204, kill switch ``metrics_scoped`` in warehouse.yaml): every ``poc_metrics_<kpi>`` model reads
the shared facts and dimensions, which union all Gold blocks, so one changed block used to rebuild all of them. A
metrics model whose concept's sources (seed_rows/kpi_measure) all map to Gold blocks (``block_sources()``) is
"scoped": its parents that derive from blocks (facts, dimensions, concept measures) are replaced by its scope blocks
and by the inputs of those parents that do not derive from blocks (kpi_measure, kpi, unit; not gold.coverage, whose
rows never reach a metrics row: OUTSIDE_METRICS, enforced by a data test). It is
rebuilt when one of those or one of its other parents was rebuilt in this run or changed after its build, not when
another source's block changed. Its fingerprint carries the code of the block-derived models above it (without the
per-source macro files and the block parents), its scope blocks and its sources' macro files, so an edit to a fact
model still rebuilds every metrics model and a new source rebuilds none of the others. A concept with a source that
maps to no block stays unscoped (the rule above).

Table compaction also makes a new snapshot, so a compacted input causes a rebuild:
conservative, never a missed change.
"""

from __future__ import annotations

from collections.abc import Iterable
import hashlib
import json
from pathlib import Path
import re
import subprocess
from typing import Any

import yaml

from warehouse import seed_rows

PROJECT = Path(__file__).resolve().parent
BLOCK_TAG = "gold_block"  # a Gold block: its children read it only when its table exists
OPTIONAL_PARENTS = "optional_parents"  # config meta: parents (model names) a model reads only when they exist
BUILT_FROM = "zohelo.built_from_ms"
FINGERPRINT = "zohelo.build_fingerprint"
# Code outside a model's own SQL that can change what it writes.
SHARED_CODE = ("macros", "dimensions", "dbt_project.yml", "../requirements.txt")
DOC_SUFFIXES = (".md",)  # documentation among the shared code (dimensions/README.md): in no fingerprint
# Per-source macro files (D34): left out of the shared digest, fingerprinted with the models that use them.
SOURCE_MACROS = "macros/sources"
# Per-source value-range declarations, read by the data test metrics_value_range only (no model calls them): left out of
# the shared digest like SOURCE_MACROS, but not given to the models that reach the registration macros, so a new or
# changed bound rebuilds nothing (a test has no build fingerprint; it runs whenever a model it reads is built).
VALUE_RANGES = "macros/value_range"
# The macros that find the per-source macros by name; dbt records no dependency for a macro called that way.
REGISTRATION = "macros/source_registration.sql"
# A model that reads a code list kept per source (config meta of warehouse/seed_rows.py) is fingerprinted with the
# list's files, as a seed is with its CSV: dbt's checksum of the model covers its SQL only.
SEED_ROWS = seed_rows.META
SOURCE_MACRO_PREFIX = "poc_"  # a macro of another name there could replace a dbt macro for every model
EXTERNAL_MODELS: frozenset[str] = frozenset()  # models built outside the scheduled build (none since #875)
# A Bronze commit stamped just before a build started may become visible only after dbt
# read that table; the recorded start is moved back by this margin (an extra rebuild at worst).
START_MARGIN_MS = 10 * 60 * 1000
# Rebuild reasons after which an incremental model's rows cannot be trusted to follow its code, or (position deletes)
# its table should be rewritten.
FULL_REFRESH_REASONS = ("no build properties", "code changed", "table missing", "position deletes", "file growth")
# Per-block incremental models (#3204 PR B, D39, macros/poc_incremental.sql): the facts and poc_dim_series replace
# only the Gold blocks the plan lists in ``changed_blocks``.
BLOCK_STRATEGY = "gold_blocks"  # their incremental_strategy
BUILD_BLOCKS = "zohelo.build_blocks"  # property: digest of the block models such a model was built with
# Position deletes pile up when blocks are replaced wholesale. In the snapshot summary total-records still counts
# the deleted rows, so the table is rewritten in full once more than half of its counted rows are deleted
# (total-position-deletes > total-records / 2; the spec's "deletes > records" can never fire, Dev 45750296222).
POSITION_DELETE_SHARE = 0.5
# Compaction is off (#875): each incremental replacement adds a data file and one or more delete files, and every scan
# of the table reads them all (R2 GETs). The live data + delete files right after a full build are recorded as
# BASE_FILES; more than FILE_GROWTH_LIMIT files added since then rebuilds the table in full ("file growth", #3204 B2).
BASE_FILES = "zohelo.base_files"
FILE_GROWTH_LIMIT = 64
# Block-scoped metrics (#3204): the models, the one macro call each must be, and where a block names its sources.
METRICS_PREFIX = "poc_metrics_"
CONCEPT_CALL = re.compile(r"\bpoc_concept_metrics\(\s*'([a-z0-9_]+)'\s*\)")
SOURCE_REGISTRATION = "poc_source__"  # macros/sources/<source>.sql: {'block_model': ...}
REGISTERED_MODEL = re.compile(r"['\"]block_model['\"]\s*:\s*['\"](\w+)['\"]")
SOURCE_SYSTEMS = "source_systems"  # config meta of a block that emits more source_systems than it is declared for
SCOPED_SWITCH = "metrics_scoped"  # warehouse.yaml; false = every metrics model reads all blocks (the rule before #3204)
# Sources (`<source>.<table>`) that a block-derived ancestor reads but whose rows never reach a metrics row, so they
# are not scope inputs (#3204 PR A2). gold.coverage adds only activated = false rows to poc_dim_measure, and the
# metrics read activated measures only. Not assumed: the data test warehouse/tests/gold_coverage_outside_metrics.sql
# fails the build when a coverage row could reach a metrics model, and ops/repo_check.py requires that test.
OUTSIDE_METRICS = ("gold_coverage.coverage",)
OUTSIDE_METRICS_TEST = "warehouse/tests/gold_coverage_outside_metrics.sql"


def metrics_scoped() -> bool:
    """The kill switch of block-scoped metrics: ``metrics_scoped`` in warehouse.yaml (absent = off)."""
    return bool((yaml.safe_load((PROJECT / "warehouse.yaml").read_text()) or {}).get(SCOPED_SWITCH, False))


def manifest(target: str = "prod") -> dict[str, Any]:
    """dbt's manifest of the project, freshly parsed (no database access)."""
    command = [
        "dbt",
        "parse",
        "--target",
        target,
        "--project-dir",
        str(PROJECT),
        "--profiles-dir",
        str(PROJECT),
        "--no-partial-parse",
    ]
    completed = subprocess.run(command, cwd=PROJECT, text=True, capture_output=True)
    if completed.returncode != 0:
        raise RuntimeError(f"dbt parse failed: {completed.stdout[-1500:]} {completed.stderr[-500:]}")
    return json.loads((PROJECT / "target" / "manifest.json").read_text())


def shared_digest() -> str:
    """One digest of the code every model shares; the files under SOURCE_MACROS and VALUE_RANGES are not in it.
    Each file enters as code_text(): an edit of comments or layout alone changes no fingerprint, so it rebuilds
    nothing (before 27 November any byte did, and a comment fix rebuilt every model)."""
    digest = hashlib.sha256()
    apart = {(PROJECT / SOURCE_MACROS).resolve(), (PROJECT / VALUE_RANGES).resolve()}
    for name in SHARED_CODE:
        root = (PROJECT / name).resolve()
        for path in sorted(root.rglob("*") if root.is_dir() else [root]):
            if path.is_file() and not apart & set(path.parents) and path.suffix not in DOC_SUFFIXES:
                digest.update(str(path.relative_to(root.parent)).encode() + b"\0" + code_text(path) + b"\0")
    return digest.hexdigest()


def code_text(path: Path) -> bytes:
    """What of a shared file can change what a model writes. SQL and Jinja (.sql): without Jinja comments and SQL
    `--` comments, every run of white space outside quotes as one space; text inside '…' and "…" is kept as it is
    (a value or a Jinja string). YAML: the parsed document (comments and layout drop out, key order stays).
    requirements.txt: the lines without `#` comments and surrounding white space. Anything else, or a file the
    reading does not fit (an unclosed quote or comment, YAML that does not parse), enters as its bytes."""
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw
    if path.suffix == ".sql":
        sql = sql_code(text)
        return raw if sql is None else sql.encode()
    if path.suffix in (".yml", ".yaml"):
        try:
            return json.dumps(yaml.safe_load(text), default=str).encode()
        except yaml.YAMLError:
            return raw
    if path.name == "requirements.txt":
        lines = (re.sub(r"(^|\s)#.*$", "", line).strip() for line in text.splitlines())
        return "\n".join(line for line in lines if line).encode()
    return raw


def without_jinja_comments(text: str) -> str | None:
    """The text as Jinja renders its comments: each {# … #} goes, even inside a SQL string, and so does the white
    space a `-` marker strips ({#- the white space before, -#} the white space after; dbt sets neither trim_blocks
    nor lstrip_blocks). White space that touches a comment stays, so `a{# c #}b` reads `ab` and `a {# c #}b` reads
    `a b`. Tags {{ … }} and {% … %} are copied as they are (a {# inside a Jinja string is no comment). None when a
    tag or a comment is not closed, or for a {% raw %} block, whose text Jinja does not read."""
    out: list[str] = []
    i, n = 0, len(text)
    while i < n:
        if text.startswith("{#", i):
            end = text.find("#}", i + 2)
            if end < 0:
                return None
            body = i + 3 if text.startswith("-", i + 2) else i + 2
            if body == i + 3:
                while out and out[-1].isspace():
                    out.pop()
            i = end + 2
            if end - 1 >= body and text[end - 1] == "-":
                while i < n and text[i].isspace():
                    i += 1
            continue
        if text.startswith(("{{", "{%"), i):
            close = "}}" if text[i + 1] == "{" else "%}"
            j, string = i + 2, ""
            while j < n and (string or not text.startswith(close, j)):
                c = text[j]
                j += 2 if string and c == "\\" else 1
                string = ("" if c == string else string) if string else (c if c in "'\"" else "")
            if j >= n:
                return None
            tag = text[i : j + 2]
            if close == "%}" and re.fullmatch(r"\{%[-+]?\s*raw\s*[-+]?%\}", tag):
                return None
            out.extend(tag)
            i = j + 2
            continue
        out.append(text[i])
        i += 1
    return "".join(out)


# sql_code() cases that ops/repo_check.py asserts (`configuration`, sql_code_cases): pairs that render different
# SQL must read differently, and constructs the reading does not follow must read None (the file counts by bytes).
# The first three pairs are the cases of the #3342 review; each pair read the same before this check existed.
SQL_CODE_DIFFER = [
    ("/* don't */ select 'a -- b' as x", "/* don't */ select 'a -- c' as x"),
    ("select a{# c #}b", "select a b"),
    ("select a {#- c -#} b", "select a b"),
    ("select -{# c #}- x\nfrom t", "select - - x\nfrom t"),
    ("select 'a''b' -- x\nfrom t", "select 'a''b' -- y\nfrom u"),
]
SQL_CODE_SAME = [
    ("select a {# c #} b -- note\nfrom t", "select   a b\n\nfrom t -- other note"),
    ("select a{# one #}b", "select a{# two #}b"),
    ("{%- set x = 1 -%}\nselect {{ x }}", "{%- set x = 1 -%} select {{  x  }}"),
]
SQL_CODE_NONE = [
    "/* don't */ select 'a -- b' as x",
    "select E'it\\'s -- b' as x",
    "select e'x' as x",
    "select $$ don't $$ as x",
    "select $t$ x $t$ as x",
    "{% raw %}{{ x }}{% endraw %}",
    "select 'open",
    "select {{ x",
    "select {# open",
]


def escape_string(text: str, at: int) -> bool:
    """The quote at `at` opens an E'…' string: an E or e right before it that is not the end of a longer name."""
    return at >= 1 and text[at - 1] in "eE" and not (at >= 2 and (text[at - 2].isalnum() or text[at - 2] == "_"))


def sql_code(text: str) -> str | None:
    """code_text() of SQL with Jinja, read as Jinja reads it first: without_jinja_comments(); a tag {{ … }} or
    {% … %} stays, with white space runs outside its Jinja strings as one space. The text between the tags is SQL:
    a `--` comment outside SQL quotes goes up to the line break (a tag inside it stays), and white space runs
    outside SQL quotes become one space. None (count the bytes) when a tag, a Jinja comment or a quote is not
    closed, and for SQL whose quotes this reading does not follow: a /* */ comment, a $ outside quotes (dollar
    quoting) and an E'…' string (backslash escapes); an apostrophe in any of them would flip the quote state and
    make a real change read as a comment. Asserted cases: SQL_CODE_DIFFER, SQL_CODE_SAME, SQL_CODE_NONE."""
    uncommented = without_jinja_comments(text)
    if uncommented is None:
        return None
    text = uncommented
    out: list[str] = []
    i, n, quote, comment, space = 0, len(text), "", False, False

    def put(piece: str) -> None:
        nonlocal space
        if space and out and not quote:
            out.append(" ")
        space = False
        out.append(piece)

    while i < n:
        if text.startswith(("{{", "{%"), i):
            close = "}}" if text[i + 1] == "{" else "%}"
            tag, j, string, gap = [text[i : i + 2]], i + 2, "", False
            while j < n and (string or not text.startswith(close, j)):
                c = text[j]
                if string:
                    tag.append(text[j : j + 2] if c == "\\" else c)
                    j += 2 if c == "\\" else 1
                    string = "" if c == string else string
                    continue
                if c.isspace():
                    gap, j = True, j + 1
                    continue
                if gap:
                    tag.append(" ")
                gap = False
                string = c if c in "'\"" else ""
                tag.append(c)
                j += 1
            if j >= n:
                return None
            put("".join(tag) + (" " if gap else "") + close)
            i = j + 2
            continue
        c = text[i]
        i += 1
        if comment:
            if c == "\n":
                comment, space = False, True
        elif quote:
            out.append(c)
            quote = "" if c == quote else quote
        elif c == "-" and text.startswith("-", i):
            comment, i = True, i + 1
        elif (c == "/" and text.startswith("*", i)) or c == "$" or (c == "'" and escape_string(text, i - 1)):
            return None
        elif c.isspace():
            space = True
        else:
            put(c)
            quote = c if c in "'\"" else ""
    return None if quote else "".join(out)


def source_macro_files(meta: dict[str, Any]) -> dict[str, list[str]]:
    """unique_id of a model or seed → the files under SOURCE_MACROS its SQL depends on (project-relative, sorted);
    pure (reads the manifest only).

    A model depends on such a file when it calls a macro defined there, directly or through other project
    macros, and on every such file when it reaches a macro of REGISTRATION, which calls the per-source macros by
    a name built at run time. Calls are taken from dbt (``depends_on.macros`` of the node and of each macro)
    and, because dbt does not record a call inside ``{% if execute %}`` or through ``context[...]``, also from the
    text: a per-source or registration macro whose name occurs as a word in the model's code or in a reached
    macro's code counts as called. A file there that defines a macro without SOURCE_MACRO_PREFIX belongs to
    every model. A file under VALUE_RANGES counts only for a model that calls one of its macros (none should)."""
    project = meta["metadata"]["project_name"]
    macros = {uid: m for uid, m in meta["macros"].items() if m["package_name"] == project}
    files = {uid: Path(m["original_file_path"]).as_posix() for uid, m in macros.items()}
    value_ranges = {uid for uid, f in files.items() if f.startswith(VALUE_RANGES + "/")}
    per_source = {uid for uid, f in files.items() if f.startswith(SOURCE_MACROS + "/")} | value_ranges
    registration = {uid for uid, f in files.items() if f == REGISTRATION}
    every = sorted({files[uid] for uid in per_source - value_ranges})
    loose = {files[uid] for uid in per_source if not macros[uid]["name"].startswith(SOURCE_MACRO_PREFIX)}
    named = {macros[uid]["name"]: uid for uid in per_source | registration}
    word = re.compile(r"\b(" + "|".join(map(re.escape, sorted(named))) + r")\b") if named else None

    def called(recorded: list[str], code: str) -> set[str]:
        found = {uid for uid in recorded if uid in macros}
        return found | ({named[name] for name in word.findall(code or "")} if word else set())

    calls = {
        uid: called(m.get("depends_on", {}).get("macros", []), m.get("macro_sql", "")) for uid, m in macros.items()
    }

    def reached(start: set[str]) -> set[str]:
        """The macros and every project macro they call, at any depth."""
        seen, queue = set(start), list(start)
        while queue:
            for callee in calls[queue.pop()] - seen:
                seen.add(callee)
                queue.append(callee)
        return seen

    out = {}
    for uid, node in meta["nodes"].items():
        if node.get("resource_type") not in ("model", "seed"):
            continue
        used = reached(called(node.get("depends_on", {}).get("macros", []), node.get("raw_code", "")))
        if used & registration:
            out[uid] = every
        else:
            out[uid] = sorted({files[m] for m in used & per_source} | loose)
    return out


def files_digest(files: list[str]) -> str:
    """Digest of project files by path and content; "" for none."""
    if not files:
        return ""
    digest = hashlib.sha256()
    for name in files:
        digest.update(name.encode() + b"\0" + (PROJECT / name).read_bytes() + b"\0")
    return digest.hexdigest()


def block_set(meta: dict[str, Any]) -> str:
    """Digest of the project's Gold block model names (tag gold_block): what a per-block model was built with."""
    names = sorted(
        node["name"]
        for node in meta["nodes"].values()
        if node.get("resource_type") == "model" and BLOCK_TAG in node.get("tags", [])
    )
    return hashlib.sha256("\n".join(names).encode()).hexdigest()[:32]


def models(meta: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """unique_id → name, namespace, table, parents (unique ids of models, seeds and sources), fingerprint, core
    (the fingerprint without per-source macro files and block parents), scope (metric_scopes(); None for others).
    Seeds are planned like models: their checksum is the CSV's, so an edited seed rebuilds itself
    and the models that ref it. A model that reads a code list kept per source (config meta SEED_ROWS) has the
    list's files in its fingerprint, so an added or edited file does the same."""
    shared = shared_digest()
    per_source = source_macro_files(meta)
    nodes = {**meta["nodes"], **meta["sources"]}
    blocks = {
        uid
        for uid, node in meta["nodes"].items()
        if node.get("resource_type") == "model" and BLOCK_TAG in node.get("tags", [])
    }
    out = {}
    for uid, node in meta["nodes"].items():
        if node.get("resource_type") not in ("model", "seed"):
            continue
        # Parents' resolved relations: repointing a source or ref to another table is a change.
        upstream = node.get("depends_on", {}).get("nodes", [])  # a seed lists none
        parents = sorted(".".join(relation(nodes[p])) for p in upstream if p in nodes)
        listed = node.get("config", {}).get("meta", {}).get(SEED_ROWS)
        rows = [path.relative_to(PROJECT).as_posix() for path in seed_rows.files(listed)] if listed else []
        code = json.dumps(
            [
                shared,
                node["checksum"]["checksum"],
                node.get("config", {}),
                parents,
                files_digest(per_source[uid] + rows),
            ],
            sort_keys=True,
            default=str,
        )
        # The code of the model without the per-source macro files and the block parents (#3204): what a scoped
        # metrics model takes from each block-derived model above it, so a new block changes none of them.
        core = json.dumps(
            [
                shared,
                node["checksum"]["checksum"],
                node.get("config", {}),
                sorted(".".join(relation(nodes[p])) for p in upstream if p in nodes and p not in blocks),
                files_digest(rows),
            ],
            sort_keys=True,
            default=str,
        )
        config = node.get("config", {})
        incremental = config.get("materialized") == "incremental"
        per_block = incremental and config.get("incremental_strategy") == BLOCK_STRATEGY
        out[uid] = {
            "name": node["name"],
            "namespace": relation(node)[0],
            "table": relation(node)[1],
            "parents": [p for p in upstream if p.startswith(("model.", "source.", "seed."))],
            # A per-block model's code is its core: a block added or removed is not "code changed" (a full refresh)
            # but a block change, replaced incrementally (block_set()).
            "fingerprint": hashlib.sha256((core if per_block else code).encode()).hexdigest()[:32],
            "core": hashlib.sha256(core.encode()).hexdigest()[:32],
            "incremental": incremental,
            "per_block": per_block,
            "block": uid in blocks,
            "optional": set(node.get("config", {}).get("meta", {}).get(OPTIONAL_PARENTS) or []),
            "scope": None,
        }
    for uid, scope in metric_scopes(meta, out).items():
        model = out[uid]
        model["scope"] = scope
        if scope is None:
            continue
        macro_files = [
            f"{SOURCE_MACROS}/{source}.sql"
            for source in scope["sources"]
            if (PROJECT / SOURCE_MACROS / f"{source}.sql").is_file()
        ]
        extra = json.dumps(
            [
                model["fingerprint"],
                sorted((out[a]["name"], out[a]["core"]) for a in scope["ancestors"]),
                sorted(out[b]["name"] for b in scope["blocks"]),
                files_digest(macro_files),
            ],
            sort_keys=True,
        )
        model["fingerprint"] = hashlib.sha256(extra.encode()).hexdigest()[:32]
    return out


def block_sources(meta: dict[str, Any]) -> dict[str, set[str]]:
    """source_system → the names of the Gold block models that emit it; pure (reads the manifest only).

    From each registration poc_source__<source>() (its 'block_model') and the config meta ``source_systems`` of a
    block that emits other source_systems too (poc_build_public_finance). A source under two blocks is returned
    with both (ops/repo_check.py forbids it)."""
    project = meta["metadata"]["project_name"]
    macros = {m["name"]: m.get("macro_sql", "") for m in meta["macros"].values() if m["package_name"] == project}
    out: dict[str, set[str]] = {}
    for name, sql in macros.items():
        found = REGISTERED_MODEL.search(sql) if name.startswith(SOURCE_REGISTRATION) else None
        if found:
            out.setdefault(name[len(SOURCE_REGISTRATION) :], set()).add(found.group(1))
    for node in meta["nodes"].values():
        if node.get("resource_type") == "model" and BLOCK_TAG in node.get("tags", []):
            for source in node.get("config", {}).get("meta", {}).get(SOURCE_SYSTEMS) or []:
                out.setdefault(source, set()).add(node["name"])
    return out


def outside_metrics(uid: str) -> bool:
    """True for a source node listed in OUTSIDE_METRICS (``source.<project>.<source>.<table>``)."""
    return uid.startswith("source.") and ".".join(uid.split(".")[-2:]) in OUTSIDE_METRICS


def concept_sources() -> dict[str, set[str]]:
    """kpi_key → the source_systems of its kpi_measure rows (seed_rows/kpi_measure)."""
    out: dict[str, set[str]] = {}
    for row in seed_rows.rows("kpi_measure"):
        out.setdefault(row["kpi_key"], set()).add(row["source_system"])
    return out


def metric_scopes(meta: dict[str, Any], graph: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any] | None]:
    """unique_id of every metrics model (``poc_metrics_*``) → its scope, or None when it stays unscoped (#3204).

    A scope is {"concept", "sources", "blocks": block unique ids, "ancestors": the block-derived models above it
    (reached without passing a block), "inputs": its parents and its ancestors' parents that neither are nor derive
    from a block, without the ancestors' OUTSIDE_METRICS sources}. Unscoped: the model is not exactly one
    poc_concept_metrics('<kpi>') call, the concept has no kpi_measure rows, or one of its sources maps to no block of
    the project."""
    by_source = block_sources(meta)
    sources_of = concept_sources()
    names = {m["name"]: uid for uid, m in graph.items()}
    derived: dict[str, bool] = {}

    def block_derived(uid: str) -> bool:
        if uid not in graph:
            return False
        if uid not in derived:
            derived[uid] = False  # a cycle cannot occur in dbt; this only guards the recursion
            derived[uid] = graph[uid]["block"] or any(block_derived(p) for p in graph[uid]["parents"])
        return derived[uid]

    out: dict[str, dict[str, Any] | None] = {}
    for uid, model in graph.items():
        if not model["name"].startswith(METRICS_PREFIX) or model["block"]:
            continue
        out[uid] = None
        calls = CONCEPT_CALL.findall(meta["nodes"][uid].get("raw_code", ""))
        if len(calls) != 1:
            continue
        sources = sources_of.get(calls[0], set())
        blocks = {names.get(b) for s in sources for b in by_source.get(s, ())}
        if not sources or any(not by_source.get(s) for s in sources) or None in blocks:
            continue
        if not all(graph[b]["block"] for b in blocks):
            continue
        ancestors: set[str] = set()
        inputs: set[str] = set()
        queue = [uid]
        while queue:
            child = queue.pop()
            for p in graph[child]["parents"]:
                if p in graph and graph[p]["block"]:
                    continue
                if block_derived(p):
                    if p not in ancestors:
                        ancestors.add(p)
                        queue.append(p)
                elif child == uid or not outside_metrics(p):  # read by the metrics model itself: always an input
                    inputs.add(p)
        out[uid] = {
            "concept": calls[0],
            "sources": sorted(sources),
            "blocks": sorted(blocks),
            "ancestors": sorted(ancestors),
            "inputs": sorted(inputs),
        }
    return out


def relation(node: dict[str, Any]) -> tuple[str, str]:
    """(namespace, table) of a model or source node."""
    if node.get("resource_type") == "source":
        return node["schema"], node.get("identifier") or node["name"]
    return node["schema"], node.get("alias") or node["name"]


def table_state(cat, namespace: str, table: str) -> dict[str, Any] | None:
    """Current snapshot time and build properties of a table; None when it does not exist."""
    from pyiceberg.exceptions import NoSuchTableError

    try:
        loaded = cat.load_table((namespace, table))
    except NoSuchTableError:
        return None
    snapshot = loaded.current_snapshot()
    props = loaded.properties
    summary = snapshot.summary if snapshot and snapshot.summary else None
    return {
        "snapshot_ms": snapshot.timestamp_ms if snapshot else 0,
        "built_from_ms": int(props[BUILT_FROM]) if BUILT_FROM in props else None,
        "fingerprint": props.get(FINGERPRINT),
        "build_blocks": props.get(BUILD_BLOCKS),
        "records": int(summary["total-records"] or 0) if summary else 0,
        "position_deletes": int(summary["total-position-deletes"] or 0) if summary else 0,
        "files": live_files(summary),
        "base_files": int(props[BASE_FILES]) if BASE_FILES in props else None,
    }


def live_files(summary: Any) -> int | None:
    """Live data + delete files of a snapshot summary; None when the summary does not count them."""
    if not summary or summary["total-data-files"] is None:
        return None
    return int(summary["total-data-files"]) + int(summary["total-delete-files"] or 0)


def file_growth(own: dict[str, Any]) -> bool:
    """True when more than FILE_GROWTH_LIMIT data + delete files were added since the last full build."""
    files, base = own.get("files"), own.get("base_files")
    return files is not None and base is not None and files - base > FILE_GROWTH_LIMIT


def plan(cat, meta: dict[str, Any], scoped: bool | None = None) -> dict[str, Any]:
    """{"select": model names to build, "reasons": name → why, "unchanged": count,
    "full_refresh": incremental models among them to rebuild in full, "changed_blocks": the Gold blocks the
    per-block incremental models replace (the dbt var changed_blocks), "waiting": models left out because a
    source table they read does not exist yet, "empty_blocks": the Gold blocks among them that have no table
    either, read as zero rows, "stalled": waiting model → the missing source tables, for every waiting model
    that has a table (the build must stop on any), "waiting_tests": the data tests that read a waiting model
    (waiting_tests; the build leaves them out), "scoped": whether metrics models were block-scoped,
    "scoped_skipped": the models the unscoped rule would also rebuild}. ``scoped``: None reads the kill switch
    (metrics_scoped()). Both rules use the same table states: the comparison costs no catalog read."""
    graph = models(meta)
    relations = {uid: (m["namespace"], m["table"]) for uid, m in graph.items()}
    for uid in {p for m in graph.values() for p in m["parents"] if p.startswith("source.")}:
        relations[uid] = relation(meta["sources"][uid])
    state = {uid: table_state(cat, *rel) for uid, rel in relations.items()}

    def updated(uid: str) -> int:
        s = state[uid]
        if s is None:
            return 0  # a missing source cannot have changed since; dbt will fail on it if read
        return s["built_from_ms"] if s["built_from_ms"] is not None else s["snapshot_ms"]

    blocked: dict[str, bool] = {}

    def waiting(uid: str) -> bool:
        """True when a source table this model reads (directly or through other models) does not exist yet:
        a newly onboarded source whose first Bronze load has not run. Building it would fail every pipeline's
        build, so it waits and is listed under "waiting". A waiting Gold block does not make its children
        wait: they skip a block whose table does not exist (poc_observations). Nor does a parent the model
        declares optional (poc_optional_ref)."""
        if uid not in blocked:
            blocked[uid] = bool(missing_sources(uid))
        return blocked[uid]

    missing: dict[str, set[str]] = {}

    def missing_sources(uid: str) -> set[str]:
        """The source tables that do not exist and make this model wait (through other models too)."""
        if uid not in missing:
            model = graph[uid]
            found = {p for p in model["parents"] if p.startswith("source.") and state.get(p) is None}
            for p in model["parents"]:
                if p in graph and not graph[p]["block"] and graph[p]["name"] not in model["optional"]:
                    found |= missing_sources(p)
            missing[uid] = found
        return missing[uid]

    def rebuild(scope_metrics: bool) -> dict[str, str]:
        """name → why, for every model to rebuild; scope_metrics: scoped metrics models read their scope only."""
        reasons: dict[str, str] = {}
        done: dict[str, bool] = {}

        def stale(uid: str) -> bool:
            if uid in done:
                return done[uid]
            model, own = graph[uid], state[uid]
            if waiting(uid):
                done[uid] = False
                return False
            scope = model["scope"] if scope_metrics else None
            # A scoped metrics model: its block-derived parents stand for their blocks; only its own blocks count.
            inputs = model["parents"] if scope is None else scope["inputs"] + scope["blocks"]
            upstream = [p for p in inputs if p in graph and stale(p)]
            reason = None
            if model["name"] in EXTERNAL_MODELS:
                reason = None
            elif own is None:
                reason = "table missing"
            elif own["built_from_ms"] is None or own["fingerprint"] is None:
                reason = "no build properties"
            elif own["fingerprint"] != model["fingerprint"]:
                reason = "code changed"
            elif model["per_block"] and own.get("position_deletes", 0) > POSITION_DELETE_SHARE * own.get("records", 0):
                reason = "position deletes"
            elif model["per_block"] and file_growth(own):
                reason = "file growth"
            elif upstream:
                reason = "upstream rebuilt: " + ", ".join(graph[p]["name"] for p in upstream[:3])
            else:
                newer = [p for p in inputs if updated(p) > own["built_from_ms"]]
                if newer:
                    reason = "input changed: " + ", ".join(".".join(relations[p]) for p in newer[:3])
                elif model["per_block"] and own.get("build_blocks") != blocks_now:
                    reason = "blocks changed"  # a block removed (an added one is rebuilt itself: upstream rebuilt)
            if reason:
                reasons[model["name"]] = reason
            done[uid] = reason is not None
            return done[uid]

        for uid in graph:
            stale(uid)
        return reasons

    blocks_now = block_set(meta)
    use_scope = metrics_scoped() if scoped is None else scoped
    reasons = rebuild(use_scope)
    skipped = sorted(set(rebuild(False)) - set(reasons)) if use_scope else []
    waits = sorted(graph[u]["name"] for u, b in blocked.items() if b)
    full = sorted(
        m["name"]
        for m in graph.values()
        if m["incremental"] and reasons.get(m["name"], "").partition(":")[0] in FULL_REFRESH_REASONS
    )
    # The blocks the per-block models replace: each block rebuilt in this run, and each block whose table changed
    # after the last build of a per-block model rebuilt incrementally (its rows there may be older). Replacing a
    # block that did not change rewrites the same rows, so one list serves every such model.
    incremental_now = [u for u, m in graph.items() if m["per_block"] and m["name"] in reasons and m["name"] not in full]
    since = min((state[u]["built_from_ms"] for u in incremental_now), default=None)
    changed_blocks = sorted(
        m["name"]
        for u, m in graph.items()
        if m["block"]
        and not blocked.get(u)
        and (m["name"] in reasons or (since is not None and state[u] is not None and updated(u) > since))
    )
    return {
        "select": sorted(reasons),
        "reasons": reasons,
        "unchanged": len(graph) - len(reasons) - sum(blocked.values()),
        "full_refresh": full,
        "changed_blocks": changed_blocks,
        "scoped": use_scope,
        "scoped_skipped": skipped,
        "waiting": waits,
        "waiting_tests": sorted(waiting_tests(meta, waits).values()),
        "empty_blocks": sorted(
            graph[u]["name"] for u, b in blocked.items() if b and graph[u]["block"] and state[u] is None
        ),
        "stalled": {
            graph[u]["name"]: sorted(".".join(relations[p]) for p in missing[u])
            for u, b in sorted(blocked.items(), key=lambda item: graph[item[0]]["name"])
            if b and state[u] is not None
        },
    }


def waiting_tests(meta: dict[str, Any], waiting: Iterable[str]) -> dict[str, str]:
    """unique_id → name of every data test that reads a model named in ``waiting``; pure.

    dbt selects a test as soon as one of its parents is selected (eager indirect selection), so a test that joins
    a waiting model to a shared one (a relationships test to a reference table, a singular test over several
    sources) would run when the shared model is rebuilt, against a table that does not exist yet (#2948)."""
    names = set(waiting)
    held = {
        uid for uid, node in meta["nodes"].items() if node.get("resource_type") == "model" and node["name"] in names
    }
    return {
        uid: node["name"]
        for uid, node in meta["nodes"].items()
        if node.get("resource_type") == "test" and held & set(node.get("depends_on", {}).get("nodes", []))
    }


TEST_OK = ("pass", "warn")  # dbt test statuses that leave a model trusted


def record(
    cat,
    meta: dict[str, Any],
    results: Iterable[dict[str, Any]],
    started_ms: int,
    full_refresh: Iterable[str] = (),
) -> dict[str, Any]:
    """Set the build properties on each model the build wrote whose own tests all passed (dbt
    run results). A table model with a failing test, or whose property write fails, keeps no
    properties (the build swapped in a new table) and is rebuilt (and tested) next time; write failures are reported,
    not raised. An incremental table keeps its old properties instead, so the next plan finds its inputs newer than
    its last recorded build ("input changed") and, for a per-block model, replaces those blocks again.
    ``full_refresh``: the plan's full refreshes; a per-block model among them (or one without the property yet)
    records its live files as BASE_FILES, the baseline of "file growth".
    A `warn` result is a pass at the severity the project declared (e.g. bdl_observations.year
    not_null warns on every build), so only `fail`, `error`, `skipped` and unknown statuses
    make a model untrusted."""
    results = list(results)
    full_refresh = set(full_refresh)
    graph = {m["name"]: m for m in models(meta).values()}
    blocks_now = block_set(meta)
    failed_tests = {
        r["unique_id"] for r in results if r["unique_id"].startswith("test.") and r["status"] not in TEST_OK
    }
    untrusted = {
        p.split(".")[-1] for t in failed_tests for p in meta["nodes"].get(t, {}).get("depends_on", {}).get("nodes", [])
    }
    built = [
        r["unique_id"].split(".")[-1]
        for r in results
        if r["unique_id"].startswith(("model.", "seed.")) and r["status"] == "success"
    ]
    recorded, failed = 0, []
    for name in built:
        model = graph.get(name)
        if model is None or name in EXTERNAL_MODELS or name in untrusted:
            continue
        try:
            table = cat.load_table((model["namespace"], model["table"]))
            props = {BUILT_FROM: str(started_ms - START_MARGIN_MS), FINGERPRINT: model["fingerprint"]}
            if model["per_block"]:
                props[BUILD_BLOCKS] = blocks_now
                snapshot = table.current_snapshot()
                files = live_files(snapshot.summary if snapshot else None)
                if files is not None and (name in full_refresh or BASE_FILES not in table.properties):
                    props[BASE_FILES] = str(files)
            with table.transaction() as tx:
                tx.set_properties(props)
            recorded += 1
        except Exception as exc:  # any failure only means a rebuild next run
            failed.append(f"{name}: {type(exc).__name__}: {str(exc)[:200]}")
    return {"recorded": recorded, "failed": failed[:5]}
