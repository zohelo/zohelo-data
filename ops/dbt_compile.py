"""Offline smoke check of the dbt project: compile every model and data test as production does and parse the SQL.

`dbt parse` neither renders model Jinja nor parses SQL, so a model with two selects and no `union all`, or a Jinja
error that shows only at compile time (a wrong macro argument, a missing ref, a macro's compiler error), would reach
the production build. This compiles every model with the target name `prod` (the namespaces and aliases of
generate_schema_name / generate_alias_name) and the `topics` var of warehouse/build.py, against in-memory DuckDB:
the R2 Data Catalog is replaced by an empty local database attached as `r2`, so no credentials or network are used.
The only stubs are empty tables for the Gold blocks, which poc_observations() looks up with load_relation, and for
the optional parents (config meta `optional_parents`), which poc_optional_ref() looks up the same way. Each
compiled SQL model or test must be exactly one statement that DuckDB's own parser accepts (nothing is bound or
run); each compiled Python model must parse. dbt writes its target and logs to a temporary directory.

repo_check runs it as the check `dbt compile`; on its own: `python -m ops.dbt_compile` (exit 1 on findings).
"""

from __future__ import annotations

import ast
import json
import os
from pathlib import Path
import re
import sys
import tempfile

ROOT = Path(__file__).resolve().parent.parent
PROJECT = ROOT / "warehouse"
OPTIONAL_PARENTS = "optional_parents"  # config meta of a model: parents read through poc_optional_ref()
BLOCK_TAG = "gold_block"  # the Gold blocks poc_observations() looks up (repo_check `configuration` keeps the list)
PROFILE = """zohelo:
  target: prod
  outputs:
    prod:
      type: duckdb
      path: ":memory:"
      threads: 4
      schema: main
      settings: {{autoinstall_known_extensions: false, autoload_known_extensions: false}}
      attach:
        - path: {catalog}
          alias: r2
"""


def problems() -> list[str]:
    """Compile every dbt model and data test offline; one line per node that fails to compile or to parse."""
    from dbt.cli.main import dbtRunner
    import duckdb
    import yaml

    topics = yaml.safe_load((PROJECT / "warehouse.yaml").read_text(encoding="utf-8"))["topics"]
    with tempfile.TemporaryDirectory(prefix="dbt_compile_") as tmp:
        catalog = Path(tmp) / "r2.duckdb"
        (Path(tmp) / "profiles.yml").write_text(PROFILE.format(catalog=json.dumps(str(catalog))), encoding="utf-8")
        os.environ["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
        flags = [
            "--project-dir", str(PROJECT),
            "--profiles-dir", tmp,
            "--target", "prod",
            "--target-path", str(Path(tmp) / "target"),
            "--log-path", str(Path(tmp) / "logs"),
            "--log-level", "none",
            "--log-level-file", "none",
            "--no-partial-parse",
            "--vars", json.dumps({"topics": topics}),
        ]  # fmt: skip
        parsed = dbtRunner().invoke(["parse", *flags])
        if not parsed.success:
            return [compile_error(str(parsed.exception))]
        manifest = parsed.result
        with duckdb.connect(str(catalog)) as con:
            optional = {
                name for node in manifest.nodes.values() for name in node.config.meta.get(OPTIONAL_PARENTS) or []
            }
            for node in manifest.nodes.values():
                if node.resource_type == "model" and (BLOCK_TAG in node.tags or node.name in optional):
                    con.execute(f'create schema if not exists "{node.schema}"')
                    con.execute(f'create table "{node.schema}"."{node.alias}" (stub integer)')
        select = ["--select", "resource_type:model", "resource_type:test"]
        compiled = dbtRunner(manifest=manifest).invoke(["compile", *flags, *select])
        if compiled.result is None:  # a compilation error stops dbt compile as a whole
            return [compile_error(str(compiled.exception))]
        found = []
        for result in compiled.result.results:
            path = f"warehouse/{result.node.original_file_path}"
            if result.node.resource_type == "test":
                path += f" (test {result.node.name})"
            if result.status != "success":
                found.append(f"{path}: {result.message}")
            elif result.node.language == "python":
                try:
                    ast.parse(result.node.compiled_code)
                except SyntaxError as exc:
                    found.append(f"{path}: compiled Python: {exc}")
            else:
                found += [f"{path}: {p}" for p in sql_problems(result.node.compiled_code, duckdb)]
        if not compiled.success and not found:
            found.append(f"dbt compile: {compiled.exception}")
        return sorted(found)


COMPILE_ERROR = re.compile(r"Error in (?:model|test|sql_operation) \S+ \((\S+)\)")


def compile_error(text: str) -> str:
    """dbt's compilation error as `<file>: <message>`, without the macro call chain."""
    match = COMPILE_ERROR.search(text)
    lines = [line.strip() for line in text.splitlines()[1:] if line.strip() and not line.strip().startswith(">")]
    if not match:
        return "dbt compile: " + " ".join(line.strip() for line in text.splitlines() if line.strip())[:1000]
    message = " ".join(line for line in lines if not COMPILE_ERROR.search(line))
    return f"warehouse/{match.group(1)}: {message[:1000]}"


def sql_problems(sql: str, duckdb: object) -> list[str]:
    """The compiled SQL is exactly one statement that DuckDB's parser accepts (parse only, no binding)."""
    try:
        statements = duckdb.extract_statements(sql)
    except duckdb.Error as exc:
        lines = [line.strip() for line in str(exc).splitlines() if line.strip().strip("^")]
        return [" | ".join(lines)]  # the message and the line it points at
    if len(statements) != 1:
        return [f"{len(statements)} statements, a model must be exactly one"]
    return []


def main() -> int:
    found = problems()
    for problem in found:
        print(problem)
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
