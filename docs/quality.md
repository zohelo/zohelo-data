# Quality rules

Every change to `main` meets these rules. They exist so the platform stays small, readable
and runnable by a person or another AI when Claude is not available. Rules marked
**[auto]** are enforced by the `check` job (or will be, see *Enforcement*); rules marked
**[review]** are checked by the independent reviewer before merge.

## 1. Principles

1. **Two layers.** What the owner reads or changes is declared (YAML, SQL, Markdown):
   sources, schedules, retention, models, metrics, dimensions, catalog, portal config.
   Python and workflows are the engine. New behaviour that someone may want to adjust is
   a declaration, not a constant in code.
2. **General → detailed.** Every folder's README and every doc starts with what it is for
   and how it fits the dataflow, then the details.
3. **Small and single-purpose.** One file, one job. Delete what is not used; no
   commented-out code, no "just in case" options.
4. **Reproducible and idempotent.** Every activity can be re-run and resumes; results
   depend only on inputs and declared configuration.
5. **Nothing silent.** Failures stop the run or are recorded with a reason (rejection
   tables, annotations). No bare `except`, no swallowed errors.

## 2. Repository hygiene

| Rule | Check |
|---|---|
| Root contains only: `.github/`, `docs/`, `ingestion/`, `ops/`, `portal/`, `warehouse/`, `run.py`, `requirements.txt`, `pyproject.toml` (tool settings), `README.md`, `AGENTS.md`, `CLAUDE.md`, `GEMINI.md`, `.gitignore`, `.gitattributes`, `.python-version`, `.node-version` | [auto] allowlist |
| No empty files except `__init__.py` | [auto] |
| No file above 1 MB; no data, outputs, notebooks, archives or credentials committed | [auto] |
| No `dev/` on `main`; no status/audit/log files in the repo (status lives in project docs) | [auto] |
| No secrets in code or history | [auto] gitleaks |
| Names follow AGENTS.md (activities `<stage>.<verb>`, sources by `source_system`, snake_case files) | [review] |

## 3. Python (engine layer)

| Rule | Check |
|---|---|
| Compiles; ruff lint clean with rule sets E, F, W, I, B, UP, SIM, RUF (settings in `pyproject.toml`) | [auto] |
| ruff format (line length 120) | [auto] |
| Public functions have type hints and a one-line docstring; modules a docstring saying what they do in the dataflow | [review] |
| Configuration comes from declarations or environment variables, never hard-coded bucket names, keys or paths | [review] |
| Network access is polite: user agent, timeouts, retries with backoff, rate limits per source | [review] |
| No new dependency without a reason in the PR; versions pinned for data libraries | [review] |
| Retired code stays in `ingestion/legacy/` until its source is rebuilt, then is deleted | [review] |

## 4. SQL and dbt (declarative layer)

| Rule | Check |
|---|---|
| `dbt parse` succeeds | [auto] |
| sqlfluff (dialect duckdb, jinja templater with dbt built-ins, no lakehouse connection; settings in `pyproject.toml`): lowercase keywords, explicit aliases, qualified columns when several tables are joined; RF04 (keyword-named columns such as `year`) and AL05 are off on purpose | [auto] |
| No `select *` in a Gold model's output from upstream models; import CTEs (`x as (select * from ref(...))`) and unions of the model's own CTEs are allowed | [review] |
| Every model has a YAML entry with description, column descriptions for keys, and at least one test (`not_null`/`unique` on the grain) | [auto] |
| Model header comment: what it is, grain, source | [review] |
| Gold facts join conformed dimensions through keys; no source-specific codes in Gold keys without a scheme prefix | [review] |
| Metrics declared in `semantic_models/` with description and filter | [review] |

## 5. YAML and declarations

| Rule | Check |
|---|---|
| yamllint clean; descriptions with `: ` quoted | [auto] |
| Source YAML has: description, extraction strategy, landing path, archive policy, Bronze tables, activities, schedule | [auto] `run.py schedule --check` + schema check |
| Schedules are valid cron and name existing runs | [auto] `run.py schedule --check` |

## 6. Workflows

| Rule | Check |
|---|---|
| Exactly the four workflows; actions pinned by full SHA with version comment; `runs-on` pinned | [auto] |
| Least privilege `permissions:` per workflow and job | [review] |
| Workflow changes, like all changes, merge after the `check` job and the independent review pass (owner authorization, 14 Oct 2026) | process |

## 7. Pull requests

- One topic per PR; title says the outcome; body says why, what changed, and how it was
  verified (trial run id, counts) — [review].
- Data changes are verified on a trial (`zz_dbt_trial` or a dev run) before merge — [review].
- Docs updated in the same PR when behaviour changes — [review].
- The reviewer records **PASS** or **FAIL with findings** on the PR; FAIL blocks merge.

## Enforcement

1. `check` job on every pull request (from the owner's `dev.yml` change; until then the
   job runs compile, pyflakes, YAML parse, configuration, dbt parse): `python -m ops.repo_check` (root allowlist and file
   rules, no `dev/`, four workflows with pinned actions, compile, ruff check + format,
   yamllint, configuration and schedules), `dbt parse`, and a gitleaks secret scan.
   Rule changes are made in `ops/repo_check.py` and `pyproject.toml`, not in the workflow.
2. sqlfluff on the dbt models is part of `ops/repo_check` (14 Oct).
3. Reviewer: an AI agent that did not write the change reviews every PR against this file
   in the scheduled pass before auto-merge; a weekly quality score goes to the Owner Desk.
