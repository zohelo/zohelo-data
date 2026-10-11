# Working on zohelo-data

The owner's current request takes precedence over historical plans.

The production platform is Cloudflare R2 + R2 Data Catalog / Apache Iceberg.
Google Drive and legacy `releases/` / `current/` folders are historical
architecture, not current targets.

## Keep it simple

Use direct Python/SQL and the smallest command needed for the task. Inspect an
existing workflow before choosing it. Do not add frameworks, agent delegation,
automatic follow-up jobs, pilots, reports or workflows without an explicit need.

The owner explicitly requested removal of repository test suites and CI/check
workflows. Do not recreate regression-test workflows, fixtures or mandatory
preflight suites unless the owner asks.

## Preserve the real operation

Keep credentials private, bounded retries/timeouts, resumable checkpoints,
writer locks, original payload bytes and ordinary input/output integrity checks.

Never:

- silently fall back from R2 to Google Drive;
- recreate `releases/` or layer `current/` as an internal visibility gate;
- claim complete source coverage from partial data;
- enable an ingestion schedule without explicit owner approval;
- delete the only recoverable raw source copy;
- expose R2 writer credentials to the browser.

## Repository structure

Keep code in the folder of its dataflow stage: `ingestion/<source>/` (shared code
in `ingestion/common/`), `transformation/<bronze|promotion|reconciliation>/`,
`semantic/`, `serving/`, `checks/`; settings in `config/`, documentation in
`docs/`. No top-level `src/` or `scripts/` folders, no dated or one-off files:
history lives in git. Python file names are snake_case; Python runs with the
repository root on `PYTHONPATH`. See README.md for the tree.

## Storage contract

- `zohelo-landing-prod/01_landing`: native/current raw inputs.
- `zohelo-landing-prod/05_archive`: verified raw history.
- R2 Data Catalog `bronze`, `silver`, `gold`: Apache Iceberg tables.
- `zohelo-lakehouse-prod/06_control`: operational state/checkpoints/receipts.

Use [docs/data-lifecycle.md](docs/data-lifecycle.md) for archival and
Landing→Bronze→Silver→Gold rules.

## Workflows

Keep exactly the three current operational Actions unless the owner asks for a
new one:

1. `1 · Ingest → Landing` (`ingestion.yml`)
2. `2 · Transform → Bronze / Silver / Gold` (`transform.yml`)
3. `3 · Deploy portal` (`deploy-portal.yml`)

Naming convention (owner direction, 11 October 2026): workflows are named by
pipeline stage and target layer. Every operation is `<source>: <action>`, or
`platform: <action>` when it is not tied to one source, e.g.
`eurostat: load new versions to Landing`. Job ids are `<source>-<action>` in
kebab-case; inputs are snake_case without per-operation prefixes; concurrency
groups are `zohelo-<stage>-<scope>`. The ingestion menu lists only authorized
operations; paused ones return when the owner resumes them. R2 paths are storage
nouns and are not renamed with operations.

No scheduled ingestion is currently authorized.

Owner direction, 2 October 2026: after retained-data reconciliation, explain the
actual versus intended stage-by-stage dataflow and cross-source dimensional
model before any ingestion resumes. Keep all ingestion paused until the owner
has reviewed that explanation and later instructs resumption. See the review
requirements and current implementation gaps in docs/architecture.md. Do not
claim Python file promotion or zero-copy references implement dbt cleansing or
integrated dimensional modeling.

Owner direction, 11 October 2026: manual Eurostat bulk-to-Landing is authorized,
only through the `eurostat: coverage check (dry run)` and
`eurostat: load new versions to Landing` operations of `1 · Ingest → Landing`
(`ingestion/eurostat/bulk_landing.py`). It writes exact provider bytes plus receipts to
`01_landing/eurostat/bulk/data/` and coverage/run records to
`06_control/eurostat_bulk_landing/`; it never parses, never touches Bronze, never
overwrites or deletes. Comext (DS-*) and partitioning of too-big datasets are out of
scope. No schedule. Everything else, including all other ingestion, stays paused.

## Branches, pull requests and triggers

Owner direction, 11 October 2026:

- All development happens on a feature branch, never by committing directly to
  `main`. Name it `<type>/<short-topic>` with type one of `feat`, `fix`, `docs`,
  `chore`.
- Changes reach `main` only through a pull request.
- After the pull request is merged, delete its branch (the repository setting
  "Automatically delete head branches" does this; delete manually if it did not).
  Do not keep merged, abandoned or one-off branches.
- `1 · Ingest → Landing` and `2 · Transform → Bronze / Silver / Gold` run only by
  manual dispatch. Never add `push` or `schedule` triggers to them, and never
  commit or merge to start a data run. `3 · Deploy portal` may run on pushes to
  `main` that change portal code.
- No workflow files on feature branches other than edits to the three
  operational workflows; any workflow file on any branch shows up in Actions.

## Deliver

Report what changed, what actually ran, whether it reached main and what remains.
Preserve other contributors' work. Prefer idempotent data operations so a failed
run can resume from durable state instead of starting over.
