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

The repository follows the dataflow (owner direction, 12 October 2026):

- `pipeline/` — the dataflow code. `pipeline/run.py` is the only entry point.
  `pipeline/sources/<source>/` holds one source's code; its `__init__.py` maps
  activity names to code (`ACTIVITIES`). `pipeline/common/` is shared code (R2
  stores, transport, lineage, archive, reporting). `pipeline/silver_gold/` is
  cross-source Silver/Gold.
- `config/sources.yaml` — the source registry: per source the authorized
  activities, storage locations and schedule. Adding or authorizing an activity
  is a change to this file plus the source's `ACTIVITIES`.
- `ops/` — ad-hoc scripts only (see `ops/README.md`).
- `semantic/`, `serving/`, `docs/`, `config/` as named.

No top-level `src/` or `scripts/` folders, no dated or one-off files outside
`ops/`: history lives in git. Python file names are snake_case; Python runs with
the repository root on `PYTHONPATH`. See README.md for the tree.

## Activities

Every source uses the same activity contract; a source implements the ones it
needs:

| Activity | Meaning |
|---|---|
| `landing.inspect` | provider catalogue vs Landing (missing / stale / current); no Landing writes |
| `landing.extract` | download new or changed items as native bytes + receipt; never overwrites |
| `landing.verify` | Landing integrity and remaining gaps after an extract |
| `bronze.load` | incremental Landing → Iceberg Bronze; raw_sha256 checkpoint; rejections recorded |
| `bronze.verify` | lineage: every Landing file is in Bronze or has a recorded rejection |
| `landing.archive` | copy Bronze-proven Landing files to `05_archive` (copy only) |
| `silver_gold.promote` | platform: Bronze → Silver → Gold (today pass-through / zero-copy) |
| `serving.refresh_index` | platform: rebuild the portal index from live R2 |

Activities are idempotent and resumable, so a scheduled run and a manual run are
the same thing. Each publishes its result as run annotations via
`pipeline.common.report` (annotations are the readable output channel; keep each
under ~4 KB).

## Storage contract

- `zohelo-landing-prod/01_landing`: native/current raw inputs.
- `zohelo-landing-prod/05_archive`: verified raw history.
- R2 Data Catalog `bronze`, `silver`, `gold`: Apache Iceberg tables.
- `zohelo-lakehouse-prod/06_control`: operational state/checkpoints/receipts.

Use [docs/data-lifecycle.md](docs/data-lifecycle.md) for archival and
Landing→Bronze→Silver→Gold rules.

## Workflows

Exactly three workflows (owner direction, 12 October 2026):

1. **Pipeline** (`pipeline.yml`) — the fixed dataflow. Inputs: `source` and `run`
   (one activity, or `landing → bronze (all steps)`); one job per activity in
   dataflow order; `pipeline/run.py plan` refuses activities that are not
   implemented or not authorized in `config/sources.yaml`. Change this file only
   when the dataflow itself changes. Never add an ad-hoc operation to it.
2. **Ops · ad-hoc** (`ops.yml`) — runs one script from `ops/` by name. All checks,
   inspections, one-off backfills and fixes go here. Delete scripts that are no
   longer needed.
3. **Deploy portal** (`deploy-portal.yml`) — portal build and deploy.

Shared job steps live in the composite action `.github/actions/activity/`.
Concurrency: one writer per source (`zohelo-pipeline-<source>`); read-only runs
use `zohelo-pipeline-<source>-read`; ops use `zohelo-ops-<script>`.

Naming: workflows are named by purpose; activities are `<stage>.<verb>`; runs in
the dispatch menu are `<stage>: <verb>`; sources use their `config/sources.yaml`
id. Job ids are kebab-case activity names. R2 paths are storage nouns and are not
renamed with code.

No scheduled run is currently authorized.

Owner direction, 2 October 2026: after retained-data reconciliation, explain the
actual versus intended stage-by-stage dataflow and cross-source dimensional
model before any ingestion resumes. Keep all ingestion paused until the owner
has reviewed that explanation and later instructs resumption. See the review
requirements and current implementation gaps in docs/architecture.md. Do not
claim Python file promotion or zero-copy references implement dbt cleansing or
integrated dimensional modeling.

Owner direction, 11 October 2026: manual Eurostat bulk-to-Landing is authorized
(`eurostat` activities in `config/sources.yaml`, code in
`pipeline/sources/eurostat/`). It writes exact provider bytes plus receipts to
`01_landing/eurostat/bulk/data/` and coverage/run records to
`06_control/eurostat_bulk_landing/`; Landing activities never parse and never
overwrite or delete. Comext (DS-*) and partitioning of too-big datasets are out of
scope. Building Eurostat Bronze from Landing is authorized (12 October 2026).
No schedule. All other ingestion stays paused.

## Branches, pull requests and triggers

Owner direction, 11 October 2026:

- All development happens on a feature branch, never by committing directly to
  `main`. Name it `<type>/<short-topic>` with type one of `feat`, `fix`, `docs`,
  `chore`.
- Changes reach `main` only through a pull request.
- After the pull request is merged, delete its branch (the repository setting
  "Automatically delete head branches" does this; delete manually if it did not).
  Do not keep merged, abandoned or one-off branches.
- **Pipeline** and **Ops · ad-hoc** run only by manual dispatch. Never add `push`
  triggers to them, never add a `schedule` without owner approval, and never
  commit or merge to start a data run. **Deploy portal** may run on pushes to
  `main` that change portal code.
- No workflow files on feature branches other than edits to the three workflows;
  any workflow file on any branch shows up in Actions.

## Deliver

Report what changed, what actually ran, whether it reached main and what remains.
Preserve other contributors' work. Prefer idempotent data operations so a failed
run can resume from durable state instead of starting over.
