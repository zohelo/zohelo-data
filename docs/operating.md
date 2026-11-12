# Operating

## Workflows

| Workflow | Runs | Trigger |
|---|---|---|
| **Pipeline** (`pipeline.yml`) | the dataflow: type a `source` id (free text, checked by the plan job) and pick a `run` | manual, `main` only |
| **Ops · ad-hoc** (`ops.yml`) | one standing task from `ops/`; weekly CodeQL scan of `main` (job `codeql`) | manual or the schedule table (ops/README.md), `main` only |
| **Dev · branch runs** (`dev.yml`) | one `dev/` script on a feature branch (trials, tests); deleted before merge. Also the merge gate: `check` and `check-portal` | trials: manual, never `main`; gate: label `merge:check` on a pull request |
| **Deploy portal** (`deploy-portal.yml`) | build and deploy `portal/app` and `portal/worker` | push to `main` changing them, or manual |
| **Branch cleanup** (`branch-cleanup.yml`) | deletes a merged pull request's branch; weekly sweep of merged branches | pull request merged, Monday, or manual |

## Activities

Every source uses the same activity names; a source implements the ones it needs. A source
whose YAML sets `extraction.kind` (for example `bulk_file`) runs the shared pattern in
`ingestion/to_landing/patterns/` instead of a folder of its own.

| Activity | Code | Meaning |
|---|---|---|
| `landing.inspect` | `ingestion/to_landing/<s>/` or `patterns/<kind>.py` | source catalogue vs Landing: missing / changed / current; no Landing writes |
| `landing.extract` | `ingestion/to_landing/<s>/` or `patterns/<kind>.py` | download missing or changed entities with receipts; never overwrites |
| `landing.verify` | `ingestion/to_landing/<s>/` or `patterns/<kind>.py` | Landing integrity and remaining gaps |
| `bronze.load` | `ingestion/to_bronze/<s>/` | incremental Landing → Bronze; rejections recorded; prunes superseded versions after each load when `bronze_latest_only: true`. Eurostat, GUS BDL: per entity, Bronze and Silver together (#875) |
| `bronze.verify` | `ingestion/common/lineage.py` | every Landing file is in Bronze or rejected with a reason; verifies one version per entity in Bronze (fails on breach); Landing files a Bronze table holds twice (warning) |
| `landing.archive` | `ingestion/common/archive.py` | move Bronze-loaded Landing files to Archive (copy, verify SHA-256, delete from Landing; receipt stays) |
| `archive.prune` | `ingestion/common/retention.py` | apply the source's Archive retention (`archive_policy` in its config) |
| `silver_gold.build` | `warehouse/build.py` | platform: dbt build of `warehouse/models/` (filter = model names; empty = only models whose inputs or code changed, leaving out models whose source table does not exist yet (a new source before its first Bronze load; listed as `waiting` in the run summary); a waiting Gold block (tag `gold_block`) does not hold back the facts, dimensions and metrics: the block is listed again under `empty_blocks`, passed to dbt as the var `waiting_blocks`, read as zero rows and noted by `build_check` as `waiting: …`; only a waiting block that has no table is listed there, and the geography map reads a new source's codes only once that Silver table exists (`poc_optional_ref`, config meta `optional_parents`), so a source without Bronze holds back only its own models; a waiting model that already has a table (an input it was built from is gone, or a new input was added before that input's first load) is listed under `stalled` and fails the build before dbt runs, with the missing source tables in the error annotation (#2903); a block without a table that the plan did not declare waiting (a partial catalog listing, a run killed between renames, a filtered build such as `poc_fact_annual+`) fails the build, and so does a block that is built in the run and fails; `fqn:*` = all); publishes the data catalog (only the Pipeline on `main`; Dev trials skip it) |
| `gold.publish` | `warehouse/publish.py`, `warehouse/publish_v2.py` | platform: rewrites 06_published/v1/ from Gold, then 06_published/v2/ when warehouse.yaml `published_v2` is true (docs/published.md). Skipped in a run whose model build another run does (`covered`, below), and after a build that was replaced in the queue by a newer one: the other run publishes after its build. Reads every metrics table before the first write; the table of a live KPI that is missing (a concurrent `silver_gold.build` swaps tables) is read again for up to 9 minutes; still missing, the run writes and removes nothing and ends green with a warning (`deferred`) while the live files are at most 6 hours old, and fails after that (#2834) |
| `serving.refresh_index` | `portal/publish_index.py` | platform: rebuild the portal file index; dbt temporary and backup tables (`__dbt_tmp`, `__dbt_backup`) are not shown, and listed tables that a concurrent `silver_gold.build` swaps away (not found again after one 15 s re-check of all misses) are skipped with a warning annotation and shown by the next refresh; more than 3 skipped tables or more than 10% of those listed fail the job (R2 answers the same 404 for a bad credential); any other error fails the job (#2825) |

A Pipeline run is one activity or a source's `full dataflow: landing → bronze → silver → gold → portal`
(owner direction 20 Oct, #875): Landing, Bronze (entity sources write Silver with Bronze), archive,
then the changed-only model build (Silver/Gold models, semantic layer, data catalog), then the
published files (gold.publish), then the portal index. Scheduled source runs are full dataflow runs. The portal index refresh is added
automatically after writing activities. An activity runs only when
it is implemented and listed under `activities:` in `ingestion/sources/<source>.yaml`
(platform activities: `warehouse/warehouse.yaml`, `portal/portal.yaml`). Activities are idempotent and resume from durable
state, so a rerun is always safe. Results are published as run annotations and the
job summary.

**One model build at a time, and no build is lost (#2932).** The build job runs in the concurrency group
`zohelo-pipeline-build`. GitHub keeps one running and one waiting job per group and cancels the waiting one when
another arrives, so the job `silver_gold.queue` (`warehouse/build_queue.py`) runs first and decides from the jobs
of the Pipeline runs that have not ended:

- **enter**: no build is waiting; the run's build runs now or waits for the running one.
- **covered**: another run's changed-only build is waiting and has not started. It plans when it starts, after
  this run's Bronze load, and rebuilds what this run changed. This run's build and `gold.publish` are skipped, the
  run ends green, and the annotation `silver_gold.build covered by run <id>` names the covering run. The proof is in
  that run: the activity step of its build job started after `decided_at` (`steps[].started_at` in the jobs API;
  the job's own `started_at` is the time it was queued), and its `warehouse dbt build` annotation lists the models.
- A build with a model filter (a `platform` run with a `filter`; job name `silver_gold.queue · filter`) covers no
  run and is covered by none: next to it a queue job waits until the waiting build has started.

Queue jobs decide in the order GitHub created them, so sources dispatched in the same minute (the scheduler does
that) cannot both take the waiting place. A queue job that cannot read the API enters with a warning. To start
several sources at once, dispatch them: one build serves all of them.

## Local development

Use the versions in `.python-version` and `.node-version`; run Python from the
repository root.

```sh
python -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
PYTHONPATH=. .venv/bin/python run.py plan --source eurostat --run "landing: inspect"
npm --prefix portal/app ci --ignore-scripts --no-audit --no-fund
npm --prefix portal/app run dev
```

Production reads and writes need the R2 credentials, which exist only in GitHub
Actions. There is no repository test suite. No workflow runs on a push to a pull request
(owner direction 12 Nov 2026): run `python -m ops.repo_check` locally while iterating. The
`check` job in `dev.yml` (linked task, compile, lint, YAML, configuration consistency, offline
dbt compile with a DuckDB parse of every model, dbt parse, secret scan) runs once, when the
label `merge:check` is added to the reviewed, final pull request, and must pass on its head
before merge ([flow.md](flow.md), merge gate).
CodeQL scans `main` weekly from Ops, not each pull request. Quality rules: [quality.md](quality.md).

### Dev trials

A Dev trial is one `dev/<name>_trial.py` on the feature branch, run with the Dev workflow. Every trial:

- writes only to a scratch namespace or prefix named `zz_dev_<branch topic>` and drops it at the end, also on failure
  (`try`/`finally`);
- never calls a production publisher directly: `warehouse/catalog.py publish()`, `gold.publish`
  (`warehouse/publish.py`) or anything else that writes `_portal_catalog/`, `06_published/` or a production namespace.
  `catalog.publish()` skips itself outside the Pipeline on `main` (#2112); other writers have no such guard, so a trial
  checks every write target it adds;
- reports what it wrote (namespace, tables, counts) as annotations, and the PR body cites the trial run id.

Do not copy an older trial script from branch history without checking it against this list: `dev/seeds_trial.py`
(commit b62f65e) rewrote the production catalog from a branch (#2106).

Schedules are declared under `schedule:` in each source's YAML and in
`warehouse/warehouse.yaml` (cron in UTC + run + optional filter). The Pipeline's
`scheduler` job (`python run.py schedule`) starts the runs that are due. It runs on GitHub's
twice-hourly schedule, which drops many events (#802), and on a dispatch of run
`scheduler: dispatch due runs` (any source; main only); both kinds share one catch-up window,
and a long gap between scheduler runs is checked in full. A weekly or monthly schedule whose
latest slot has no successful run since is dispatched again, at most twice per slot (#839);
`python run.py schedule --check` validates the declarations.
`python run.py schedule --next` prints each scheduled source's next run (UTC), the basis of next_expected_at in status.json.

## Source declarations

Every source is declared in `ingestion/sources/<source>.yaml` with one key set, validated by
`python run.py schedule --check`:

| Key | Meaning |
|---|---|
| `source_system` | the source id (= file name) |
| `description` | what the source is |
| `extraction` | `initial`, `production`, `incremental_policy`, `schedule` (cron in UTC + run + optional filter) |
| `landing` | Landing folder |
| `archive_policy` | `move`, `retention` (`keep_latest`, `keep_days`), `prune` (`report`/`delete`), optional `without_bronze`; required when `bronze.load` is authorized |
| `archive_folder` | Archive folder of a source not yet rebuilt (default `05_archive/<source>/`) |
| `refresh` | required for loaded sources (owner policy, 21 Oct, #875): scheduled ingestion of a **small** source is `mode: full`; of a **big** source `mode: incremental` with `unit` (the entity a new version replaces), incremental in **every layer** (needs `to_bronze/<s>/entities.py`); a change that requires recalculating the whole table (e.g. Silver logic) is a **full build** for any source. `decided`: `D32` plus the PR or issue of that source (sources decided before D32 cite the owner decision issue, `#875`); small or big is judged per source by Claude under the owner's rules of 20–21 Oct (size, update pattern, whether the source offers changed-since; no fixed row threshold), D32 |
| `bronze_latest_only` | optional boolean; when `true`, a Bronze load removes superseded versions (policy #822/#845) |
| `bronze`, `silver`, `gold` | tables the source owns in each layer; `*_rejected_files` tables record decoder rejections. `gold` is the one place a source's own Gold tables are listed ([source-registration.md](source-registration.md)) |
| `full_rebuild` | optional; row ceilings of the source's own full-rebuild dbt models, `model: {rows: <ceiling>[, exception: "#<issue>"]}` (owner policy #822), with the measured count in a comment |
| `legacy_tables` | tables of retired code still in the catalog (globs allowed), until deleted |
| `activities` | the Pipeline activities authorized for the source |

Shared Gold models (dimensions, facts, seeds, KPI metrics), their row ceilings and the Eurostat Silver
batching settings are declared in `warehouse/warehouse.yaml`. Code derives table lists (Bronze proofs,
archive sources, inventory, the Gold list and the row ceilings: `warehouse/declared.py`) from these files;
nothing is listed twice.
