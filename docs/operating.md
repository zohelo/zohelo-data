# Operating

## Workflows

| Workflow | Runs | Trigger |
|---|---|---|
| **Pipeline** (`pipeline.yml`) | the dataflow: pick a `source` and a `run` | manual, `main` only |
| **Ops · ad-hoc** (`ops.yml`) | one standing task from `ops/` | manual, `main` only |
| **Dev · branch runs** (`dev.yml`) | one `dev/` script on a feature branch (trials, tests); deleted before merge | manual, never `main` |
| **Deploy portal** (`deploy-portal.yml`) | build and deploy `portal/app` and `portal/worker` | push to `main` changing them, or manual |

## Activities

Every source uses the same activity names; a source implements the ones it needs. A source
whose YAML sets `extraction.kind` (for example `bulk_file`) runs the shared pattern in
`ingestion/to_landing/patterns/` instead of a folder of its own.

| Activity | Code | Meaning |
|---|---|---|
| `landing.inspect` | `ingestion/to_landing/<s>/` or `patterns/<kind>.py` | source catalogue vs Landing: missing / changed / current; no Landing writes |
| `landing.extract` | `ingestion/to_landing/<s>/` or `patterns/<kind>.py` | download missing or changed entities with receipts; never overwrites |
| `landing.verify` | `ingestion/to_landing/<s>/` or `patterns/<kind>.py` | Landing integrity and remaining gaps |
| `bronze.load` | `ingestion/to_bronze/<s>/` | incremental Landing → Bronze; rejections recorded; prunes superseded versions after each load when `bronze_latest_only: true` |
| `bronze.verify` | `ingestion/common/lineage.py` | every Landing file is in Bronze or rejected with a reason; verifies one version per entity in Bronze (fails on breach); Landing files a Bronze table holds twice (warning) |
| `landing.archive` | `ingestion/common/archive.py` | move Bronze-loaded Landing files to Archive (copy, verify SHA-256, delete from Landing; receipt stays) |
| `archive.prune` | `ingestion/common/retention.py` | apply the source's Archive retention (`archive_policy` in its config) |
| `silver_gold.promote` | `warehouse/passthrough/` | platform: Bronze → Silver → Gold pass-through for sources not yet modelled |
| `silver_gold.build` | `warehouse/build.py` | platform: dbt build of `warehouse/models/` (filter = model names); Eurostat Silver in batches; publishes the data catalog |
| `serving.refresh_index` | `portal/publish_index.py` | platform: rebuild the portal file index |

A Pipeline run is one activity or `landing → bronze (all steps)`; the portal index
refresh is added automatically after writing activities. An activity runs only when
it is implemented and listed under `activities:` in `ingestion/sources/<source>.yaml`
(platform activities: `warehouse/warehouse.yaml`, `portal/portal.yaml`). Activities are idempotent and resume from durable
state, so a rerun is always safe. Results are published as run annotations and the
job summary.

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
Actions. There is no repository test suite; the `check` job in `dev.yml` runs on every
pull request (compile, lint, YAML, configuration consistency, dbt parse) and must pass
before merge. Quality rules: [quality.md](quality.md).

Schedules are declared under `schedule:` in each source's YAML and in
`warehouse/warehouse.yaml` (cron in UTC + run + optional filter). The Pipeline's
`scheduler` job (`python run.py schedule`) starts the runs that are due. It runs on GitHub's
twice-hourly schedule, which drops many events (#802), and on a dispatch of run
`scheduler: dispatch due runs` (any source; main only); both kinds share one catch-up window,
and a long gap between scheduler runs is checked in full. A weekly or monthly schedule whose
latest slot has no successful run since is dispatched again, at most twice per slot (#839);
`python run.py schedule --check` validates the declarations.

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
| `bronze_latest_only` | optional boolean; when `true`, a Bronze load removes superseded versions (policy #822/#845) |
| `bronze`, `silver`, `gold` | tables the source owns in each layer; `*_rejected_files` tables record decoder rejections |
| `legacy_tables` | tables of retired code still in the catalog (globs allowed), until deleted |
| `activities` | the Pipeline activities authorized for the source |

Shared Gold models and the Eurostat Silver batching settings are declared in
`warehouse/warehouse.yaml`. Code derives table lists (Bronze proofs, archive sources,
inventory) from these files; nothing is listed twice.
