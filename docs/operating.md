# Operating

## Workflows

| Workflow | Runs | Trigger |
|---|---|---|
| **Pipeline** (`pipeline.yml`) | the dataflow: pick a `source` and a `run` | manual, `main` only |
| **Ops · ad-hoc** (`ops.yml`) | one standing task from `ops/` | manual, `main` only |
| **Dev · branch runs** (`dev.yml`) | one `dev/` script on a feature branch (trials, tests); deleted before merge | manual, never `main` |
| **Deploy portal** (`deploy-portal.yml`) | build and deploy `portal/app` and `portal/worker` | push to `main` changing them, or manual |

## Activities

Every source uses the same activity names; a source implements the ones it needs.

| Activity | Code | Meaning |
|---|---|---|
| `landing.inspect` | `ingestion/to_landing/<s>/` | source catalogue vs Landing: missing / changed / current; no Landing writes |
| `landing.extract` | `ingestion/to_landing/<s>/` | download missing or changed entities with receipts; never overwrites |
| `landing.verify` | `ingestion/to_landing/<s>/` | Landing integrity and remaining gaps |
| `bronze.load` | `ingestion/to_bronze/<s>/` | incremental Landing → Bronze; rejections recorded |
| `bronze.verify` | `ingestion/common/lineage.py` | every Landing file is in Bronze or rejected with a reason |
| `landing.archive` | `ingestion/common/archive.py` | move Bronze-loaded Landing files to Archive (copy, verify SHA-256, delete from Landing; receipt stays) |
| `archive.prune` | `ingestion/common/retention.py` | apply the source's Archive retention (`archive_policy` in its config) |
| `silver_gold.promote` | `warehouse/passthrough/` | platform: Bronze → Silver → Gold pass-through |
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
Actions. There is no repository test suite or CI workflow.
