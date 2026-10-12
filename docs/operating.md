# Operating

## Workflows

| Workflow | Runs | Trigger |
|---|---|---|
| **Pipeline** (`pipeline.yml`) | the dataflow: pick a `source` and a `run` | manual |
| **Ops · ad-hoc** (`ops.yml`) | one script from `ops/` | manual |
| **Deploy portal** (`deploy-portal.yml`) | build and deploy `portal/app` and `portal/worker` | push to `main` changing them, or manual |

## Activities

Every source uses the same activity names; a source implements the ones it needs.

| Activity | Code | Meaning |
|---|---|---|
| `landing.inspect` | `ingestion/sources/<s>/` | source catalogue vs Landing: missing / changed / current; no Landing writes |
| `landing.extract` | `ingestion/sources/<s>/` | download missing or changed entities with receipts; never overwrites |
| `landing.verify` | `ingestion/sources/<s>/` | Landing integrity and remaining gaps |
| `bronze.load` | `landing_to_bronze/sources/<s>/` | incremental Landing → Bronze; rejections recorded |
| `bronze.verify` | `landing_to_bronze/common/lineage.py` | every Landing file is in Bronze or rejected with a reason |
| `landing.archive` | `landing_to_bronze/common/archive.py` | copy Bronze-accepted Landing files to Archive |
| `silver_gold.promote` | `warehouse/passthrough/` | platform: Bronze → Silver → Gold pass-through |
| `serving.refresh_index` | `portal/publish_index.py` | platform: rebuild the portal file index |

A Pipeline run is one activity or `landing → bronze (all steps)`; the portal index
refresh is added automatically after writing activities. An activity runs only when
it is implemented and listed under `activities:` in the source's `source.yaml`
(or `config/platform.yaml`). Activities are idempotent and resume from durable
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
