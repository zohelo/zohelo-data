# Ops: standing operational tasks

Scripts here are run by the **Ops · ad-hoc** workflow (`ops.yml`) from `main`: defined
tasks the owner may want to run again from time to time. One script per run, with the
R2 credentials, by manual dispatch. Scripts that change anything do so only with an
explicit flag (`--apply`, `--delete`); without it they only report.

Not for experiments, trials or one-off fixes: those are `dev/` scripts on a feature
branch, run with **Dev · branch runs** and deleted before the merge. Recurring table
maintenance (compaction, snapshot expiration) is an R2 Data Catalog setting, not a
script.

| Script | Purpose | Writes |
|---|---|---|
| `platform_inventory` | Per source: Landing / Archive files, GB and latest write; Bronze / Silver / Gold tables, rows, files, GB (from the snapshot summary, or the manifests when a writer left the totals out); total snapshots. Storage per bucket and folder with a monthly cost estimate (R2 Standard beyond the 10 GB free tier; warning above $2); tables and Landing folders no source claims; registered tables that do not exist. Doubles as the connectivity check. Args: `--source <id> …` | nothing |
| `landing_objects` | What is stored in Landing: per prefix (ending in `/`) object count, MB, key groups and a sample; with `--peek` the first bytes (gzip decoded) and most frequent XML tags of up to `--limit` objects; a key peeks that object. Use when a source changes format or Bronze rejects files. Args: `PREFIX_OR_KEY … [--peek] [--limit 3]` | nothing |
| `catalog_maintenance` | R2 Data Catalog settings: compaction and snapshot expiration on, with their values. | nothing |
| `repo_check` | The [auto] rules of `docs/quality.md`: root allowlist, no `dev/`, the four workflows with SHA-pinned actions, file size/type, compile, ruff check and format, yamllint, sqlfluff on the dbt models (pinned version installed on demand), configuration and schedules. Run by the `check` job on every pull request (no credentials); run it locally before pushing: `python -m ops.repo_check`. | nothing |
| `runner_resources` | Run by the activity action (`.github/actions/activity`, not by ops.yml): `sample` records free disk and memory every 30 s in the background and publishes live annotations while the activity runs (a notice every 25 min for the first 100 min, a warning when free memory or disk first falls below a threshold), which survive a runner that is shut down as a whole; `report`, only when the activity failed or was cancelled, annotates the lowest values, the last ten minutes and the kernel's out-of-memory lines (runs killed from outside, exit 143/137, leave no annotation of their own). | nothing |
| `notify_failure` | Run by the Pipeline workflow's `notify` job (not by ops.yml), on a failed or cancelled run: opens an `alert` issue "Pipeline failed: <source> · <run>" with the run link, or comments on the open one; closing the issue re-arms it. | GitHub issues |
| `orphan_files` | Parquet files under `02_bronze/`, `03_silver/`, `04_gold/` and `__r2_data_catalog/` that no snapshot of any table references, including folders of dropped tables (R2 maintenance never removes them). Args: `--min-age-hours 48`, `--delete` | with `--delete`: removes those files |
| `project` | The organisation's Projects board through GraphQL, with a token of the `zohelo-automation` GitHub App (variable `PROJECT_APP_ID`, secret `PROJECT_APP_KEY`). Args: `inspect` (projects, fields, options, iterations, views); `status <issue> [ON_TRACK\|AT_RISK\|OFF_TRACK\|COMPLETE\|INACTIVE]` posts the newest comment by the owner or an org member on the Work log issue as a status update of project 1 | `status`: adds a project status update |
| `scheduler_trigger` | The external clock of the Pipeline scheduler (#802): Cloudflare Worker `zohelo-scheduler` (`ops/scheduler_worker/`) whose Cron Trigger (`17,47 * * * *`) dispatches Pipeline `platform` · `scheduler: dispatch due runs`, because GitHub's own schedule fires only a fraction of its slots. Args: `status` (default: Cron Triggers, whether the Worker's token is set), `deploy`, `pause`, `resume` | `deploy`/`pause`/`resume`: the Worker and its Cron Triggers |
| `retry_rejected` | Bronze rejections (`bronze.eurostat_rejected_files`) grouped by reason and dataset family; with `--apply` deletes the matching rows so the next `bronze: load` retries them (the table history keeps them). Args: `--source eurostat [--dataset GLOB …] [--apply]` | with `--apply`: rows of the rejections table |
| `restore_from_archive` | Receipts in Landing whose data file is gone; with `--apply` copies the Archive file back and verifies its SHA-256. Args: `--source eurostat [--dataset GLOB …] [--apply]` | with `--apply`: adds Landing files, never overwrites |

**Scheduler clock runbook.** First setup: Ops `script=scheduler_trigger args=deploy`; the
owner creates the fine-grained token (the organisation must allow fine-grained tokens and may
have to approve it) and adds it to the Worker as secret `GITHUB_TOKEN`; `args=status` then
shows `token_set: true`, and Worker dispatches appear within 30 minutes. Pause: Ops `script=scheduler_trigger args=pause` (GitHub's own
schedule keeps running as the backup; a later `deploy` restores the Cron Triggers). Resume:
`args=resume`. The Worker's only secret, `GITHUB_TOKEN` (fine-grained, this repository,
Actions: read and write), is set by the owner in the Cloudflare dashboard and never passes
through the repository; when it expires, dispatches fail with HTTP 401 and the 07:00
operations session sees the scheduler run count drop. Scheduler runs started by the Worker
are workflow dispatches named `Pipeline · platform · scheduler: dispatch due runs`.

Rules: one file per script (a script's deployable assets may sit in a folder of its own, like `scheduler_worker/`), `snake_case.py`, runnable as `python -m ops.<name>`. Write
output files to `$OPS_OUTPUT_DIR` (kept 7 days as a run artifact) and publish results as
annotations (`ingestion.common.report.annotate`; GitHub shows at most 10 per step). List
every script here; remove it when the task no longer exists.
