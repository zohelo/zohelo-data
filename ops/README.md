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
| `platform_inventory` | Per source: Landing / Archive files, GB and latest write; Bronze / Silver / Gold tables, rows, files, GB. Storage per bucket and folder with a monthly cost estimate (R2 Standard beyond the 10 GB free tier; warning above $2); tables and Landing folders no source claims; registered tables that do not exist. Doubles as the connectivity check. Args: `--source <id> …` | nothing |
| `landing_objects` | What is stored in Landing: per prefix (ending in `/`) object count, MB, key groups and a sample; with `--peek` the first bytes (gzip decoded) and most frequent XML tags of up to `--limit` objects; a key peeks that object. Use when a source changes format or Bronze rejects files. Args: `PREFIX_OR_KEY … [--peek] [--limit 3]` | nothing |
| `catalog_maintenance` | R2 Data Catalog settings: compaction and snapshot expiration on, with their values. | nothing |
| `repo_check` | The [auto] rules of `docs/quality.md`: root allowlist, no `dev/`, the four workflows with SHA-pinned actions, file size/type, compile, ruff check and format, yamllint, sqlfluff on the dbt models (pinned version installed on demand), configuration and schedules. Run by the `check` job on every pull request (no credentials); run it locally before pushing: `python -m ops.repo_check`. | nothing |
| `notify_failure` | Run by the Pipeline workflow's `notify` job (not by ops.yml): opens an `alert` issue "Pipeline failed: <source> · <run>" with the run link, or comments on the open one; closing the issue re-arms it. | GitHub issues |
| `orphan_files` | Parquet files under `02_bronze/`, `03_silver/`, `04_gold/` and `__r2_data_catalog/` that no snapshot of any table references, including folders of dropped tables (R2 maintenance never removes them). Args: `--min-age-hours 48`, `--delete` | with `--delete`: removes those files |
| `project` | The organisation's Projects board through GraphQL, with a token of the `zohelo-automation` GitHub App (variable `PROJECT_APP_ID`, secret `PROJECT_APP_KEY`). Args: `inspect` (projects, fields, options, iterations, views) | nothing (read-only) |
| `retry_rejected` | Bronze rejections (`bronze.eurostat_rejected_files`) grouped by reason and dataset family; with `--apply` deletes the matching rows so the next `bronze: load` retries them (the table history keeps them). Args: `--source eurostat [--dataset GLOB …] [--apply]` | with `--apply`: rows of the rejections table |
| `restore_from_archive` | Receipts in Landing whose data file is gone; with `--apply` copies the Archive file back and verifies its SHA-256. Args: `--source eurostat [--dataset GLOB …] [--apply]` | with `--apply`: adds Landing files, never overwrites |

Rules: one file per script, `snake_case.py`, runnable as `python -m ops.<name>`. Write
output files to `$OPS_OUTPUT_DIR` (kept 7 days as a run artifact) and publish results as
annotations (`ingestion.common.report.annotate`; GitHub shows at most 10 per step). List
every script here; remove it when the task no longer exists.
