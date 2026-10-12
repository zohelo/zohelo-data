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
| `platform_inventory` | Per source: Landing / Archive files, GB and latest write; Bronze / Silver / Gold tables, rows, files, GB. Storage per bucket and folder; tables and Landing folders no source claims; registered tables that do not exist. Doubles as the connectivity check. Args: `--source <id> …` | nothing |
| `orphan_files` | Data files in table folders that no snapshot of any table references (R2 maintenance never removes them). Args: `--min-age-hours 48`, `--delete` | with `--delete`: removes those files |
| `retry_rejected` | Bronze rejections grouped by reason and dataset family; with `--apply` clears them so the next `bronze: load` retries (records kept under `retried/`). Args: `--source eurostat [--dataset GLOB …] [--apply]` | with `--apply`: control records |
| `restore_from_archive` | Receipts in Landing whose data file is gone; with `--apply` copies the Archive file back and verifies its SHA-256. Args: `--source eurostat [--dataset GLOB …] [--apply]` | with `--apply`: adds Landing files, never overwrites |

Rules: one file per script, `snake_case.py`, runnable as `python -m ops.<name>`. Write
output files to `$OPS_OUTPUT_DIR` (kept 7 days as a run artifact) and publish results as
annotations (`ingestion.common.report.annotate`; GitHub shows at most 10 per step). List
every script here; remove it when the task no longer exists.
