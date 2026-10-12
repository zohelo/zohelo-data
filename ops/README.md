# Ops: standing operational tasks

Scripts here are run by the **Ops · ad-hoc** workflow (`ops.yml`) from `main`: defined
tasks the owner may want to run again from time to time, such as inventories and
checks. One script per run, with the R2 credentials, by manual dispatch.

Not for experiments, trials or one-off fixes: those are `dev/` scripts on a feature
branch, run with **Dev · branch runs** and deleted before the merge.

Rules:

- One file per script, `snake_case.py`, runnable as `python -m ops.<name> [args]`.
  Files starting with `_` are helpers and cannot be dispatched.
- Write output files to `$OPS_OUTPUT_DIR` (kept as a run artifact for 7 days); publish
  results as annotations with `ingestion.common.report.annotate`.
- List every script below. Remove a script when the task no longer exists; move it
  into the Pipeline as an activity if the dataflow needs it on every run.

| Script | Purpose |
|---|---|
| `inspect_retained_inputs` | Read-only R2 inventory per source: Landing, Archive, control, Iceberg tables. Args: `--source gus_bdl …` (at most 5 sources per run: GitHub shows only 10 annotations per step) |
| `_r2_inventory` | Helper for `inspect_retained_inputs` |
