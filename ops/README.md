# Ops: ad-hoc scripts

Scripts here are run by the **Ops · ad-hoc** workflow (`ops.yml`): one script per run,
with the R2 credentials, by manual dispatch. They are checks, inspections and
one-off fixes, not part of the dataflow. The **Pipeline** workflow never changes for
ad-hoc work.

Rules:

- One file per script, `snake_case.py`, runnable as `python -m ops.<name> [args]`.
  Files starting with `_` are helpers and cannot be dispatched.
- Write output files to `$OPS_OUTPUT_DIR`; the workflow keeps them as a run artifact
  for 7 days. Publish results as annotations with `ingestion.common.report.annotate`.
- List every script in the table below with why it exists and when to remove it.
  Delete scripts that are no longer needed; git keeps the history.
- If an ops script becomes something the dataflow needs on every run, move it into
  the pipeline as an activity instead of running it here repeatedly.

| Script | Purpose | Remove when |
|---|---|---|
| `inspect_retained_inputs` | Read-only R2 inventory per source (Landing, Archive, control, Iceberg tables). Args: `--source gus_bdl …` (at most 5 sources per run: GitHub shows only 10 annotations per step) | every registered source has its own `landing.verify` / `bronze.verify` |
| `migrate_eurostat_landing` | One-off: copy Eurostat Landing from its two earlier layouts into the agreed layout (`--mode plan` / `--mode copy`) | the Eurostat code reads only the agreed layout and the originals are retired |
| `dbt_trial` | Trial: dbt-duckdb reads Bronze and writes an Iceberg table to the R2 catalog (scratch namespace `zz_dbt_trial`, dropped afterwards) | the trial result is decided |
| `_r2_inventory` | Helper for `inspect_retained_inputs` | with it |
