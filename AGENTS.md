# Working on zohelo-data

The owner's current request takes precedence. This file holds the rules; the
design is in `docs/`.

## Platform

- Free platform: Cloudflare R2, R2 Data Catalog (Apache Iceberg), R2 SQL, GitHub
  Actions. No paid subscriptions; anything beyond free tiers is an owner decision.
- Google Drive and `releases/` / `current/` folders are retired; never fall back to
  or recreate them.

## Repository structure (owner direction, 12 October 2026)

Organized by dataflow stage; see README.md and docs/architecture.md:
`ingestion/`, `warehouse/`, `portal/`,
`.github/workflows/` + `run.py`, `ops/`, `docs/`.

- A source's configuration is `ingestion/sources/<source_system>.yaml`; its code is
  `ingestion/to_landing/<source_system>/` and `ingestion/to_bronze/<source_system>/`.
- Configuration lives next to the code it controls (`ingestion/sources/<source>.yaml`,
  `warehouse/warehouse.yaml`, `portal/portal.yaml`); no shared config folder until
  there is genuine infrastructure configuration. Never add configuration that no code
  reads — describe it in `docs/` instead.
- Retired code lives only in `ingestion/legacy/` until its source is rebuilt.
- No dated, one-off, status or result files anywhere; history lives in git.
- Python file names are snake_case; Python runs from the repository root.

## Documentation

`docs/` holds documentation only — architecture, Landing and extraction rules,
operating, source routes — as small as possible. Never add status reports, audits,
run results, plans or ad-hoc notes to the repository. Update the existing document
instead of adding a new one.

## Workflows and activities

Exactly four workflows (owner direction, 12 October 2026):

1. **Pipeline** (`pipeline.yml`) — the fixed dataflow, one job per activity
   (docs/operating.md). Runs only from `main`. Change it only when the dataflow
   changes; never add ad-hoc operations to it.
2. **Ops · ad-hoc** (`ops.yml`) — standing operational tasks in `ops/` that the owner
   may run again (inventories, checks). Runs only from `main`. Not for experiments.
3. **Dev · branch runs** (`dev.yml`) — one-off trials, tests and checks that need R2,
   while working on a feature branch. Scripts live in `dev/` on that branch and are
   deleted before the merge; `dev/` never exists on `main`. Never runs on `main`.
   Writes only to scratch locations (`zz_dev_*` namespaces or prefixes) that the same
   script removes, unless the owner approved a one-off production change.
4. **Deploy portal** (`deploy-portal.yml`) — portal build and deploy.

Shared Pipeline job steps are in `.github/actions/activity/`.

- An activity runs only when implemented and listed under `activities:` in the
  source's `ingestion/sources/<source>.yaml` (platform activities: `warehouse/warehouse.yaml`,
  `portal/portal.yaml`). Adding a source or an activity means code plus that list.
- When something cannot be checked from the agent session, use a `dev/` script on the
  feature branch; never push to `main` or add an Ops script just to run a check.
- Activities publish results as run annotations (`ingestion.common.report`, under
  about 4 KB each); job logs are not readable from agent sessions.
- Pipeline, Ops and Dev run by manual dispatch only. No `push` triggers on them; no
  `schedule` without owner approval. Never commit or merge to start a data run.
  Deploy portal runs on pushes to `main` that change `portal/app` or `portal/worker`.
- Naming: activities `<stage>.<verb>`; runs `<stage>: <verb>`; sources by
  `source_system` id; concurrency groups `zohelo-pipeline-<source>[-read]`,
  `zohelo-ops-<script>`, `zohelo-dev-<branch>-<script>`, `zohelo-portal`.

## Extraction (owner direction, 12 October 2026)

Per source, in order: the source's bulk feature; else a researched bulk workaround
that the owner decides; else the API after testing it for a full load. Record the
decision and the production load (`snapshot`/`delta`, incremental policy, schedule)
in `ingestion/sources/<source>.yaml`. Landing layout and rules: docs/landing.md.

## Data safety

- Keep credentials private; bounded retries and timeouts; resumable checkpoints; one
  writer per source; exact source bytes; integrity checks on every write.
- State lives in the data (Landing receipts, Iceberg tables and their file lists); no
  control or run-record folders in R2. Run results go to annotations and the job summary.
- Storage is paid beyond the free tier: store each thing once, keep Bronze lean (no
  per-row hashes or JSON copies of keys; one file per source file), remove what no
  table or receipt references.
- Never overwrite Landing. Landing → Archive is a move (owner decision 13 October 2026):
  once Bronze holds a file, copy it to the Archive, verify the SHA-256, then delete it
  from Landing; the receipt stays. Archive retention per source as configured
  (Eurostat: newest version + 90 days); docs/landing.md.
- Never claim complete source coverage from partial data.
- Never expose R2 writer credentials to the browser.
- Do not claim Silver/Gold modelling exists while `warehouse/passthrough` only copies
  Bronze.

## Authorizations

- Eurostat: Landing activities (11 October 2026) and Bronze build (12 October 2026).
  Comext (DS-*) is out of scope.
- GUS BDL: agreed approach 13 October 2026 (subgroup entity, website export for the
  initial load with one route and no IP rotation, API refresh of changed subgroups).
- A source's `activities:` list, merged to `main`, is its authorization. Other sources
  stay paused until their PR adds activities.
- No schedule is authorized.

## Autonomy

Work the [playbook](docs/playbook.md) without asking when the step stays inside the
agreed rules:

- Research sources, write code, run Dev branch runs, open pull requests and merge
  those in the "Claude merges" group below.
- Run and rerun authorized Pipeline activities from `main`, including restarts after
  transient failures and the next run after a time budget; follow up with scheduled
  check-ins instead of waiting in the conversation.
- Run Ops tasks in report mode; delete orphan files and apply `archive_policy` as
  configured.
- Fix defects found on the way, in a pull request.

Ask the owner first:

- An extraction route that is a workaround (website automation, unofficial files) or
  whose terms are unclear; anything needing an account, key or paid service.
- Deleting data outside the configured policies (dropping tables, raw data without a
  verified Archive copy, another layout's files).
- Schedules, new workflows, changes to the agreed rules or the repository structure.

Report at milestones (a source reaches a playbook stage, a decision is needed,
something failed twice); keep reports short.

## Branches and pull requests

- Develop on a feature branch `<type>/<short-topic>` (`feat`, `fix`, `docs`,
  `chore`); never commit directly to `main`.
- Changes reach `main` only through a pull request that passes the `check` job
  (`dev.yml`: compile, lint, configuration consistency, repository rules; no
  credentials, no committed tests).
- **Claude merges** (auto-merge after `check` passes): fixes and features inside the
  playbook for sources whose `activities:` are already authorized, ingestion and Ops
  code, Bronze and Silver models, Gold models following `docs/model.md`, docs that do
  not change rules.
- **The owner merges**: changes to `AGENTS.md`, `docs/playbook.md` or the rules in
  `docs/model.md`; a new source or a change to any `activities:` list; anything that
  deletes data or changes `archive_policy`/retention; workflows, infrastructure, costs,
  credentials. The PR description says which group it belongs to.
- Merged branches are deleted (repository setting); delete leftovers manually.
- No workflow files on branches other than edits to the four workflows.
- The `check` job is the only CI; do not restore removed tests, test workflows or
  historical task queues.

## Deliver

Report what changed, what actually ran, whether it reached `main` and what remains.
