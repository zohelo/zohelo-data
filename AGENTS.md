# Working on zohelo-data

The owner's current request takes precedence. This file holds the rules; the
design is in `docs/`.

## Mandate (owner direction, 13 October 2026)

Claude leads the platform build autonomously, as product owner and orchestrator, until
the platform is in production: every source in every layer, modelled and scheduled;
problem sources researched and, if no route exists, parked for the owner; the portal
reflects R2; storage and retention optimized for every source; the dimensional model,
the semantic layer and a catalog of dimensions, KPIs and lineage complete. Claude plans
the work (backlog in the claude.ai project `zohelo-data`, not in the repository), uses
agents and scheduled sessions, and asks the owner only for the decisions listed under
"Ask the owner first".

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
- Pipeline, Ops and Dev run by manual dispatch or by `schedule` triggers that Claude
  adds to `pipeline.yml`/`ops.yml` for authorized activities (polite to the source,
  within free tiers). No `push` triggers on them. Never commit or merge to start a
  data run.
  Deploy portal runs on pushes to `main` that change `portal/app` or `portal/worker`.
- Naming: activities `<stage>.<verb>`; runs `<stage>: <verb>`; sources by
  `source_system` id; concurrency groups `zohelo-pipeline-<source>[-read]`,
  `zohelo-ops-<script>`, `zohelo-dev-<branch>-<script>`, `zohelo-portal`.

## Extraction (owner direction, 12 October 2026)

Per source, in order: the source's bulk feature; else a researched bulk workaround
(Claude decides when the terms allow it and access stays polite; unclear terms go to
the owner); else the API after testing it for a full load. Record the
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
  initial load, API refresh of changed subgroups). Owner decisions 22–23 October 2026 (#889):
  the cheapest ~100 subgroups via the API; the rest from the website, with parallel website
  sessions (separate GitHub jobs, one account; start with 3, up to 10, scaled by measured
  part times and error rates). No VPNs.
- A source's `activities:` list, merged to `main`, is its authorization. Claude adds
  sources and activities through pull requests under the Mandate.
- Claude may schedule authorized activities (see Workflows and activities).
- Standing owner permissions (24 October 2026): Claude closes an `alert` issue once the
  cause is fixed and the source's latest run is green, with a note on the issue; Claude
  deletes its own leftover work branches (`test/*`, `dev/*`, and others the owner names)
  through Ops `branch_cleanup` (report first, then `--apply`).

## Autonomy

Work the [playbook](docs/playbook.md) without asking: research sources, write code,
run Dev branch runs, open and merge pull requests, run and rerun Pipeline and Ops
activities, add sources, activities, schedules and retention policies, change the
model and the docs, and fix defects on the way. Follow up with scheduled sessions
instead of waiting in a conversation.

Ask the owner first (only these):

- Anything paid or beyond a free tier; a new account, key or credential the owner must
  create; changing the platform stack (R2, R2 Data Catalog, GitHub Actions, dbt).
- An extraction route whose terms or legality are unclear, or that would need IP
  rotation, VPNs or impolite access (never done).
- Irreversible deletion outside the configured policies: dropping production tables,
  raw data without a verified Archive copy, deleting Archive data beyond a policy.
- Any change to AGENTS.md itself.

A problem source with no workable route after full research is parked with the
findings for the owner, and work continues elsewhere. Report at milestones and when an
owner decision is needed; keep reports short.

## Branches and pull requests

- Develop on a feature branch `<type>/<short-topic>` (`feat`, `fix`, `docs`,
  `chore`); never commit directly to `main`.
- Changes reach `main` only through a pull request that passes the `check` job
  (`dev.yml`: compile, lint, configuration consistency, repository rules; no
  credentials, no committed tests).
- **Claude merges** with auto-merge after `check` passes: everything under the Mandate
  except changes to AGENTS.md.
- **The owner merges** changes to AGENTS.md and pull requests that touch the "Ask the
  owner first" items. The PR description says which group it belongs to.
- Merged branches are deleted (repository setting); leftover `test/*` and `dev/*` branches
  are deleted with Ops `branch_cleanup`. Dev trials reuse one `test/<topic>` branch.
- No new workflow files; edits to the four workflows are allowed.
- The `check` job is the only CI; do not restore removed tests, test workflows or
  historical task queues.

## Deliver

Report what changed, what actually ran, whether it reached `main` and what remains.
