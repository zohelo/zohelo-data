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
- Never overwrite Landing. Delete a Landing file only after its Archive copy is
  verified and no checkpoint points to it.
- Never claim complete source coverage from partial data.
- Never expose R2 writer credentials to the browser.
- Do not claim Silver/Gold modelling exists while `warehouse/passthrough` only copies
  Bronze.

## Authorizations

- Eurostat: Landing activities (11 October 2026) and Bronze build (12 October 2026).
  Comext (DS-*) is out of scope.
- All other ingestion is paused until the owner resumes it per source.
- No schedule is authorized.

## Branches and pull requests

- Develop on a feature branch `<type>/<short-topic>` (`feat`, `fix`, `docs`,
  `chore`); never commit directly to `main`.
- Changes reach `main` only through a pull request; the owner merges.
- Merged branches are deleted (repository setting); delete leftovers manually.
- No workflow files on branches other than edits to the four workflows.
- Do not restore removed tests, CI workflows or historical task queues.

## Deliver

Report what changed, what actually ran, whether it reached `main` and what remains.
