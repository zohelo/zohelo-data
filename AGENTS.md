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
`config/`, `ingestion/`, `landing_to_bronze/`, `warehouse/`, `portal/`,
`.github/workflows/` + `run.py`, `ops/`, `docs/`.

- A source's code lives in `ingestion/sources/<source_system>/` and
  `landing_to_bronze/sources/<source_system>/`; its configuration is
  `ingestion/sources/<source_system>/source.yaml`.
- Retired code lives only in `ingestion/legacy/` until its source is rebuilt.
- No dated, one-off, status or result files anywhere; history lives in git.
- Python file names are snake_case; Python runs from the repository root.

## Documentation

`docs/` holds documentation only — architecture, Landing and extraction rules,
operating, source routes — as small as possible. Never add status reports, audits,
run results, plans or ad-hoc notes to the repository. Update the existing document
instead of adding a new one.

## Workflows and activities

Exactly three workflows: **Pipeline** (`pipeline.yml`), **Ops · ad-hoc** (`ops.yml`),
**Deploy portal** (`deploy-portal.yml`). Shared Pipeline job steps are in
`.github/actions/activity/`.

- Pipeline is the fixed dataflow, one job per activity (docs/operating.md). Change it
  only when the dataflow changes; never add ad-hoc operations to it.
- An activity runs only when implemented and listed under `activities:` in the
  source's `source.yaml` (or `config/platform.yaml`). Adding a source or an activity
  means code plus that list.
- Ad-hoc checks, inspections and one-off fixes go to `ops/` and run through Ops.
  Each ops script is listed in `ops/README.md` and deleted in the pull request that
  finishes its task.
- Activities publish results as run annotations (`ingestion.common.report`, under
  about 4 KB each); job logs are not readable from agent sessions.
- Pipeline and Ops run by manual dispatch only. No `push` triggers on them; no
  `schedule` without owner approval. Never commit or merge to start a data run.
  Deploy portal runs on pushes to `main` that change `portal/app` or `portal/worker`.
- Naming: activities `<stage>.<verb>`; runs `<stage>: <verb>`; sources by
  `source_system` id; concurrency groups `zohelo-pipeline-<source>[-read]`,
  `zohelo-ops-<script>`, `zohelo-portal`.

## Extraction (owner direction, 12 October 2026)

Per source, in order: the source's bulk feature; else a researched bulk workaround
that the owner decides; else the API after testing it for a full load. Record the
decision and the production load (`snapshot`/`delta`, incremental policy, schedule)
in `source.yaml`. Landing layout and rules: docs/landing.md.

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
- No workflow files on branches other than edits to the three workflows.
- Do not restore removed tests, CI workflows or historical task queues.

## Deliver

Report what changed, what actually ran, whether it reached `main` and what remains.
