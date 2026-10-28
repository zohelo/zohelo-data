# zohelo backlog — item specification

Status: approved by the owner, 27 Oct 2026. Applies to `zohelo/zohelo-data`, `zohelo/zohelo-bi`, `zohelo/zohelo`.

## 1. Principles

1. **The YAML is the plan; issues are its rendering.** Every Area, Epic, Deliverable and Task is written in
   `.github/backlog/` of the repo that ships it. `ops/backlog_sync.py` (zohelo-data, Ops workflow) creates and
   updates the GitHub issues from it. Nobody creates a backlog issue by hand; owner tickets (issue forms) are the
   only exception and are triaged into the YAML.
2. **No PR without an issue.** Every PR body contains `Closes #<Task number>` (or `Refs #<n>` for a partial
   step of a Bug). The `check` workflow in every repo fails a PR that links no open Task or Bug.
3. **A Task is written for an implementer that knows nothing but the Task.** A less capable model must be able
   to deliver it without asking a question and without re-deciding anything. If the writer cannot fill a field
   precisely, the Task is not ready and stays in the Deliverable as a `todo:` line.
4. **One Task = one branch = one PR ≤ 1 day of agent work, ≤ 400 changed lines (deletions excepted).**
   Bigger work is split, never extended.
5. **Done work is in the backlog too.** Work already merged is recorded as closed Tasks with `done_by:` the PR,
   so history and progress are complete. Stages finished before this backlog existed may be recorded as done
   Deliverables with evidence in `notes` (file path, PR or run URL) instead of reconstructed Tasks.
6. **Verification-only Tasks** (observe N runs, confirm a load completed) have no code change: they close with a
   comment linking the evidence run, and declare `verify_only: true`; the PR-link check does not apply to them.

## 2. Layers

| Layer | GitHub issue type | Parent | Size | Closes when |
|---|---|---|---|---|
| Area | `Area` | none (product = repo) | standing | never while the product lives |
| Epic | `Epic` | Area | one owner-recognisable outcome, ≤ 1 phase | all Deliverables closed and its `done_when` verified |
| Deliverable | `Deliverable` | Epic (may be in another repo of the org) | ≤ 5 days, a shippable component or stage | all Tasks closed and its `done_when` verified |
| Task / Bug | `Task` / `Bug` | Deliverable | ≤ 1 day, one PR | its PR merges (`Closes #n`) and `verify` passes |

`backlog_sync apply` closes a Deliverable once all its Tasks are closed and it has no `todo` lines, and an Epic once all its
Deliverables are closed (comment lists the `done_when`), so progress rolls up on the board; it never reopens an issue.
The Quality Engineer reopens one whose `done_when` turns out false.

## 3. Files

```
<repo>/.github/backlog/
  areas.yaml                 # the repo's Areas, in display order
  epics/<epic-key>.yaml      # one file per Epic: the Epic, its Deliverables and their Tasks
zohelo-data only:
  stages.yaml                # the 11-stage source template (source Epics are generated from the register)
```

Keys are stable lower-case slugs, unique across the org, prefixed by repo: `data/…`, `bi/…`, `com/…`.
A Deliverable key is `<epic-key>/<slug>`; a Task key `<deliverable-key>/<slug>`. Keys never change after an
issue exists (the issue carries `<!-- zohelo:key=… -->`); renaming a title is fine.

## 4. Schema

### Area (`areas.yaml`)

```yaml
areas:
  - key: data/ops                     # required
    title: Platform · Operations & quality   # required, ≤ 60 chars
    purpose: >-                       # required, 1–3 sentences, business language
      Keeps the platform running unattended: schedules, alerts, clean storage, credentials.
```

### Epic (`epics/<key>.yaml`, top level)

```yaml
key: data/ops/r2-cleanup              # required
area: data/ops                        # required, an Area key in this repo
title: R2 clean-up                    # required, ≤ 60 chars, no ids
phase: 0                              # required: 0 | 1 | 2 | 3 | run
priority: P1                          # required: P1 | P2 | P3
target: 2026-11-14                    # optional, Epic target date
why: >-                               # required, 2–4 sentences: the business need, in owner language
  …
outcome: >-                           # required, 1–2 sentences: what is true when this Epic is done
  …
in_scope: [ … ]                       # required, short bullets
out_of_scope: [ … ]                   # required, at least one bullet
done_when: [ … ]                      # required, checkable statements (measured, not "works well")
depends_on: [ <epic keys> ]           # optional
decisions: [ "owner 27 Oct #2" ]      # optional: decision-log rows this Epic relies on
deliverables: [ … ]                   # required, see below
```

### Deliverable (list item under `deliverables:`)

```yaml
- key: data/ops/r2-cleanup/duplicates
  title: Duplicate raw data removed    # ≤ 60 chars
  repo: zohelo-data                    # optional; default = repo of the file (cross-repo stages)
  done_when: [ … ]                     # required, checkable
  depends_on: [ <deliverable keys> ]   # optional
  state: open                          # open | done (done needs evidence in notes)
  notes: [ … ]                         # optional; evidence for done Deliverables
  todo: [ "…" ]                        # optional: work known but not yet written as Tasks (not ready)
  tasks: [ … ]                         # required unless every item is still in todo
```

### Task (list item under `tasks:`) — the Definition of Ready is "every required field is precise"

```yaml
- key: data/ops/r2-cleanup/duplicates/opendata-landing
  type: Task                           # Task | Bug
  title: Delete the opendata_org Landing copy   # imperative, ≤ 70 chars, no ids
  effort: S                            # S ≤ 2 h · M ≤ ½ day · L ≤ 1 day (larger = split)
  state: open                          # open | done (done needs done_by)
  done_by: https://github.com/zohelo/…/pull/123   # done Tasks only
  contract: false                      # true = touches workflows, credentials, published-layer
                                       # paths, catalogue schema, design tokens, AGENTS.md → architect approval
  owner_action: null                   # text when the owner must do something (≤ 10 min, exact clicks)
  verify_only: false                   # true = no code change; closes with a comment linking evidence; label verify-only
  repo: zohelo-bi                      # optional override for one Task; prefer a Deliverable with repo:
  context: >-                          # required: why this Task exists and what the implementer must know;
    …                                  # name the current state ("today X does Y") and the target state
  files:                               # required: every path to touch, with the operation
    - edit: ops/platform_inventory.py
    - edit: ops/README.md
    - delete: ingestion/legacy/
  steps:                               # required: numbered, concrete, in order; no "investigate"
    - …
  acceptance:                          # required: checkable statements a reviewer can tick
    - …
  verify:                              # required: exact commands or workflow runs and the expected result
    - run: python -m pytest ops/tests/test_platform_inventory.py -q
      expect: all tests pass
    - workflow: Ops (ops.yml), script platform_inventory, mode plan
      expect: annotation "zz_dev namespaces: 0"
  out_of_scope: [ … ]                  # required, at least one bullet: what NOT to change
  depends_on: [ <keys> ]               # optional; any layer, any repo (keys are org-unique)
  notes: [ … ]                         # optional: pitfalls, links, known traps
```

Rules for Task writers (checked by `repo_check`):

- `files` lists real paths: edit/delete paths exist on `main` or are created by a Task this one depends on; a new
  path is `create`. Settings-only and owner Tasks use `files: []` and `verify_only: true`.
- Max 3 open `owner:action` items: owner Tasks wait with `depends_on`; the sync adds the label when they become due.
- Destructive Ops runs (`--apply`, `--delete`) need a decision in the Epic's `decisions:`; otherwise they are owner actions.
- `steps` never say "investigate", "consider", "as needed", "etc."; a step that needs a decision is a
  decision recorded in the Epic's `decisions` first.
- `acceptance` and `verify` are runnable in CI, the Dev workflow or an Ops run; the session itself can't reach
  Cloudflare, so cloud effects are verified through a workflow annotation.
- Secrets are named, never valued. Destructive operations run through an Ops script in plan mode first, then
  `--apply`, and the Task says so.
- Title ≤ 70, body ≤ 3 KB rendered; longer context goes to `docs/` and is linked.

## 5. Rendered issue (what the implementer reads)

```
<!-- zohelo:key=data/ops/r2-cleanup/duplicates/opendata-landing -->
**Parent:** #<deliverable> · **Effort:** S · **Phase:** 0 · **Contract:** no

## Context
…
## Files
- edit `ops/platform_inventory.py` …
## Steps
1. …
## Acceptance
- [ ] …
## Verify
- `python -m pytest …` → all tests pass
## Out of scope
- …
## How to deliver
Branch `<type>/<number>-<topic>` (feat, fix, chore, docs) from `main`. Read `AGENTS.md`, then this issue, nothing else first.
Open one PR titled like this issue with `Closes #<this>` in the body (`Closes zohelo/<repo>#<this>` when the PR is in another repo). Stop and comment here if any step
is impossible as written; do not improvise.
<!-- zohelo:notes — everything below is free text and is never overwritten by the sync -->
```

## 6. Labels and project fields set by the sync

Labels: `verify-only`, `source:<id>`, `stage:<n>-<name>`, `layer:*`, `contract`, `owner:action`, `owner:decision`, `alert`,
`ticket`. Project "zohelo portfolio" fields: Product, Phase, Priority, Effort, Sprint, Target date, Status
(Status follows issue state and linked PRs through built-in automation).
