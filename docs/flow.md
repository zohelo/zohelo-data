# How work flows (the factory)

Owner request, 11 November 2026: Claude runs the project as an orchestrator of background agents in one session,
always with at least one agent working, and every change passes the same stations. Merging follows O34.

## Stations

| # | Station | Label | Who | Leaves the station when |
|---|---|---|---|---|
| 1 | Intake | – | Orchestrator, or a triage agent (Sonnet) for large sweeps | the issue is classified done / obsolete / relevant, with priority (P1–P3) and size (S/M/L) |
| 2 | Spec | `flow:spec` | Fable for open-ended work (design, strategy, scoring), Opus otherwise | the issue body says what "done" means and how it is checked; skipped when it already does |
| 3 | Ready | `flow:ready` | – | a build slot is free (work-in-progress limit below) |
| 4 | Build | `flow:build` | builder agent (Opus; Sonnet for routine edits), in its own git worktree or clone | the PR is open, linked with `Closes #n`, all checks green, and the builder ran its own verification |
| 5 | Review | `flow:review` | an independent review agent that did not write the change: review-council (correctness, security, interface, architecture, test evidence, policy) or the PR-review toolkit; `ui-ux-designer` (zohelo-bi, Fable) for portal UI | verdict merge / merge after fixes / do not merge, with file:line findings |
| 6 | Rework | `flow:rework` | the same builder, resumed with the findings | fixes pushed as follow-up commits, checks green; back to Review only if the reviewer asked for it or a blocker was fixed |
| 7 | Merge | – | orchestrator (O34: checks green and no blockers) | merged; the owner is told what went in |
| 8 | Verify | `flow:verify` | orchestrator | the next scheduled production run proves it (data), or the deploy and UI suite do (portal); then the issue closes |
| – | Owner | `flow:owner` | owner | business, legal, public, AGENTS.md and "Ask the owner first" items; at most three on the "where we are" page |

## Rules

- **Work in progress:** at most 3 builders and 2 reviewers at a time; never two builders in the same files.
- **Rework limit:** two rounds. A third failure goes back to Spec (often Fable) instead of a third attempt.
- **Never idle:** when an agent finishes, the orchestrator reviews its result and starts the next item from Ready
  in the same step; Ready holds at least three items.
- **Model per station:** Fable for open-ended specs and judgement calls; Opus for building and for reviews of
  data and code; Sonnet for triage and sweeps; Haiku for mass mechanical checks.
- **Source work** (O36) follows [playbook.md](playbook.md) with the project agents in `.claude/agents/`: `source-researcher`
  (Sonnet) at Spec, `source-builder` (Opus) and `gold-modeller` (Fable) at Build, `data-quality-reviewer` (Opus) at Review.
- **Evidence over claims:** a builder reports what it ran; a reviewer reads the diff, not the builder's summary.
  Nothing is "done" before Verify.
- **Refusals stand:** a permission or safety refusal is reported, never worked around.
- **Documentation (O39):** each repo documents itself; zohelo-data holds the data platform and the published
  contract only. A document or decision that concerns only zohelo.com goes to zohelo/zohelo, only zohelo-bi to
  zohelo/zohelo-bi; a moved decision row keeps its id here as a pointer.
- **Visibility:** the `flow:` labels on issues show the station in GitHub; the task list in the session shows the
  running lanes; `claude/where-we-are.md` in the claude.ai project shows the state and what needs the owner.
