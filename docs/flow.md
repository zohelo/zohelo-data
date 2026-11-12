# How work flows (the factory)

Owner request, 11 November 2026: Claude runs the project as an orchestrator of background agents in one session,
always with at least one agent working, and every change passes the same stations. Merging follows O34; the
orchestrator's role follows O38; models follow the owner's rules of 12 November ("Model policy" below).

## Orchestrator (O38)

The orchestrator session runs on **Opus 5.5** as product owner and DevOps lead.

- **Owns:** turning the direction into a prioritised backlog and briefs; routing work to stations and models; the
  merge gate (checks green, an independent review without blockers, and the reviewed head SHA unchanged);
  production dispatches and watching them; agent and runner limits ("Concurrency" below); feeding review lessons
  into the briefs and the agents' checklists; the status page and the "Needs you" list.
- **Does not:** write code or models, review, or take design decisions with lasting consequences. An orchestrator
  that builds cannot be reviewed independently (O34).
- **Escalates to the `architect` agent (Fable):** architecture, the published contract, UX direction (with
  `ux-designer`), research questions, and trade-offs between options.
- **Escalates to the owner:** money, accounts and agreements, legal readings, and what zohelo should become.
- **Why Opus:** routine routing should not spend Fable.

## Stations

| # | Station | Label | Who | Leaves the station when |
|---|---|---|---|---|
| 1 | Intake | – | orchestrator; for a large sweep the `clerk` agent lists the facts and the orchestrator classifies | the issue is classified done / obsolete / relevant, with priority (P1–P3) and size (S/M/L) |
| 2 | Spec | `flow:spec` | orchestrator for a brief that applies a decided design; `architect`, `ux-designer` or `owner-researcher` (Fable) when the escalation rule applies; `source-researcher` for a source | the issue body says what "done" means and how it is checked; skipped when it already does |
| 3 | Ready | `flow:ready` | – | a builder can start (work-in-progress rule below) |
| 4 | Build | `flow:build` | a builder agent (`source-builder`, `gold-modeller`, `portal-builder`, or a general Opus agent with a brief), in its own git worktree or clone | the PR is open, linked with `Closes #n`, `python -m ops.repo_check` passes locally, and the builder ran its own verification |
| 5 | Review | `flow:review` | an independent review agent that did not write the change: `data-quality-reviewer` (source and Gold), `publish-contract-reviewer` (published layer and its readers), `code-reviewer` (everything else), plus `ux-designer` for a change users see | verdict merge / merge after fixes / do not merge, with file:line findings |
| 6 | Rework | `flow:rework` | the same builder, resumed with the findings | fixes pushed as follow-up commits, checks green; back to Review only if the reviewer asked for it or a blocker was fixed |
| 7 | Merge | `merge:check` (on the PR) | orchestrator (O34: `check` green on the final head, no blockers, and the head SHA is the one the reviewer named) | merged; the owner is told what went in |
| 8 | Verify | `flow:verify` | orchestrator; `clerk` extracts the run evidence | the next scheduled production run proves it (data), or the deploy and UI suite do (portal); then the issue closes |
| – | Owner | `flow:owner` | owner | business, legal, public, AGENTS.md and "Ask the owner first" items; at most three on the "where we are" page |

## Rules

- **Work in progress:** sized by review and runner capacity ("Concurrency" below): reviews before new builds, at
  most 4 Dev runs in flight, never two builders in the same files.
- **Rework limit:** two rounds. A third failure goes back to Spec with the `architect` instead of a third attempt.
- **Never idle:** when an agent finishes, the orchestrator reviews its result and starts the next item from Ready
  in the same step; Ready holds at least three items.
- **Source work** (O36) follows [playbook.md](playbook.md) with the project agents in `.claude/agents/`:
  `source-researcher` at Spec, `source-builder` and `gold-modeller` at Build, `data-quality-reviewer` at Review.
- **One roster:** the agents for all three repositories live in zohelo-data `.claude/agents/`, because the
  orchestrator session starts here; an agent works in a clone or worktree of the repository its brief names.
  Each file sets the model, the tools (reviewers and `owner-researcher` cannot edit), the skills to load and a
  pre-PR or review checklist.
- **Lessons loop:** a defect that review finds twice becomes a line of the builders' pre-PR checklist and of the
  reviewer's checklist in `.claude/agents/`, in the PR that fixes it or the next one. The brief of a dispatch names
  only what is new since the agent file was last changed.
- **Evidence over claims:** a builder reports what it ran; a reviewer reads the diff, not the builder's summary.
  Nothing is "done" before Verify.
- **Refusals stand:** a permission or safety refusal is reported, never worked around.
- **Merge gate (owner direction 12 Nov 2026: no Actions run on every push):** pushes to a PR start no workflow.
  Builders and rework run `python -m ops.repo_check` (and `npm run lint`/`typecheck` in `portal/app` for portal
  changes) locally before every push. When the PR is reviewed and final, the orchestrator adds the label
  `merge:check`; `check` (the required status check) runs once on that head, and `check-portal` after it when
  `portal/app` changed. Green → merge (squash). A push after the label leaves the new head without `check`:
  remove and re-add the label once the fix is pushed. "Re-run jobs" repeats `check` on the same commit only (for a
  flaky failure), never on a newer head.
- **Visibility:** the `flow:` labels on issues show the station in GitHub; the task list in the session shows the
  running lanes; `claude/where-we-are.md` in the claude.ai project shows the state and what needs the owner.

## Model policy

Owner's rules, 12 November 2026: **Fable 5.2** only for the most complex work (architecture, design, advanced
research, mostly non-technical work, UX/UI, ideas, concepts); **Opus 5.5** for most standard work; **Sonnet** and
**Haiku** only where they add value for the simplest tasks. Each agent file sets its model; the orchestrator
overrides it per dispatch only as this table says.

| Station or task | Model | Agent | Why |
|---|---|---|---|
| Orchestration: backlog, briefs, routing, merge gate, production runs, status page | Opus | the session | O38 |
| Architecture; the published contract; a new fact kind, grain or shared dimension; a trade-off between options; the spec after two failed reworks | Fable | `architect` | lasting consequences, no document decides it yet |
| UX direction, information architecture, visual design, design review of a change users see | Fable | `ux-designer` | design is judgement, and the owner reads the result first |
| Research for the owner: strategy, market, legal background, ideas, concepts, non-technical briefs | Fable | `owner-researcher` | open questions, mostly non-technical |
| Source review: route, limits, licence, approved or parked | Opus | `source-researcher` | a wrong licence or route reading is expensive and was found at review; not a task for a cheaper model |
| Source build, Landing to Silver and the schedule | Opus | `source-builder` | standard work with a playbook |
| Gold for one source: build stage, seeds, metrics, under the decided model | Opus | `gold-modeller` | applies `docs/model.md`; a question the model does not answer goes to `architect` |
| Portal and site code in zohelo-data `portal/`, zohelo-bi and zohelo | Opus | `portal-builder` | builds from a `ux-designer` spec or the design template |
| Other code: `ops/`, `run.py`, shared ingestion code, workflows, docs that apply a decision | Opus | general agent with a brief | standard work |
| Review of a source or Gold PR | Opus | `data-quality-reviewer` | needs a shell and the run evidence |
| Review of a PR that changes the published layer or what reads it | Opus | `publish-contract-reviewer` | one reviewer who reads both sides of the contract |
| Review of any other code PR | Opus | `code-reviewer` | six lenses in one context ("Review without sub-agents" below) |
| Inventories (issues, PRs, branches, register against evidence), register and label bookkeeping, branch-cleanup lists | Sonnet | `clerk` | a rule decides every row and a command checks it |
| Extracting lines and counts from job logs and run annotations | Haiku | `clerk`, dispatched with `model: haiku` | high volume, nothing to decide; the output is quoted lines with their run ids |

**When to escalate to Fable.** One of these is true, and the orchestrator can write it as one question with the
options it sees:

1. the answer outlives the pull request: it changes `docs/model.md`, `docs/published.md` or the schemas, a shared
   dimension, the repository structure, the design template, or what a reader of the portals sees first;
2. two or more options are workable and no document or decision picks one;
3. the question is non-technical or open-ended: research, positioning, wording for the public, an idea to judge;
4. a change failed review twice (rework limit), or two reviewers disagree on a blocker.

Fable answers with a spec, a decision proposal (a D row with how to undo it) or a review; the build that follows
goes back to an Opus builder. Applying a decided design, modelling one more source, code review and routing are
not escalations.

**When Sonnet or Haiku.** All three hold: the task has no judgement (a written rule decides every output row);
the orchestrator can check the result with a command or a sample of five rows; and nothing in the output is a
verdict, a licence reading, a priority or shipped code. Everything else is Opus. A task type whose `clerk` result
was wrong twice goes back to Opus and leaves the table. Sonnet is not used for "routine edits" of code or models:
the rework costs more than the model saves.

**Review without sub-agents.** In this setup an agent started by the orchestrator cannot start further agents, so a
reviewer cannot dispatch the review-council specialists or the PR-review toolkit's agents (skills do load inside an
agent). Each reviewer therefore runs the lenses itself, in one context, and names them in its `Skills used:` line as
`review-council lenses by hand` (O24: a missing skill is named and replaced, never skipped). For a PR that changes
shared code many sources run through (`ingestion/common/`, `run.py`, `warehouse/macros/`, `warehouse/publish.py`,
the workflows), the orchestrator may dispatch review-council specialists itself, next to the reviewer.

## Concurrency

The owner upgraded his plan on 12 November 2026 (18:16) and has no five-hour usage limit any more, so no cap is
derived from a usage window. The limits below have their own reasons; the orchestrator checks them before each
dispatch. Evidence for them, 11–12 November: GitHub runners queued when many agents dispatched Dev runs together,
and stale merges of a shared file were a review finding.

- **Agents in total:** no fixed number; sized by review and runner capacity. A new builder starts only when every
  open PR that waits for a review or a rework has an agent on it, and when a Dev slot will be free for its trial.
  Fable agents (`architect`, `ux-designer`, `owner-researcher`) start when their station needs them.
- **Reviews before new builds.** A waiting review or rework is dispatched before a new build: finished work that
  is not merged is the most expensive kind.
- **Short briefs, resumed agents:** rework resumes the same builder instead of starting a new one, and a brief
  names files and issue numbers instead of pasting them, so no agent re-reads what it already holds.
- **Dev runs:** at most 4 in flight across all agents and one per agent. A builder runs one trial that covers
  Landing, Bronze, Silver (and Gold) with every check in it, instead of a run per step; a researcher probes all
  its URLs in one run. Before dispatching, an agent counts the queued and running Dev runs
  (`gh api "repos/zohelo/zohelo-data/actions/runs?status=queued&per_page=50"`, the same with `in_progress`) and
  waits when four are in flight or any Pipeline run is queued: scheduled production runs go first.
- **Waiting is cheap, polling is not:** an agent that waits for a run checks it once a minute at most, and reads
  the annotations once, when the run has ended.
- **One writer per shared file at merge time:** PRs that touch the same shared file merge one at a time; the next
  one is updated from `main` and its checks rerun before it merges (stale merges were a review finding), unless
  they go in one merge train.
- **Merge trains** (introduced 12 November 2026): reviewed PRs that touch shared append-only files (for example
  `ingestion/register.yaml`, `docs/decisions.md`) are combined into one train branch by a merge operator. The
  operator merges each PR's reviewed head SHA into the train, resolves the appends, runs the checks once on the
  train head and merges the train into `main` with a merge commit, so each contained PR's commits reach `main`
  and GitHub marks those PRs merged. A PR whose head changed after its review stays out of the train.
