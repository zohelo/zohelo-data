# Development lanes (scheduled agent sessions)

Owner decision 28 Oct 2026: four implementer lanes and one architect lane run as scheduled Claude sessions. Every
session starts fresh: it reads `AGENTS.md`, `docs/decisions.md`, `docs/backlog.md` and this file first, then works.

## Lanes

| Lane | Owns (issue key prefixes, see `<!-- zohelo:key=... -->` in each issue) | Repo of the work |
|---|---|---|
| 1 Platform & ops | `data/ops/`, `data/portal/`, `data/backlog/`, `data/warehouse/`, `data/metadata/`, `data/published/`, `data/ingestion/` | zohelo-data |
| 2 Sources | `data/src-` | zohelo-data (stage 10 in zohelo-bi, stage 11 in zohelo) |
| 3 zohelo-bi | `bi/` | zohelo-bi |
| 4 zohelo.com | `com/` | zohelo |
| Architect | triggered by the lanes: reviews and merges every open PR; writes Tasks; runs the sync; weekly status | all |

## Implementer session (lanes 1–4)

0. **Rework first.** Before picking anything new, list your lane's open PRs whose issue has label `changes-requested`
   (or whose latest architect comment asks for a change): make exactly the requested change, push, remove the label,
   reply on the PR, and trigger the architect for it (step 5).
1. **Pick.** List open issues of type Task or Bug in the lane's repos (`gh api 'repos/zohelo/<repo>/issues?state=open&type=Task&per_page=100&page=N'`,
   loop pages yourself). Keep those whose key starts with the lane's prefixes. Skip: label `in-progress` set less than
   4 hours ago (another run owns it), label `owner:action`, Tasks with an open PR that says `Closes #<n>`, and Tasks
   whose `depends_on` (read it in `.github/backlog/epics/*.yaml` of the repo, by key) are not all closed — except when
   every unclosed dependency has an open PR from this lane: then the Task is pickable as a stacked Task (step 4). Order: phase, then priority
   of the Epic (P1 first), then issue number.
2. **Claim.** Add label `in-progress` and a comment `claimed by <lane> run <UTC time>`.
3. **Deliver** exactly as the issue's "How to deliver" says: worktree from `origin/main`, branch `<type>/<n>-<topic>`,
   repo checks, the issue's Verify, one PR with `Closes #<n>` first. Verify-only Tasks: no PR; close with a comment
   linking the evidence run. Destructive Ops runs only as AGENTS.md allows.
4. **Never merge your own PR**; the architect merges. Keep working while PRs wait. A stacked Task branches from the
   dependency's PR branch (not main); its PR targets `main` and has `Depends on #<PR>` under `Closes #<n>`. Stack at
   most 3 deep. When a dependency has merged (squash), rebase the stacked branch with
   `git rebase --onto origin/main <old dependency head>` and force-push with lease.
5. **Trigger the architect** after every 5 PRs and at the end of the run: fire the scheduled task "zohelo architect ·
   review, merge, plan" (fire_trigger, id in the session prompt) with the text `Review lane <n>: PRs <list>`. If firing
   is refused, say so in the summary.
5a. **Follow your PRs while you work.** About every 20 minutes, and before each new Task, check the PRs you opened this
   run: merged → rebase anything stacked on it (step 4); sent back (`changes-requested` or an architect comment asking
   for a change) → fix it now, push, reply, re-trigger the architect; then continue with the next Task.
6. **Stuck?** If a step is impossible as written or needs a decision, comment on the issue, remove `in-progress`, add
   label `design:question`, and take the next Task. Never improvise around the issue.
7. **Repeat**, aiming for 10–20 PRs per run, until about 3 hours have passed or nothing in the lane is pickable. Before ending, wait up to
   30 minutes for the architect to finish reviewing your open PRs (check every 5 minutes) and handle every result as in
   step 5a; then remove `in-progress` from anything you did not finish, and end with a summary: PRs opened, Tasks closed, questions raised.

## Architect session

Triggered by the lanes (and once a week, Monday 08:23 Warsaw, for the weekly status). Several triggers can overlap:
before reviewing a PR add label `reviewing` to it and skip PRs that already carry it (added less than 1 hour ago);
remove the label when done.

1. **Review and merge** every open non-Dependabot PR in the three repos, oldest first: compare the diff with its issue's
   Steps, Acceptance and Out of scope; read the check results. Merge (squash) when checks pass and the review finds no
   problem; otherwise comment the exact change needed on the PR and label the issue `changes-requested` (the lane fixes
   it next run). Rebase conflicting PRs when the fix is mechanical.
2. **Dependabot**: merge green minor/patch PRs; close PRs made redundant; failing ones become a Bug Task in lane 1.
3. **Questions**: answer `design:question` issues from the plan and `docs/decisions.md`; record new decisions there;
   anything that is the owner's call goes to the weekly status (max 3 open owner items).
4. **Tasks**: when a lane has fewer than 5 pickable Tasks, write the next Tasks into `.github/backlog/` (spec:
   `docs/backlog.md`) from the Deliverables' `todo` lines, open a PR, merge it, and run Ops `backlog_sync apply`.
5. **Weekly (Monday run)**: status update for the owner — merged PRs per lane, Epics' progress, risks, ≤ 3 decisions.

## Shared rules

- Permissions recorded in AGENTS.md and `docs/decisions.md` (O8, O10) apply; if the session refuses an action, report
  it in the summary instead of working around it.
- Commits end with `Co-Authored-By:` and `Claude-Session:` lines as the session's attribution instructions say.
- GraphQL and `gh pr create` are not available in sessions: use REST (`gh api`).
- Log downloads are blocked: read workflow results from check-run annotations.
