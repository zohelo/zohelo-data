# Developer standards

Read at the start of every run, after AGENTS.md, docs/decisions.md and docs/team.md. Format: [README](README.md).

## Standards

- **Small, single-purpose PRs** that do exactly the Task: reviewers find more defects in small changes, and a small
  PR merges before main moves. Google, *Small CLs*: https://google.github.io/eng-practices/review/developer/small-cls.html
- **Run the repo's full check before pushing**, not only the Task's Verify (docs/team.md, Developer step 3); a red
  check costs a review round.
- **Change only the lines the Task changes**; format only touched files. Unrelated diffs hide the real change.
- **Write the PR for the reviewer**: what changed, why, how it was verified, what is out of scope. Google, *Writing
  good CL descriptions*: https://google.github.io/eng-practices/review/developer/cl-descriptions.html
- **Make illegal states hard**: validate inputs at the boundary, fail loudly with a clear message, bounded retries
  and timeouts (AGENTS.md, Data safety).
- **Accessible UI by default** (`ui` PRs): semantic HTML, labels, keyboard, contrast to WCAG 2.2 AA:
  https://www.w3.org/WAI/WCAG22/quickref/
- **Debt you see but do not fix** is filed as a `tech-debt` issue (docs/team.md, Architect), not fixed off-Task.

## Checklist

- Start: rework of own PRs first; read the Task's Steps, Acceptance, Out of scope and linked spec.
- Before the PR: full repo check green; diff contains only the Task; `Closes #<n>` first; `ui` label and routes for
  visible changes; `kind:content` label and the changed texts and values when the PR changes what an indicator or
  chart says.
- End: unfinished claims released; `Lesson:` line in the summary when something went wrong or a better way was found.

## Lessons

- 2026-W45-S2: format only touched files (`bunx prettier --write <files>`), never the whole repo; three PRs were sent
  back for wholesale reformatting (zohelo-bi#269, #270, zohelo#328).
- 2026-W44: never force-push or amend a pushed commit; update a branch by merging main into it (docs/team.md,
  shared rules).
