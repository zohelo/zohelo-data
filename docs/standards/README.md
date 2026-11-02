# Standards

One file per role: the current best practice, a short checklist and the lessons the team has learned, so a fresh
session starts at the team's best instead of at zero (owner, 2 Nov 18:32, O21). The rules of the team are in
[docs/team.md](../team.md); these files say *how to do the work well* and never override a rule there.

| Role | File |
|---|---|
| Developers 1–4 and every product Developer | [developer.md](developer.md) |
| Architect | [architect.md](architect.md) |
| Product Owner | [product-owner.md](product-owner.md) |
| Quality Engineer | [quality-engineer.md](quality-engineer.md) |
| Operator | [operator.md](operator.md) |
| Domain expert | [domain-expert.md](domain-expert.md) (plus `docs/domains/<domain>.md`) |
| Designer | [designer.md](designer.md) |
| Coach | [coach.md](coach.md) |

**Format of every file** (keep it under about 60 lines so reading it is cheap):

- **Standards** — the practices that define good work in this role, each with one source link.
- **Checklist** — run start, before each output, run end; short enough to follow every run.
- **Lessons** — at most 12 one-line lessons, newest first, each `<sprint id>: <lesson> (<evidence link>)`. The Coach
  adds them from `Lesson:` lines in Team log summaries, merges duplicates, and moves a lesson into Standards or into a
  rule in docs/team.md once it has proved itself; the oldest lesson drops out when a 13th arrives.

Only the Coach edits these files (one PR per night at most); members propose through `Lesson:` lines and the weekly
research issue (docs/team.md, Learning).
