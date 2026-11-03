# Coach standards

Read at the start of every run, after AGENTS.md, docs/decisions.md and docs/team.md. Format: [README](README.md).

## Standards

- **Fix the system, not the run**: a mistake seen twice is a cause in a rule, a template or a check; change it there.
  Blameless postmortems: https://sre.google/sre-book/postmortem-culture/
- **Retrospective as inspect and adapt**: keep / change / try, at most 3 changes a night, each with evidence. Scrum
  Guide 2020: https://scrumguides.org/scrum-guide.html
- **Checklists over memory** for steps that are skipped under pressure. Atul Gawande, *The Checklist Manifesto*
  (summary): https://readingraphics.com/book-summary-the-checklist-manifesto/
- **Verbal reflection works for agents**: a short written lesson read at the next attempt improves the next attempt.
  Shinn et al., *Reflexion* (2023): https://arxiv.org/abs/2303.11366
- **Context is a budget**: rules short and specific, the smallest set of high-signal tokens. Anthropic, *Effective
  context engineering for AI agents*: https://anthropic.com/engineering/effective-context-engineering-for-ai-agents
- **Automate a rule when you can** (`ops/repo_check.py`); a rule a machine checks is never forgotten.
- **Never lower a quality bar** without an owner decision.

## Checklist

- Each kind of work: load its skills first (docs/team.md, Skills); end the summary with `Skills used: …`.
- Start: Team log since the last Coach entry; `Lesson:` lines; sent-back PRs; refusals; failed runs.
- Each change: recurring cause (twice or more), evidence, one PR.
- End: retro comment; standards curated; Team log entry; `Lesson:` line when something went wrong or a better way was
  found.

## Lessons

- 2026-W45-S2: a refusal that is not a merge or a start is reported once and not retried by later runs (closing
  zohelo#327 and #315–#317 was refused for four runs).
