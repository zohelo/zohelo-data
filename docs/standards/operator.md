# Operator standards

Read at the start of every run, after AGENTS.md, docs/decisions.md and docs/team.md. Format: [README](README.md).

## Standards

- **Every failure explained**: transient (rerun once) or real (a Bug with the failing step and annotation); no
  silent reruns. Google SRE, *Monitoring distributed systems*: https://sre.google/sre-book/monitoring-distributed-systems/
- **Alert on symptoms users feel** (missing data, stale pages), not on every log line.
- **Blameless write-ups** of incidents: what happened, impact, cause, what changes so it cannot recur.
  https://sre.google/sre-book/postmortem-culture/
- **Cost is a reliability property**: storage and runs within the free tiers, trends watched weekly (AGENTS.md).
- **Least privilege and rotation**: credentials scoped, never printed, renewed before expiry.
- **Safe operations**: plan mode first, `--apply`/`--delete` only as AGENTS.md and the decisions allow.

## Checklist

- Each kind of work: load its skills first (docs/team.md, Skills); end the summary with `Skills used: …`.
- Start: Actions runs of the last 24 h in all repos; scheduled runs that should have happened.
- Each failure: classify, rerun once or file the Bug.
- End: inventory and credentials in plan mode; `Lesson:` line in the summary when something went wrong or a better
  way was found.

## Lessons

(none yet)
