# Domain expert standards

Read at the start of every run, after AGENTS.md, docs/decisions.md and docs/team.md, together with
`docs/domains/<domain>.md` for the run's domain. Format: [README](README.md).

## Standards

- **Definitions from the producer's methodology**: concept, unit, frequency, seasonal adjustment, real vs nominal,
  base year, breaks. Eurostat metadata (ESMS): https://ec.europa.eu/eurostat/web/metadata
- **Comparability before comparison**: same concept, coverage and period across sources and countries.
- **Honest charts**: zero baselines for bars, consistent scales, units and sources on every chart. Financial Times
  *Visual vocabulary*: https://github.com/Financial-Times/chart-doctor/tree/main/visual-vocabulary
- **Revisions and release calendars matter**: say which vintage a number is and when it changes.
- **Statistical quality dimensions**: relevance, accuracy, timeliness, comparability, coherence. European Statistics
  Code of Practice: https://ec.europa.eu/eurostat/web/quality/european-quality-standards/european-statistics-code-of-practice
- **The knowledge file is the memory**: every run leaves the domain file better than it found it.

## Checklist

- Each kind of work: load its skills first (docs/team.md, Skills); end the summary with `Skills used: …`.
- Start: the domain file; the item in the start message.
- Each review (specs, Tasks, `kind:content` PRs on their current head): `Domain review (<domain>): approved` (on a PR
  `approved (<head sha>)`) or the exact changes; judge what readers see and what it says, not the code (O23).
- End: domain file updated by PR when something was learned; `Lesson:` line in the summary when something went wrong
  or a better way was found.

## Lessons

(none yet)
