# Architect standards

Read at the start of every run, after AGENTS.md, docs/decisions.md and docs/team.md. Format: [README](README.md).

## Standards

- **Views a newcomer can read**: C4 level 1 (context) and 2 (containers) for every product; level 3 only where it
  helps. https://c4model.com/
- **Write significant decisions down with their trade-offs** (ADR: status, context, decision, consequences,
  alternatives). https://openpracticelibrary.com/practice/architectural-decision-records-adr
- **Check architecture properties by machine**: each property that matters has a fitness function with a threshold.
  https://www.thoughtworks.com/radar/techniques/architectural-fitness-function
- **Contracts before code**: keys, schemas, versioning, one writer, known readers (D5, D24, docs/published.md).
- **Prefer reversible steps**: expand/contract for changes to shared data (D6); free tiers first (AGENTS.md).
- **Review for correctness, design and fit**, not style that a tool checks. Google, *What to look for in a code
  review*: https://google.github.io/eng-practices/review/reviewer/looking-for.html
- **Debt is a ranked register**, interest × risk, not a feeling (docs/team.md, Architect).

## Checklist

- Start: open PRs named in the start message; the top claimed `architecture` item.
- Each review: diff against the issue's Steps, Acceptance and Out of scope; checks; one review per head.
- Each run: at most 10 reviews, then one architecture step; after the cut-off, reviews only.
- End: re-list open PRs; `Lesson:` line in the summary when something went wrong or a better way was found.

## Lessons

- 2026-W44: review each PR head once; repeated reviews of the same head (zohelo-bi#181 five times) spend the shared
  GitHub write limit without adding anything.
