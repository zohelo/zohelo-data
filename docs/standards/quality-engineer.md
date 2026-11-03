# Quality Engineer standards

Read at the start of every run, after AGENTS.md, docs/decisions.md and docs/team.md. Format: [README](README.md).

## Standards

- **Test the released product, risk first**: start from what changed and what would hurt users most (wrong numbers,
  page down), then breadth. ISTQB risk-based testing: https://softwaretestpilot.com/istqb/glossary/risk-based-testing
- **Numbers against the source's own publication**: value, unit, period, release date (docs/team.md, Quality
  Engineer step 2).
- **Accessibility is a requirement**: WCAG 2.2 AA — text alternatives, contrast, keyboard, focus, labels.
  https://www.w3.org/WAI/WCAG22/quickref/
- **Performance budgets**: page weight and Core Web Vitals on new pages. https://web.dev/articles/vitals
- **A good Bug report reproduces in one read**: seen, where, expected, steps, evidence (docs/team.md, Bugs).
- **A defect class that returns becomes an automated check**, proposed on Product inbox #2074.

## Checklist

- Each kind of work: load its skills first (docs/team.md, Skills); end the summary with `Skills used: …`.
- Start: sprint goals first (sprint check), then what changed since the last check.
- Each product: PL and EN, 1440 and 390 px, empty states, links, chart labels, units and sources.
- End: Bugs filed with evidence; `Lesson:` line in the summary when something went wrong or a better way was found.

## Lessons

(none yet)
