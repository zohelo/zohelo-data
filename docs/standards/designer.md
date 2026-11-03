# Designer standards

Read at the start of every run, after AGENTS.md, docs/decisions.md and docs/team.md. Format: [README](README.md).

## Standards

- **Research before taste**: users, jobs and devices first; decisions cite evidence (docs/team.md, Design process).
- **Usability heuristics** as the review lens: visibility of status, match with the real world, consistency, error
  prevention, recognition over recall. Nielsen Norman Group: https://www.nngroup.com/articles/ten-usability-heuristics/
- **Accessibility is a requirement**, WCAG 2.2 AA: https://www.w3.org/WAI/WCAG22/quickref/
- **Design tokens, not values**: colour, type, spacing and radius come from the Design System artifact's tokens,
  which `@zohelo/design` implements. W3C Design Tokens
  Community Group format: https://www.designtokens.org/
- **Charts that tell the truth**: the right chart for the relationship, direct labels, units and sources. FT *Visual
  vocabulary*: https://github.com/Financial-Times/chart-doctor/tree/main/visual-vocabulary
- **Data-dense but calm**: hierarchy through type and space, colour for meaning only. GOV.UK Design System as a public
  data reference: https://design-system.service.gov.uk/
- **Review from screenshots against the artboards, never the code**: 1440 and 390 px, PL and EN (O23).
- **Design in Design artifacts**: directions, the Design System artifact and page specs are artifacts from the
  account's artifact types, not HTML in the repos (O23).

## Checklist

- Each kind of work: load its skills first (docs/team.md, Skills); end the summary with `Skills used: …`.
- Start: `ui` and `kind:content` PRs waiting for a design review.
- Each review: screenshots (the PR's automatic ones, else rendered locally only to look), compare with the artboards; `Design review: approved (<head sha>)` or exact changes.
- End: one concrete design-process output; `Lesson:` line in the summary when something went wrong or a better way was
  found.

## Lessons

(none yet)
