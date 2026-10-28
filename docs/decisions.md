# Decisions

Decisions that bind implementers; read this first, after AGENTS.md.

| id | date | question | decision | by |
|---|---|---|---|---|
| O1 | 2026-10-27 | Phases | Phases 0–3 approved. | owner |
| O2 | 2026-10-27 | Old raw data | Delete ≈30 GB of duplicate and old-pipeline raw data. | owner |
| O3 | 2026-10-27 | Markets data | Finnhub removed; markets use ECB, NBP and World Bank open data. | owner |
| O4 | 2026-10-27 | Licence | No code licence; all rights reserved. | owner |
| O5 | 2026-10-27 | Analyst portfolio | Analyst portfolio runs under the owner's name. | owner |
| O6 | 2026-10-27 | zohelo-bi sign-in | zohelo-bi sign-in only at v1. | owner |
| O7 | 2026-10-27 | Org secrets | Org secrets stay visible to all public repos (A-10 only on the owner's request). | owner |
| O8 | 2026-10-27 | Standing permission | Standing permission: Claude closes pull requests and issues, deletes merged branches, and creates, edits and closes issues and project items in the zohelo organisation without asking; Claude dispatches Ops runs with --apply or --delete only for a Task whose Epic cites an owner decision; otherwise the run is an owner action. | owner |
| O9 | 2026-10-27 | Shared repo and branches | No .github or other shared repo; merged branches are deleted by branch-cleanup.yml. | owner |
| O10 | 2026-10-27 | Merging | Claude and the agents it dispatches merge pull requests once their checks pass (squash). | owner |
| O11 | 2026-10-28 | Development cadence | Four implementer lanes (platform, sources, zohelo-bi, zohelo.com) and an architect lane run as scheduled sessions every 3 hours 07–22 Warsaw time, each run aiming for 10–20 PRs (stacking on its own open PRs); the lanes trigger the architect, which reviews and merges (plus a Monday run for the weekly status); see docs/team.md (renamed Developers 1–4 and Architect, O13). | owner |
| O12 | 2026-10-27 | Entity-model Silver | The 9 Silver tables written by warehouse/entity/*.sql are current and kept (tracker fixed, #1036). | owner |
| O13 | 2026-10-28 | Team | The lanes become a team of members defined in docs/team.md: Developers 1–4, Architect, Product Owner, Quality Engineer, Operator, Domain expert (one member, one knowledge file per domain in docs/domains/) and Coach. Every member can be started on demand by any other member or the owner; schedules are only a safety net. Each scheduled task's instruction only points to docs/team.md. New products (zohelo-journal, zohelo-research, zohelo-products, growth) go through discovery briefs first; when the owner approves a brief, the product gets its repo, backlog and its own Developer. zohelo-consulting is the owner's call. | owner |
| O15 | 2026-10-28 | Sprints | The team works in sprints matching the 5-hour usage window (S1 07–12, S2 12–17, S3 17–22 Warsaw), each with one goal set by the Product Owner and closed by a release document (GitHub Release `zohelo-<date>-s<n>` in zohelo-data) and a project status update; every merge still deploys to production. Developers' scheduled runs move to the sprint starts. See docs/team.md. | owner |
| D1 | 2026-10-27 | Bootstrap | Bootstrap issues by REST until the first sync. | architect |
| D2 | 2026-10-27 | Destructive runs | Destructive Ops runs only for Tasks citing an owner decision. | architect |
| D3 | 2026-10-27 | Design package | @zohelo/design ships as a GitHub Release tarball. | architect |
| D4 | 2026-10-27 | Chart code | Chart code moves to zohelo first; pivot and transforms stay in zohelo-bi packages/semantic. | architect |
| D5 | 2026-10-27 | Published layer | Published-layer contract written by zohelo-data first: unit_key, geo/<level>.geojson, gold.publish sole writer. | architect |
| D6 | 2026-10-27 | Gold cut-over | Gold v1 cut-over is expand/contract. | architect |
| D7 | 2026-10-27 | bi credentials | zohelo-bi reads the lakehouse with read-only credentials. | architect |
| D8 | 2026-10-27 | Owner actions | At most 3 open owner actions. | architect |
| D9 | 2026-10-27 | Branch cleanup | branch-cleanup.yml only (no other branch-deletion mechanism). | architect |
| D10 | 2026-10-27 | Branch names | Branches are <type>/<issue-number>-<topic>. | architect |
| D11 | 2026-10-27 | Verify-only | verify-only label and run-count rules. | architect |
| D12 | 2026-10-27 | Implementer reading | Implementers never read claude.ai docs. | architect |
| D13 | 2026-10-27 | Decision log | This log is the one decision log implementers read first. | architect |
| D14 | 2026-10-27 | Cross-repo Tasks | Cross-repo Tasks sit under a Deliverable with repo:. | architect |
| D15 | 2026-10-27 | Secrets scope | Secrets scope changes only on the owner's request. | architect |
| D16 | 2026-10-28 | Formatter scope | Repo formatters (Prettier in zohelo-bi and zohelo) ignore `.github/backlog/`: backlog YAML keeps the layout of docs/backlog.md in all three repos. | architect |
| D17 | 2026-10-28 | Backlog-refill PRs | A PR that only writes Tasks into `.github/backlog/` closes a one-off Task issue "Architect: write the next lane Tasks (<date>)" created by REST without a key marker (the PR check needs an open Task); the sync does not manage it. | architect |
| D18 | 2026-10-28 | Scheduled-run filter | A scheduled source run is a Pipeline run with event `schedule`, or event `workflow_dispatch` with triggering actor `github-actions[bot]` (the scheduler dispatches with the workflow token); the scheduler's own runs are `zohelo-automation[bot]`; the owner's login means manual. Observe Tasks (D11) count only scheduled runs. | architect |
| D19 | 2026-10-28 | Gold v1 calendar | dim_date's SQL moves into dim_calendar as CTEs (no ref); Gold v1 has 30 models and no dim_date (design question #1596). | architect |
| D20 | 2026-10-28 | bi API filter values | Quoting (single quotes doubled) is the SQL-injection defence; SAFE_VALUE keeps allowing `'`, `;` and spaces so names like "Côte d'Ivoire" work (design question zohelo-bi#104). | architect |
| D21 | 2026-10-28 | zohelo.com theme | The site forces the light theme (`data-theme="light"` on `<html>`) until its components use token classes; dark mode arrives with the token adoption. | architect |
| D22 | 2026-10-28 | latest.json size | `kpi/<kpi_key>/latest.json` may be up to 100 kB (uncompressed): `values` keeps one entry per source × geography at country and voivodeship level, which is 45–64 kB on prod Gold (design question #2036). | architect |
| D23 | 2026-10-28 | GUS DBW terms | DBW is open-conditional under https://stat.gov.pl/copyright (copying and own analyses allowed with the source given; no named licence); the register never claims CC BY 4.0 for DBW (design question #1591). | architect |
| D24 | 2026-10-28 | bi published validators | zohelo-bi validates exactly what the published JSON Schemas declare and adds no rules of its own; patterns for kpi_key and timestamps are added to the zohelo-data schemas first, then mirrored in bi (design question zohelo-bi#99). | architect |
| D25 | 2026-10-28 | zohelo.com charts | Pages render @zohelo/design visuals (vega) only through a React Router `.client` module rendered after hydration (vega's Node build needs the optional `canvas` package, which breaks pre-rendering); first use `src/components/SampleChart.client.tsx` (zohelo#182). | architect |
| D27 | 2026-10-28 | Schema check name | `warehouse/publish.py` keeps `checked(coverage)` (per-source checked_at); the published-JSON schema check is `schema_checked(obj, schema)` (design question #2128). | architect |
