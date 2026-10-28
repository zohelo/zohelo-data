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
| O11 | 2026-10-28 | Development cadence | Four implementer lanes (platform, sources, zohelo-bi, zohelo.com) and an architect lane run as scheduled sessions every 3 hours 07–22 Warsaw time, each run aiming for 10–20 PRs (stacking on its own open PRs); the lanes trigger the architect, which reviews and merges (plus a Monday run for the weekly status); see docs/lanes.md. | owner |
| O12 | 2026-10-27 | Entity-model Silver | The 9 Silver tables written by warehouse/entity/*.sql are current and kept (tracker fixed, #1036). | owner |
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
