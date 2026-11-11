# Published layer contract

Contracts of 06_published/v1 and 06_published/v2 (zohelo-bi) — see docs/published.md. Consumers copy fixtures; change schemas only with a contract-labelled PR.

- `schemas/`: one JSON Schema (draft 2020-12) per JSON or GeoJSON file of the contract.
- `fixtures/`: illustrative files in the published layout (`generated_at` 2026-01-01T00:00:00Z marks them as examples); `python -m ops.repo_check` checks them against the schemas; one `kpi/<kpi_key>/latest.json` per published KPI except those in `publish.SKIP` (D26).
- `schemas/v2/`, `fixtures/v2/`: contract v2 (`catalog.json`, `latest.json`, `kpi/<kpi_key>/series.json`, `calendar.json`, `status.json`, `topics.json`); fixtures built by `warehouse/publish_v2.py` from synthetic Gold rows and also checked against the cross-file rules (`ops.repo_check.published_v2_problems`).
