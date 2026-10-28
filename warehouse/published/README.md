# Published layer contract

Contract of 06_published/v1 — see docs/published.md. Consumers copy fixtures; change schemas only with a contract-labelled PR.

- `schemas/`: one JSON Schema (draft 2020-12) per JSON file of the contract.
- `fixtures/`: illustrative files in the published layout (`generated_at` 2026-01-01T00:00:00Z marks them as examples); `python -m ops.repo_check` checks them against the schemas.
