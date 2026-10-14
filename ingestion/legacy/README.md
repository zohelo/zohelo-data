# Legacy ingestion code

Retired code kept until each source is rebuilt under the extraction strategy in
[docs/landing.md](../../docs/landing.md). Nothing here is run by the Pipeline.

| Folder | Content | Removed when |
|---|---|---|
| `adapters/` | API adapters for GUS BDL and GUS DBW (Eurostat and World Bank WDI rebuilt, adapters removed 14 Oct 2026) | that source is rebuilt |
| `gus_bdl_web/` | GUS BDL Web UI bulk download workers (Node, Playwright) | GUS BDL is rebuilt |
