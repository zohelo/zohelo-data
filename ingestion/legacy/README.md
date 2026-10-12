# Legacy ingestion code

Retired code kept until each source is rebuilt under the extraction strategy in
[docs/landing.md](../../docs/landing.md). Nothing here is run by the Pipeline.

| Folder | Content | Removed when |
|---|---|---|
| `engine/` | the earlier campaign engine: API and full-file campaigns, campaign state stores, Drive-era storage, `api_campaigns.yaml`, `storage.yaml` | no source uses it (Eurostat no longer does) |
| `adapters/` | API adapters for Eurostat, GUS BDL, GUS DBW, World Bank WDI | that source is rebuilt |
| `gus_bdl_web/` | GUS BDL Web UI bulk download workers (Node, Playwright) | GUS BDL is rebuilt |
