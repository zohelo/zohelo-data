"""Dry run of the BDL metadata extract for a few subgroups plus the unit list (no writes)."""
from ingestion.to_landing.gus_bdl import metadata

raise SystemExit(metadata.main(["--filter", "P2497,P2504,P3446,P1754", "--units", "--dry-run", "--budget-minutes", "30"]))
