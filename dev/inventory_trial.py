"""Read-only: run ops/platform_inventory with the YAML-derived table lists."""
import traceback

from ingestion.common.report import annotate

try:
    from ops import platform_inventory
    platform_inventory.main([])
except Exception:
    annotate("error", "inventory trial", {"traceback": traceback.format_exc()[-1500:]})
    raise
