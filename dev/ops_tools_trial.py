"""Trial of the two new ops scripts (read-only)."""
from ops import catalog_maintenance, landing_objects

catalog_maintenance.main([])
landing_objects.main(["01_landing/nbp/", "--peek", "--limit", "1"])
