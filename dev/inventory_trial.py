"""Dev trial (deleted before merge): platform_inventory, tracker or retire_tables (list only) over every namespace."""

import sys

from ops import platform_inventory, retire_tables, tracker

if __name__ == "__main__":
    tool = {"inventory": platform_inventory, "tracker": tracker, "retire": retire_tables}[sys.argv[1]]
    raise SystemExit(tool.main([]))
