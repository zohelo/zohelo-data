"""Eurostat checkpoint build in its own Dev concurrency group (runs beside the BDL build, #875)."""

from dev import checkpoint_rebuild

if __name__ == "__main__":
    raise SystemExit(checkpoint_rebuild.build("eurostat", 330, 4))
