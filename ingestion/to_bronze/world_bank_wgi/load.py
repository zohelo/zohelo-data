#!/usr/bin/env python3
"""World Bank WGI bronze.load: newest Landing copy of WGI_CSV.zip → bronze.wgi_observations, bronze.wgi_metadata.

Decoders shared with the other DataBank bulk archives: ingestion/common/databank_bronze.py.
"""

from __future__ import annotations

from ingestion.common import databank_bronze

SOURCE = "world_bank_wgi"


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: one archive of a few MB."""
    return databank_bronze.load(SOURCE, "wgi", landing_root=landing_root, namespace=namespace)
