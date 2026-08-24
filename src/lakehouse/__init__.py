"""cloud-lakehouse-platform: a governed, incremental Azure Delta lakehouse.

Layers
------
bronze : immutable, append-only landing of raw source records + ingest metadata
silver : deduplicated, conformed, quality-enforced business entities (SCD2 where needed)
gold   : dimensional star schema + serving aggregates consumed by Databricks SQL / Power BI
"""

__version__ = "1.0.0"

LAYERS = ("bronze", "silver", "gold")
