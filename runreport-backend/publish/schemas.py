"""Table schemas / column lists for publishing.

Populate these lists once the target Supabase tables are finalized.
"""

from __future__ import annotations


TABLE_SCHEMAS: dict[str, dict[str, object]] = {
    # Core publish targets (placeholder columns)
    "EscapementReports": {
        "required_columns": [],
        "delete_filter": None,
    },
    "EscapementReport_PlotData": {
        "required_columns": [],
        "delete_filter": None,
    },
    "Columbia_FishCounts": {
        "required_columns": [],
        "delete_filter": None,
    },
    "NOAA_flows": {
        "required_columns": [],
        "delete_filter": None,
    },
    "USGS_flows": {
        "required_columns": [],
        "delete_filter": None,
    },
}

DATASET_TABLES: dict[str, list[str]] = {
    "columbia": ["Columbia_FishCounts"],
    "flows": ["NOAA_flows", "USGS_flows"],
    "escapement": ["EscapementReports", "EscapementReport_PlotData"],
}

# Placeholder registry + metadata tables for Escapement publishing.
REGISTRY_TABLES: dict[str, list[str]] = {
    "escapement": ["Escapement_PDFRegistry"],
}

METADATA_TABLES: list[str] = ["Dataset_Metadata"]
