from __future__ import annotations

from vajra_regime import paths


FOUNDATION_VERSION = "NIFTY500_POINT_IN_TIME_V1"
INVALID_SYMBOL_TOKENS = frozenset(
    {
        "FINANCE",
        "HOTELS",
        "MISCELLANEOUS",
        "PHARMACEUTICALS",
        "PUBLICATION",
        "REFINERIES",
        "SHIPPING",
        "T&D",
        "TRADING",
        "WER",
    }
)
DATA_ROOT = paths.NIFTY500_PIT
CHECKPOINT_ROOT = paths.NIFTY500_PIT_CHECKPOINTS

GOOGLE_FREEZE_EVIDENCE = [
    {
        "spreadsheet_id": "1N-muJk9iLRRu23dLkptPQ2BdiOPZUB16UYTiledSaXY",
        "title": "AUTOMATIC 750 MARKET BREADTH",
        "current_revision_id_at_freeze": "46320",
        "sheet_count": 16,
        "read_only_inspected": True,
    },
    {
        "spreadsheet_id": "1QSEKmPMaMQk90oCXhVnXcNZWDKXR34qhacCj870Z0Po",
        "title": "VAJRA 500 SCANNER",
        "current_revision_id_at_freeze": "31689",
        "sheet_count": 1,
        "read_only_inspected": True,
    },
]
