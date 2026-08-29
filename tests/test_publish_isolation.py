"""Data must flow one way: store -> published.

If any build step ever read back from the published folder, a corrupted or truncated publish
could silently poison the next rebuild, and the failure-safety property the engine claims
would not hold.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

BUILD_MODULES = (
    "src/vajra_regime/nifty500_migration/production_pipeline.py",
    "src/vajra_regime/nifty500_migration/incremental_catchup.py",
    "src/vajra_regime/nifty500_migration/raw_ohlcv.py",
    "src/vajra_regime/nifty500_migration/certified_adjusted.py",
    "src/vajra_regime/nifty500_migration/timeline.py",
    "src/vajra_regime/rolling_master.py",
    "src/vajra_regime/monthly_universe.py",
    "src/vajra_regime/nse_live.py",
    "src/vajra_regime/corporate_actions.py",
    "config/default.yaml",
)


def test_build_modules_never_read_the_published_dataset() -> None:
    for relative in BUILD_MODULES:
        text = (REPO_ROOT / relative).read_text(encoding="utf-8")
        assert "NIFTY_DATA_SHARE_CENTER" not in text, relative
        assert "paths.DATA_ROOT" not in text, relative
        assert "PUBLISHED_" not in text, relative
        # The literal published root, as opposed to the VAJRA_DATA_ROOT env var name, which
        # config/default.yaml legitimately mentions in a comment.
        assert "D:\\VAJRA_DATA" not in text, relative
        assert "D:/VAJRA_DATA" not in text, relative


def test_only_the_publisher_writes_to_the_published_root() -> None:
    """`paths.DATA_ROOT` may be referenced by the publisher, the quality checker and the
    documentation generator, and by nothing else."""
    # The publisher writes it; the verification modules read it back on purpose, which is the
    # whole point of checking the published files rather than the build pipeline's claims.
    allowed = {"publish.py", "publish_docs.py", "publish_runner.py", "quality.py",
               "quality_runner.py", "external_check.py", "paths.py", "config.py"}
    offenders = []
    for path in (REPO_ROOT / "src" / "vajra_regime").rglob("*.py"):
        if path.name in allowed:
            continue
        text = path.read_text(encoding="utf-8")
        if "DATA_ROOT" in text and "paths.DATA_ROOT" in text:
            offenders.append(path.name)
    assert offenders == [], offenders


def test_no_strategy_or_scanner_code_survives() -> None:
    """The engine does one job. These are the modules the reset removed; if one comes back,
    the folder has stopped being an OHLCV engine."""
    src = REPO_ROOT / "src" / "vajra_regime"
    for gone in (
        "core_strategy.py",
        "core_strategy_52w.py",
        "backtest_pipeline.py",
        "regime.py",
        "scanner_parity.py",
        "google_dashboard_sync.py",
        "telegram_notifications.py",
        "feature_store.py",
        "forecast_model.py",
        "friend_export.py",
    ):
        assert not (src / gone).exists(), gone
    for gone_package in ("ai_technical_analyst", "ai_technical_analyst_v1_1"):
        assert not (src / gone_package).exists(), gone_package
