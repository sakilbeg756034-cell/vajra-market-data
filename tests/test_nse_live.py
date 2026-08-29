from __future__ import annotations

import io
import zipfile
from datetime import date
from pathlib import Path

import duckdb

from vajra_regime.nse_live import (
    RAW_TABLE,
    append_raw_day_to_master,
    normalize_udiff_bhavcopy,
    official_bhavcopy_url,
)


def _write_sample_zip(path: Path) -> None:
    csv_text = """TradDt,TckrSymb,ISIN,SctySrs,OpnPric,HghPric,LwPric,ClsPric,TtlTradgVol,TtlTrfVal
2026-01-02,AAA,INE000A01001,EQ,100,110,95,105,1000,105000
2026-01-02,AAAOLD,INE000A01001,EQ,100,110,95,105,500,52500
2026-01-02,BBB,INE000B01001,BE,50,55,49,54,2000,108000
2026-01-02,BAD,INE000C01001,EQ,10,9,8,9,100,900
"""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("BhavCopy_NSE_CM_0_0_0_20260102_F_0000.csv", csv_text)
    path.write_bytes(buffer.getvalue())


def test_official_bhavcopy_url() -> None:
    assert official_bhavcopy_url(date(2026, 1, 2)).endswith(
        "BhavCopy_NSE_CM_0_0_0_20260102_F_0000.csv.zip"
    )


def test_normalize_udiff_filters_and_deduplicates(tmp_path: Path) -> None:
    zip_path = tmp_path / "sample.zip"
    _write_sample_zip(zip_path)

    frame, report = normalize_udiff_bhavcopy(
        zip_path,
        date(2026, 1, 2),
        minimum_kept_rows=1,
    )

    assert report["raw_rows"] == 4
    assert report["kept_rows"] == 1
    assert frame["ISIN"].tolist() == ["INE000A01001"]
    assert frame["Symbol"].tolist() == ["AAA"]
    assert frame["Volume"].tolist() == [1000]
    assert not frame.duplicated(["Date", "ISIN"]).any()


def test_raw_master_append_is_idempotent(tmp_path: Path) -> None:
    database = tmp_path / "master.duckdb"
    with duckdb.connect(str(database)):
        pass

    zip_path = tmp_path / "sample.zip"
    _write_sample_zip(zip_path)
    frame, report = normalize_udiff_bhavcopy(
        zip_path,
        date(2026, 1, 2),
        minimum_kept_rows=1,
    )
    manifest = {
        "Date": "2026-01-02",
        "Status": "VALIDATED",
        "SourceURL": official_bhavcopy_url(date(2026, 1, 2)),
        "ZipPath": str(zip_path),
        "ParquetPath": str(tmp_path / "day.parquet"),
        "SourceSha256": frame["SourceSha256"].iloc[0],
        "RawRows": report["raw_rows"],
        "KeptRows": report["kept_rows"],
        "Message": "test",
        "RecordedAtUTC": "2026-01-03T00:00:00+00:00",
    }

    assert append_raw_day_to_master(database, frame, manifest) == 1
    assert append_raw_day_to_master(database, frame, manifest) == 0

    with duckdb.connect(str(database), read_only=True) as connection:
        row_count = connection.execute(
            f"SELECT COUNT(*) FROM {RAW_TABLE}"
        ).fetchone()[0]
        date_count = connection.execute(
            f"SELECT COUNT(*) FROM {RAW_TABLE} WHERE Date = '2026-01-02'"
        ).fetchone()[0]
    assert row_count == 1
    assert date_count == 1
