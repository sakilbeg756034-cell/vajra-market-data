from __future__ import annotations

import csv
import hashlib
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen
from uuid import uuid4

from vajra_regime.checkpoint import atomic_json, canonical_hash, sha256_file
from vajra_regime.nifty500_migration.constants import CHECKPOINT_ROOT, DATA_ROOT, FOUNDATION_VERSION


USER_AGENT = "Mozilla/5.0 (compatible; VAJRA-Nifty500-PIT-Audit/1.0)"
PRESS_RELEASE_PAGE = "https://www.niftyindices.com/press-release"
CURRENT_CONSTITUENTS = "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"
SUPPORTING_SOURCES = {
    "press_release_index": PRESS_RELEASE_PAGE,
    "current_constituents": CURRENT_CONSTITUENTS,
    "methodology": "https://www.niftyindices.com/Methodology/Method_NIFTY_Equity_Indices.pdf",
    "rebalance_schedule": "https://www.niftyindices.com/resources/index-rebalancing-schedule",
    "index_page": "https://www.niftyindices.com/indices/equity/broad-based-indices/nifty-500",
    "index_data_subscription": "https://www.niftyindices.com/offerings/data-subscription",
    "nse_reports": "https://www.nseindia.com/all-reports",
    "nse_paid_historical": "https://www.nseindia.com/static/market-data/eod-historical-data-subscription",
}
MAX_ATTEMPTS = 4


class _PressReleaseParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[dict[str, str]] = []
        self._href: str | None = None
        self._text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() == "a":
            self._href = dict(attrs).get("href")
            self._text = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.casefold() == "a" and self._href is not None:
            if "/press_release/" in self._href.casefold() and ".pdf" in self._href.casefold():
                self.links.append(
                    {
                        "title": " ".join("".join(self._text).split()),
                        "url": urljoin(PRESS_RELEASE_PAGE, self._href),
                    }
                )
            self._href = None
            self._text = []


def _download(url: str, path: Path, *, expected_type: str | None = None) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 0:
        return {
            "status": "REUSED_HASH_VALID_LOCAL",
            "path": str(path),
            "url": url,
            "size_bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
    last_error = ""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        temporary = path.with_name(f".{path.name}.{uuid4().hex}.partial")
        try:
            request = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
            with urlopen(request, timeout=90) as response:  # noqa: S310 - fixed audited HTTPS sources.
                payload = response.read()
                content_type = str(response.headers.get("Content-Type", ""))
            if not payload:
                raise ValueError("empty response")
            if expected_type and expected_type not in content_type.casefold():
                raise ValueError(f"unexpected content type {content_type!r}")
            temporary.write_bytes(payload)
            os.replace(temporary, path)
            return {
                "status": "DOWNLOADED",
                "path": str(path),
                "url": url,
                "size_bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
                "content_type": content_type,
                "attempts": attempt,
            }
        except (HTTPError, URLError, TimeoutError, OSError, ValueError) as exc:
            temporary.unlink(missing_ok=True)
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < MAX_ATTEMPTS:
                time.sleep(min(2 ** (attempt - 1), 8))
    return {"status": "FAILED", "path": str(path), "url": url, "error": last_error, "attempts": MAX_ATTEMPTS}


def _unique_press_links(index_path: Path) -> list[dict[str, str]]:
    parser = _PressReleaseParser()
    parser.feed(index_path.read_text(encoding="utf-8", errors="replace"))
    unique: dict[str, dict[str, str]] = {}
    for item in parser.links:
        canonical = item["url"].split("?", maxsplit=1)[0]
        unique.setdefault(canonical.casefold(), {**item, "url": canonical})
    return sorted(unique.values(), key=lambda item: item["url"].casefold())


def _archive_name(url: str) -> str:
    name = Path(urlparse(url).path).name
    if not name:
        name = hashlib.sha256(url.encode()).hexdigest()[:16] + ".html"
    return name


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    keys = sorted({key for row in rows for key in row})
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def archive_official_sources() -> dict[str, Any]:
    raw = DATA_ROOT / "01 Raw Source Archives"
    press_dir = raw / "NSE Indices Press Releases"
    current_dir = raw / "Official Current Constituents"
    support_dir = raw / "Official Methodology and Policies"
    provenance = DATA_ROOT / "10 Provenance"
    logs = DATA_ROOT / "11 Logs"
    for directory in (press_dir, current_dir, support_dir, provenance, logs, CHECKPOINT_ROOT):
        directory.mkdir(parents=True, exist_ok=True)

    index_path = press_dir / "press-release-index.html"
    index_record = _download(PRESS_RELEASE_PAGE, index_path, expected_type="text/html")
    if index_record["status"] == "FAILED":
        raise RuntimeError(f"Official press release index unavailable: {index_record['error']}")
    press_links = _unique_press_links(index_path)
    if len(press_links) < 1_000:
        raise RuntimeError(f"Official press release archive unexpectedly small: {len(press_links)}")

    records: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {
            pool.submit(_download, item["url"], press_dir / _archive_name(item["url"]), expected_type="pdf"): item
            for item in press_links
        }
        for completed, future in enumerate(as_completed(futures), start=1):
            item = futures[future]
            record = {**item, **future.result(), "source_tier": "A_AUTHORITATIVE_NSE_INDICES"}
            records.append(record)
            if completed % 100 == 0:
                print(f"Official press releases archived: {completed}/{len(futures)}", flush=True)

    current_record = _download(CURRENT_CONSTITUENTS, current_dir / "ind_nifty500list.csv")
    current_record.update({"title": "Official current Nifty 500 constituent file", "source_tier": "A_AUTHORITATIVE_NSE_INDICES"})
    records.append(current_record)
    support_records: list[dict[str, Any]] = []
    for label, url in SUPPORTING_SOURCES.items():
        if label in {"press_release_index", "current_constituents"}:
            continue
        suffix = Path(urlparse(url).path).suffix or ".html"
        path = support_dir / f"{label}{suffix}"
        record = _download(url, path)
        record.update({"title": label, "source_tier": "A_AUTHORITATIVE_NSE_OR_NSE_INDICES"})
        support_records.append(record)
        records.append(record)

    records.sort(key=lambda item: (item.get("source_tier", ""), item.get("url", "")))
    manifest_path = provenance / "official_source_download_manifest.csv"
    _write_csv(manifest_path, records)
    failures = [record for record in records if record["status"] == "FAILED"]
    generated = datetime.now(UTC).isoformat()
    status = {
        "status": "COMPLETE" if not failures else "INCOMPLETE_SOURCE_DOWNLOADS",
        "generated_at_utc": generated,
        "foundation_version": FOUNDATION_VERSION,
        "official_press_release_links": len(press_links),
        "successful_or_cached_press_releases": sum(record["status"] != "FAILED" for record in records[: len(press_links)]),
        "failed_download_count": len(failures),
        "failed_downloads": failures,
        "current_constituent_status": current_record["status"],
        "manifest_path": str(manifest_path),
        "manifest_sha256": sha256_file(manifest_path),
        "paid_source_policy": {
            "nse_indices_historical_constituent_subscription_exists": True,
            "nse_historical_eod_subscription_exists": True,
            "purchase_or_subscription_attempted": False,
            "payment_details_used": False,
        },
    }
    status["status_payload_sha256"] = canonical_hash(status)
    status_path = logs / "official_source_archive_status.json"
    atomic_json(status_path, status)
    checkpoint = {
        "phase": 1,
        "name": "OFFICIAL_SOURCE_DISCOVERY_AND_ARCHIVE",
        "status": status["status"],
        "recorded_at_utc": generated,
        "manifest_path": str(manifest_path),
        "manifest_sha256": status["manifest_sha256"],
        "press_releases": len(press_links),
        "failures": len(failures),
        "payment_made": False,
    }
    checkpoint["checkpoint_fingerprint_sha256"] = canonical_hash(checkpoint)
    atomic_json(CHECKPOINT_ROOT / "phase_01_official_source_archive.json", checkpoint)
    return status
