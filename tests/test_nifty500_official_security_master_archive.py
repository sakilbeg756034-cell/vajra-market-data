from __future__ import annotations

from vajra_regime.nifty500_migration.official_security_master_archive import parse_nse_security_master_html


def test_parse_nse_security_master_html_extracts_official_rows() -> None:
    payload = """
    <table>
      <tr><th>Sr</th><th>Symbol</th><th>Company Name</th></tr>
      <tr><td>76</td><td>BONGAIREFN</td><td>Bongaigaon Refinery &amp; Petrochemicals Ltd</td></tr>
      <tr><td>393</td><td>SATYAM</td><td>Satyam Computer Services Ltd</td></tr>
    </table>
    """
    rows = parse_nse_security_master_html(payload)
    assert [row["symbol"] for row in rows] == ["BONGAIREFN", "SATYAM"]
    assert rows[0]["company_name"] == "Bongaigaon Refinery & Petrochemicals Ltd"
    assert all(row["confidence"] == "VERIFIED_OFFICIAL" for row in rows)
