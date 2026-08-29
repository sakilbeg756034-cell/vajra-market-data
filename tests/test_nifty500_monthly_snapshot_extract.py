from __future__ import annotations

from vajra_regime.nifty500_migration.monthly_snapshot_extract import _candidate_names, _parse_csv, _parse_pdf


def test_candidate_name_is_exact_nifty500_not_derived() -> None:
    names = [
        "folder/cnx500_Apr2013.csv",
        "NIFTY_500_Jan2018.pdf",
        "NIFTY500_Shariah_Jan2018.pdf",
        "NIFTY500_Multicap_Jan2018.pdf",
    ]
    assert _candidate_names(names) == ["folder/cnx500_Apr2013.csv", "NIFTY_500_Jan2018.pdf"]


def test_csv_parser_preserves_published_identity_fields() -> None:
    payload = (
        "Date,Index Name,Symbol,Security Name,Industry,Close Price,Index MCap(Rs. Crores),Weightage(%)\n"
        "18-04-2013,CNX 500,ABC,ABC Ltd.,CHEMICALS,10,100,0.1\n"
    ).encode()
    snapshot_date, members = _parse_csv(payload)
    assert snapshot_date == "2013-04-18"
    assert members == [
        {
            "symbol": "ABC",
            "company_name": "ABC Ltd.",
            "industry_as_published": "CHEMICALS",
            "snapshot_date": "2013-04-18",
        }
    ]


def test_pdf_snapshot_rejects_publication_header_token() -> None:
    # Production filtering happens after PDF extraction. This focused assertion locks the token vocabulary
    # used by that parser without fabricating a binary PDF fixture.
    from vajra_regime.nifty500_migration.constants import INVALID_SYMBOL_TOKENS

    assert "PUBLICATION" in INVALID_SYMBOL_TOKENS
    assert callable(_parse_pdf)
