"""Tests for sealed_products table (Connection layer — JSON column parsing)."""


def test_contents_parsed_as_object(sample_db):
    rows = sample_db.execute(
        "SELECT contents FROM sealed_products WHERE uuid = $1",
        ["sealed-uuid-001"],
    )
    assert len(rows) == 1
    assert isinstance(rows[0]["contents"], dict)
    assert "pack" in rows[0]["contents"]


def test_identifiers_parsed_as_object(sample_db):
    rows = sample_db.execute(
        "SELECT identifiers FROM sealed_products WHERE uuid = $1",
        ["sealed-uuid-001"],
    )
    assert len(rows) == 1
    assert isinstance(rows[0]["identifiers"], dict)
    assert rows[0]["identifiers"]["tcgplayerProductId"] == "162583"


def test_purchase_urls_parsed_as_object(sample_db):
    rows = sample_db.execute(
        "SELECT purchaseUrls FROM sealed_products WHERE uuid = $1",
        ["sealed-uuid-001"],
    )
    assert len(rows) == 1
    assert isinstance(rows[0]["purchaseUrls"], dict)
    assert "tcgplayer" in rows[0]["purchaseUrls"]


def test_filter_by_set_code(sample_db):
    rows = sample_db.execute(
        "SELECT * FROM sealed_products WHERE setCode = $1",
        ["A25"],
    )
    assert len(rows) == 2
    assert all(r["setCode"] == "A25" for r in rows)


def test_filter_by_category(sample_db):
    rows = sample_db.execute(
        "SELECT * FROM sealed_products WHERE category = $1",
        ["booster_box"],
    )
    assert len(rows) == 2
