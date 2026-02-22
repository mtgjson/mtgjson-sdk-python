"""Tests for the legality query module."""


def test_formats_for_card(sdk_offline):
    formats = sdk_offline.legalities.formats_for_card("card-uuid-001")
    assert "modern" in formats
    assert formats["modern"] == "Legal"
    assert formats["vintage"] == "Restricted"


def test_legal_in(sdk_offline):
    cards = sdk_offline.legalities.legal_in("modern")
    assert len(cards) == 2
    names = {c.name for c in cards}
    assert "Lightning Bolt" in names
    assert "Counterspell" in names


def test_is_legal(sdk_offline):
    assert sdk_offline.legalities.is_legal("card-uuid-001", "modern") is True
    assert sdk_offline.legalities.is_legal("card-uuid-001", "standard") is False


def test_banned_in(sdk_offline):
    # No banned cards in our sample data
    cards = sdk_offline.legalities.banned_in("modern")
    assert cards == []


def test_restricted_in(sdk_offline):
    cards = sdk_offline.legalities.restricted_in("vintage")
    assert len(cards) == 1
    assert cards[0]["name"] == "Lightning Bolt"


def test_suspended_in(sdk_offline):
    cards = sdk_offline.legalities.suspended_in("historic")
    assert len(cards) == 1
    assert cards[0]["name"] == "Counterspell"


def test_not_legal_in(sdk_offline):
    cards = sdk_offline.legalities.not_legal_in("standard")
    assert len(cards) == 2
