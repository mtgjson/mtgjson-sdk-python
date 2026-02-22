"""Tests for set queries."""

import pytest

from mtgjson_sdk.models.sets import SetList

# Price data matching SAMPLE_CARDS (card-uuid-001 in A25, card-uuid-002 in MH2)
_SET_PRICE_DATA = [
    {
        "uuid": "card-uuid-001",
        "source": "paper",
        "provider": "tcgplayer",
        "currency": "USD",
        "category": "retail",
        "finish": "normal",
        "date": "2024-01-03",
        "price": 2.00,
    },
    {
        "uuid": "card-uuid-002",
        "source": "paper",
        "provider": "tcgplayer",
        "currency": "USD",
        "category": "retail",
        "finish": "normal",
        "date": "2024-01-03",
        "price": 5.00,
    },
    {
        "uuid": "card-uuid-003",
        "source": "paper",
        "provider": "tcgplayer",
        "currency": "USD",
        "category": "retail",
        "finish": "normal",
        "date": "2024-01-03",
        "price": 3.00,
    },
]


@pytest.fixture
def sdk_with_prices(sdk_offline):
    """SDK fixture with price data loaded alongside cards/sets."""
    sdk_offline._conn.register_table_from_data("prices_today", _SET_PRICE_DATA)
    return sdk_offline


def test_get_set(sdk_offline):
    s = sdk_offline.sets.get("A25")
    assert isinstance(s, SetList)
    assert s.name == "Masters 25"
    assert s.code == "A25"


def test_get_set_case_insensitive(sdk_offline):
    s = sdk_offline.sets.get("a25")
    assert s is not None
    assert s.code == "A25"


def test_get_set_not_found(sdk_offline):
    assert sdk_offline.sets.get("XXXXX") is None


def test_list_sets(sdk_offline):
    sets = sdk_offline.sets.list()
    assert len(sets) == 2


def test_list_sets_by_type(sdk_offline):
    sets = sdk_offline.sets.list(set_type="masters")
    assert len(sets) == 1
    assert sets[0].code == "A25"


def test_search_sets(sdk_offline):
    sets = sdk_offline.sets.search(name="Horizons")
    assert len(sets) >= 1
    assert any("Horizons" in s.name for s in sets)


def test_set_count(sdk_offline):
    assert sdk_offline.sets.count() == 2


# === Financial summary tests ===


def test_financial_summary(sdk_with_prices):
    """get_financial_summary should return aggregate price stats."""
    summary = sdk_with_prices.sets.get_financial_summary("A25")
    assert summary is not None
    # A25 has card-uuid-001 ($2.00) and card-uuid-003 ($3.00)
    assert summary["card_count"] == 2
    assert summary["total_value"] == 5.00
    assert summary["min_value"] == 2.00
    assert summary["max_value"] == 3.00


def test_financial_summary_single_card(sdk_with_prices):
    summary = sdk_with_prices.sets.get_financial_summary("MH2")
    assert summary is not None
    assert summary["card_count"] == 1
    assert summary["total_value"] == 5.00


def test_financial_summary_no_prices(sdk_offline):
    """Returns None when price data isn't loaded."""
    summary = sdk_offline.sets.get_financial_summary("A25")
    assert summary is None


def test_financial_summary_no_data_for_set(sdk_with_prices):
    """Returns None for a set with no matching price data."""
    summary = sdk_with_prices.sets.get_financial_summary("XXXXX")
    assert summary is None
