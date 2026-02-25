"""Tests for the price query module."""

import pytest

from mtgjson_sdk.queries.prices import PriceQuery

# === PriceQuery integration tests ===

SAMPLE_PRICE_DATA = [
    {
        "uuid": "card-uuid-001",
        "source": "paper",
        "provider": "tcgplayer",
        "currency": "USD",
        "price_type": "retail",
        "finish": "normal",
        "date": "2024-01-01",
        "price": 1.50,
    },
    {
        "uuid": "card-uuid-001",
        "source": "paper",
        "provider": "tcgplayer",
        "currency": "USD",
        "price_type": "retail",
        "finish": "normal",
        "date": "2024-01-02",
        "price": 1.75,
    },
    {
        "uuid": "card-uuid-001",
        "source": "paper",
        "provider": "tcgplayer",
        "currency": "USD",
        "price_type": "retail",
        "finish": "normal",
        "date": "2024-01-03",
        "price": 2.00,
    },
    {
        "uuid": "card-uuid-001",
        "source": "paper",
        "provider": "tcgplayer",
        "currency": "USD",
        "price_type": "retail",
        "finish": "foil",
        "date": "2024-01-01",
        "price": 3.50,
    },
    {
        "uuid": "card-uuid-001",
        "source": "paper",
        "provider": "tcgplayer",
        "currency": "USD",
        "price_type": "retail",
        "finish": "foil",
        "date": "2024-01-03",
        "price": 4.00,
    },
    {
        "uuid": "card-uuid-001",
        "source": "paper",
        "provider": "tcgplayer",
        "currency": "USD",
        "price_type": "buylist",
        "finish": "normal",
        "date": "2024-01-03",
        "price": 0.80,
    },
    {
        "uuid": "card-uuid-002",
        "source": "paper",
        "provider": "tcgplayer",
        "currency": "USD",
        "price_type": "retail",
        "finish": "normal",
        "date": "2024-01-03",
        "price": 5.00,
    },
]


@pytest.fixture
def price_query(sample_db):
    """PriceQuery with sample price data loaded."""
    sample_db.register_table_from_data("all_prices_today", SAMPLE_PRICE_DATA)
    sample_db.register_table_from_data("all_prices", SAMPLE_PRICE_DATA)
    pq = PriceQuery.__new__(PriceQuery)
    pq._conn = sample_db
    return pq


def test_today_returns_latest_date(price_query):
    """today() should only return prices from the most recent date."""
    rows = price_query.today("card-uuid-001")
    dates = {r["date"] for r in rows}
    assert dates == {"2024-01-03"}


def test_today_with_provider_filter(price_query):
    rows = price_query.today("card-uuid-001", provider="tcgplayer")
    assert all(r["provider"] == "tcgplayer" for r in rows)
    assert all(r["date"] == "2024-01-03" for r in rows)


def test_today_with_finish_filter(price_query):
    rows = price_query.today("card-uuid-001", finish="foil")
    assert all(r["finish"] == "foil" for r in rows)
    assert len(rows) == 1
    assert rows[0]["price"] == 4.00


def test_today_with_price_type_filter(price_query):
    rows = price_query.today("card-uuid-001", price_type="buylist")
    assert all(r["price_type"] == "buylist" for r in rows)
    assert len(rows) == 1


def test_history_all_dates(price_query):
    """history() should return all dates in chronological order."""
    rows = price_query.history("card-uuid-001", finish="normal", price_type="retail")
    assert len(rows) == 3
    dates = [r["date"] for r in rows]
    assert dates == ["2024-01-01", "2024-01-02", "2024-01-03"]


def test_history_date_range(price_query):
    rows = price_query.history(
        "card-uuid-001",
        finish="normal",
        price_type="retail",
        date_from="2024-01-02",
        date_to="2024-01-03",
    )
    assert len(rows) == 2
    dates = [r["date"] for r in rows]
    assert dates == ["2024-01-02", "2024-01-03"]


def test_history_date_from_only(price_query):
    rows = price_query.history(
        "card-uuid-001",
        finish="normal",
        price_type="retail",
        date_from="2024-01-03",
    )
    assert len(rows) == 1
    assert rows[0]["price"] == 2.00


def test_price_trend(price_query):
    trend = price_query.price_trend(
        "card-uuid-001", provider="tcgplayer", finish="normal"
    )
    assert trend is not None
    assert trend["min_price"] == 1.50
    assert trend["max_price"] == 2.00
    assert trend["first_date"] == "2024-01-01"
    assert trend["last_date"] == "2024-01-03"
    assert trend["data_points"] == 3


def test_price_trend_no_data(price_query):
    trend = price_query.price_trend("nonexistent-uuid")
    assert trend is None


# === cheapest_printings / most_expensive_printings (arg_min/arg_max) ===


def test_cheapest_printings(price_query):
    """cheapest_printings returns one row per card name with arg_min data."""
    rows = price_query.cheapest_printings()
    assert len(rows) >= 1
    # Each row has the expected columns
    for r in rows:
        assert "name" in r
        assert "cheapest_set" in r
        assert "cheapest_uuid" in r
        assert "min_price" in r
    # card-uuid-001 ($2.00) should be cheaper than card-uuid-002 ($5.00)
    names = {r["name"]: r for r in rows}
    assert names["Lightning Bolt"]["min_price"] < names["Counterspell"]["min_price"]


def test_most_expensive_printings(price_query):
    """most_expensive_printings returns one row per card name with arg_max data."""
    rows = price_query.most_expensive_printings()
    assert len(rows) >= 1
    for r in rows:
        assert "name" in r
        assert "priciest_set" in r
        assert "max_price" in r
    # Counterspell ($5.00) > Lightning Bolt ($2.00)
    assert rows[0]["max_price"] >= rows[-1]["max_price"]  # ordered DESC


def test_cheapest_printings_no_prices(sample_db):
    """Returns empty list when no price data exists."""
    pq = PriceQuery.__new__(PriceQuery)
    pq._conn = sample_db
    assert pq.cheapest_printings() == []
