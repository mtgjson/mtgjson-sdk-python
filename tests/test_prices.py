"""Tests for the price query module."""

import io
import json

import pytest

from mtgjson_sdk.queries.prices import PriceQuery, _stream_flatten_prices

# === _stream_flatten_prices unit tests ===


def _flatten_to_list(data: dict) -> list[dict]:
    """Helper: stream-flatten to an in-memory buffer, parse back to list."""
    buf = io.StringIO()
    count = _stream_flatten_prices(data, buf)
    buf.seek(0)
    rows = [json.loads(line) for line in buf if line.strip()]
    assert len(rows) == count
    return rows


def test_flatten_prices():
    data = {
        "card-uuid-001": {
            "paper": {
                "tcgplayer": {
                    "currency": "USD",
                    "retail": {
                        "normal": {"2024-01-01": 1.50, "2024-01-02": 1.75},
                        "foil": {"2024-01-01": 3.50},
                    },
                }
            }
        }
    }
    rows = _flatten_to_list(data)
    assert len(rows) == 3
    assert all(r["uuid"] == "card-uuid-001" for r in rows)
    assert all(r["provider"] == "tcgplayer" for r in rows)
    assert all(r["currency"] == "USD" for r in rows)

    normal_rows = [r for r in rows if r["finish"] == "normal"]
    assert len(normal_rows) == 2
    foil_rows = [r for r in rows if r["finish"] == "foil"]
    assert len(foil_rows) == 1


def test_flatten_prices_empty():
    assert _flatten_to_list({}) == []


def test_flatten_prices_mixed_sources():
    data = {
        "uuid-1": {
            "paper": {
                "tcgplayer": {
                    "currency": "USD",
                    "retail": {"normal": {"2024-01-01": 1.0}},
                }
            },
            "mtgo": {
                "cardhoarder": {
                    "currency": "USD",
                    "retail": {"normal": {"2024-01-01": 0.05}},
                }
            },
        }
    }
    rows = _flatten_to_list(data)
    assert len(rows) == 2
    sources = {r["source"] for r in rows}
    assert sources == {"paper", "mtgo"}


# === PriceQuery integration tests ===

SAMPLE_PRICE_DATA = [
    {
        "uuid": "card-uuid-001",
        "source": "paper",
        "provider": "tcgplayer",
        "currency": "USD",
        "category": "retail",
        "finish": "normal",
        "date": "2024-01-01",
        "price": 1.50,
    },
    {
        "uuid": "card-uuid-001",
        "source": "paper",
        "provider": "tcgplayer",
        "currency": "USD",
        "category": "retail",
        "finish": "normal",
        "date": "2024-01-02",
        "price": 1.75,
    },
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
        "uuid": "card-uuid-001",
        "source": "paper",
        "provider": "tcgplayer",
        "currency": "USD",
        "category": "retail",
        "finish": "foil",
        "date": "2024-01-01",
        "price": 3.50,
    },
    {
        "uuid": "card-uuid-001",
        "source": "paper",
        "provider": "tcgplayer",
        "currency": "USD",
        "category": "retail",
        "finish": "foil",
        "date": "2024-01-03",
        "price": 4.00,
    },
    {
        "uuid": "card-uuid-001",
        "source": "paper",
        "provider": "tcgplayer",
        "currency": "USD",
        "category": "buylist",
        "finish": "normal",
        "date": "2024-01-03",
        "price": 0.80,
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
]


@pytest.fixture
def price_query(sample_db):
    """PriceQuery with sample price data loaded."""
    sample_db.register_table_from_data("prices_today", SAMPLE_PRICE_DATA)
    pq = PriceQuery.__new__(PriceQuery)
    pq._conn = sample_db
    pq._cache = None
    pq._loaded = True
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


def test_today_with_category_filter(price_query):
    rows = price_query.today("card-uuid-001", category="buylist")
    assert all(r["category"] == "buylist" for r in rows)
    assert len(rows) == 1


def test_history_all_dates(price_query):
    """history() should return all dates in chronological order."""
    rows = price_query.history("card-uuid-001", finish="normal", category="retail")
    assert len(rows) == 3
    dates = [r["date"] for r in rows]
    assert dates == ["2024-01-01", "2024-01-02", "2024-01-03"]


def test_history_date_range(price_query):
    rows = price_query.history(
        "card-uuid-001",
        finish="normal",
        category="retail",
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
        category="retail",
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
    pq._cache = None
    pq._loaded = True
    assert pq.cheapest_printings() == []


# === Streaming correctness tests ===


def test_stream_flatten_writes_valid_ndjson_to_file(tmp_path):
    """_stream_flatten_prices writes valid NDJSON to a real file."""
    data = {
        "uuid-1": {
            "paper": {
                "tcgplayer": {
                    "currency": "USD",
                    "retail": {"normal": {"2024-01-01": 1.0, "2024-01-02": 1.5}},
                    "buylist": {"normal": {"2024-01-01": 0.5}},
                }
            }
        },
        "uuid-2": {
            "mtgo": {
                "cardhoarder": {
                    "currency": "TIX",
                    "retail": {"normal": {"2024-01-01": 0.02}},
                }
            }
        },
    }
    out_path = tmp_path / "prices.ndjson"
    with open(out_path, "w", encoding="utf-8") as f:
        count = _stream_flatten_prices(data, f)

    # Re-read and parse each line independently
    lines = out_path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == count
    for line in lines:
        row = json.loads(line)
        assert "uuid" in row
        assert "price" in row
        assert isinstance(row["price"], float)


def test_stream_flatten_count_matches_output():
    """Returned count matches actual number of lines written."""
    data = {
        "u1": {
            "paper": {
                "tcgplayer": {
                    "currency": "USD",
                    "retail": {
                        "normal": {"2024-01-01": 1.0},
                        "foil": {"2024-01-01": 2.0, "2024-01-02": 2.5},
                    },
                }
            }
        }
    }
    buf = io.StringIO()
    count = _stream_flatten_prices(data, buf)
    buf.seek(0)
    actual_lines = [line for line in buf if line.strip()]
    assert count == 3
    assert len(actual_lines) == count


# === No-data state tests ===


def test_today_no_price_table(sample_db):
    """today() returns [] when prices_today table doesn't exist."""
    pq = PriceQuery.__new__(PriceQuery)
    pq._conn = sample_db
    pq._cache = None
    pq._loaded = True
    # prices_today is NOT registered — should return []
    result = pq.today("nonexistent-uuid")
    assert result == []


def test_get_no_price_table(sample_db):
    """get() returns None when prices_today table doesn't exist."""
    pq = PriceQuery.__new__(PriceQuery)
    pq._conn = sample_db
    pq._cache = None
    pq._loaded = True
    result = pq.get("nonexistent-uuid")
    assert result is None
