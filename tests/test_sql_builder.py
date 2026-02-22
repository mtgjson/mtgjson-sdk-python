"""Tests for the SQL builder."""

import duckdb
import pytest

from mtgjson_sdk._sql import SQLBuilder


def test_basic_select():
    q = SQLBuilder("cards")
    sql, params = q.build()
    assert sql == "SELECT *\nFROM cards"
    assert params == []


def test_where_eq():
    q = SQLBuilder("cards").where_eq("name", "Bolt")
    sql, params = q.build()
    assert "WHERE name = $1" in sql
    assert params == ["Bolt"]


def test_where_gte_lte():
    q = SQLBuilder("cards").where_gte("manaValue", 2.0).where_lte("manaValue", 5.0)
    sql, params = q.build()
    assert "manaValue >= $1" in sql
    assert "manaValue <= $2" in sql
    assert params == [2.0, 5.0]


def test_where_or():
    q = SQLBuilder("cards").where_or(
        ("name = $1", "Lightning Bolt"),
        ("name = $1", "Counterspell"),
    )
    sql, params = q.build()
    assert "(name = $1 OR name = $2)" in sql
    assert params == ["Lightning Bolt", "Counterspell"]


def test_where_or_combined_with_and():
    q = (
        SQLBuilder("cards")
        .where_eq("setCode", "A25")
        .where_or(("rarity = $1", "rare"), ("rarity = $1", "mythic"))
    )
    sql, params = q.build()
    assert "setCode = $1" in sql
    assert "(rarity = $2 OR rarity = $3)" in sql
    assert params == ["A25", "rare", "mythic"]


def test_group_by():
    q = SQLBuilder("cards").select("setCode", "COUNT(*)").group_by("setCode")
    sql, params = q.build()
    assert "GROUP BY setCode" in sql


def test_having():
    q = (
        SQLBuilder("cards")
        .select("setCode", "COUNT(*) AS cnt")
        .group_by("setCode")
        .having("COUNT(*) > $1", 10)
    )
    sql, params = q.build()
    assert "HAVING COUNT(*) > $1" in sql
    assert params == [10]


def test_distinct():
    q = SQLBuilder("cards").select("name").distinct()
    sql, params = q.build()
    assert sql.startswith("SELECT DISTINCT name")


def test_where_regex():
    q = SQLBuilder("cards").where_regex("text", "deals \\d+ damage")
    sql, params = q.build()
    assert "regexp_matches(text, $1)" in sql
    assert params == ["deals \\d+ damage"]


def test_where_regex_with_other_conditions():
    q = SQLBuilder("cards").where_eq("setCode", "A25").where_regex("text", "^Draw")
    sql, params = q.build()
    assert "setCode = $1" in sql
    assert "regexp_matches(text, $2)" in sql
    assert params == ["A25", "^Draw"]


def test_where_fuzzy():
    q = SQLBuilder("cards").where_fuzzy("name", "Ligtning Bolt")
    sql, params = q.build()
    assert "jaro_winkler_similarity(name, $1) > 0.8" in sql
    assert params == ["Ligtning Bolt"]


def test_where_fuzzy_custom_threshold():
    q = SQLBuilder("cards").where_fuzzy("name", "Bolt", threshold=0.9)
    sql, params = q.build()
    assert "jaro_winkler_similarity(name, $1) > 0.9" in sql
    assert params == ["Bolt"]


def test_where_fuzzy_with_other_conditions():
    q = (
        SQLBuilder("cards")
        .where_eq("setCode", "A25")
        .where_fuzzy("name", "Ligtning Bolt")
    )
    sql, params = q.build()
    assert "setCode = $1" in sql
    assert "jaro_winkler_similarity(name, $2) > 0.8" in sql
    assert params == ["A25", "Ligtning Bolt"]


def test_full_query():
    q = (
        SQLBuilder("prices_today")
        .select("provider", "AVG(price) AS avg_price")
        .where_eq("uuid", "abc-123")
        .where_gte("date", "2024-01-01")
        .group_by("provider")
        .having("AVG(price) > $1", 1.0)
        .order_by("avg_price DESC")
        .limit(10)
    )
    sql, params = q.build()
    assert "SELECT provider, AVG(price) AS avg_price" in sql
    assert "WHERE uuid = $1 AND date >= $2" in sql
    assert "GROUP BY provider" in sql
    assert "HAVING AVG(price) > $3" in sql
    assert "ORDER BY avg_price DESC" in sql
    assert "LIMIT 10" in sql
    assert params == ["abc-123", "2024-01-01", 1.0]


# === Input validation tests ===


def test_limit_rejects_string():
    with pytest.raises(TypeError, match="non-negative integer"):
        SQLBuilder("t").limit("1; DROP TABLE t")


def test_limit_rejects_negative():
    with pytest.raises(TypeError, match="non-negative integer"):
        SQLBuilder("t").limit(-1)


def test_limit_accepts_zero():
    q = SQLBuilder("t").limit(0)
    sql, _ = q.build()
    assert "LIMIT 0" in sql


def test_offset_rejects_string():
    with pytest.raises(TypeError, match="non-negative integer"):
        SQLBuilder("t").offset("0")


def test_offset_rejects_negative():
    with pytest.raises(TypeError, match="non-negative integer"):
        SQLBuilder("t").offset(-5)


def test_fuzzy_threshold_rejects_out_of_range():
    with pytest.raises(ValueError, match="between 0 and 1"):
        SQLBuilder("t").where_fuzzy("name", "Bolt", threshold=2.0)


# === Execution-validated tests (run SQL against real DuckDB) ===


@pytest.fixture
def duckdb_conn():
    """In-memory DuckDB with a small test table."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE items (name VARCHAR, category VARCHAR, price DOUBLE)")
    conn.execute("INSERT INTO items VALUES ('Alpha', 'A', 1.0)")
    conn.execute("INSERT INTO items VALUES ('Beta', 'A', 2.0)")
    conn.execute("INSERT INTO items VALUES ('Gamma', 'B', 3.0)")
    conn.execute("INSERT INTO items VALUES ('Delta', 'B', 4.0)")
    conn.execute("INSERT INTO items VALUES ('Epsilon', 'A', 5.0)")
    yield conn
    conn.close()


def test_where_eq_executes(duckdb_conn):
    """where_eq produces valid SQL that returns correct rows."""
    sql, params = SQLBuilder("items").where_eq("name", "Gamma").build()
    rows = duckdb_conn.execute(sql, params).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "Gamma"


def test_group_by_having_executes(duckdb_conn):
    """GROUP BY + HAVING produces valid aggregate SQL."""
    sql, params = (
        SQLBuilder("items")
        .select("category", "COUNT(*) AS cnt")
        .group_by("category")
        .having("COUNT(*) >= $1", 3)
        .build()
    )
    rows = duckdb_conn.execute(sql, params).fetchall()
    # Category A has 3 items, B has 2 — only A passes HAVING >= 3
    assert len(rows) == 1
    assert rows[0][0] == "A"
    assert rows[0][1] == 3


def test_where_or_executes(duckdb_conn):
    """where_or produces valid SQL returning union of matches."""
    sql, params = (
        SQLBuilder("items")
        .where_or(("name = $1", "Alpha"), ("name = $1", "Delta"))
        .order_by("name")
        .build()
    )
    rows = duckdb_conn.execute(sql, params).fetchall()
    assert len(rows) == 2
    assert rows[0][0] == "Alpha"
    assert rows[1][0] == "Delta"


def test_distinct_executes(duckdb_conn):
    """DISTINCT produces valid SQL with no duplicates."""
    sql, params = (
        SQLBuilder("items").select("category").distinct().order_by("category").build()
    )
    rows = duckdb_conn.execute(sql, params).fetchall()
    assert len(rows) == 2
    assert rows[0][0] == "A"
    assert rows[1][0] == "B"
