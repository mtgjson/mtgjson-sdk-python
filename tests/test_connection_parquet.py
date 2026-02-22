"""Integration tests for Connection view creation from real Parquet files.

These tests exercise the full _ensure_view → _build_csv_replace pipeline
that conftest's register_table_from_data() bypasses. They write real
Parquet files with pyarrow, then verify Connection introspects and
transforms columns correctly.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mtgjson_sdk.cache import CacheManager
from mtgjson_sdk.connection import Connection


@pytest.fixture
def parquet_conn(tmp_path):
    """Connection whose cache.ensure_parquet() returns temp parquet files.

    Usage: call ``write_parquet(view_name, table)`` to write a parquet file,
    then ``conn.ensure_views(view_name)`` to trigger the real _ensure_view path.
    """
    cache = CacheManager(tmp_path / "cache", offline=True)
    conn = Connection(cache)
    paths: dict[str, Path] = {}

    original_ensure = cache.ensure_parquet

    def patched_ensure(view_name: str) -> Path:
        if view_name in paths:
            return paths[view_name]
        return original_ensure(view_name)

    cache.ensure_parquet = patched_ensure

    def write_parquet(view_name: str, table: pa.Table) -> Path:
        p = tmp_path / f"{view_name}.parquet"
        pq.write_table(table, p)
        paths[view_name] = p
        return p

    conn._write_parquet = write_parquet
    yield conn
    conn.close()
    cache.close()


# === CSV column splitting tests ===


def test_csv_column_split(parquet_conn):
    """VARCHAR columns in _STATIC_LIST_COLUMNS are split into arrays."""
    table = pa.table(
        {
            "uuid": ["u1", "u2"],
            "name": ["Bolt", "Spell"],
            "colors": ["R, U", "W"],
            "types": ["Instant", "Sorcery, Instant"],
            "subtypes": ["", "Wizard"],
        }
    )
    parquet_conn._write_parquet("cards", table)
    parquet_conn.ensure_views("cards")

    rows = parquet_conn.execute("SELECT * FROM cards ORDER BY uuid")
    assert rows[0]["colors"] == ["R", "U"]
    assert rows[0]["types"] == ["Instant"]
    assert rows[1]["types"] == ["Sorcery", "Instant"]


def test_csv_empty_string_becomes_empty_list(parquet_conn):
    """Empty string in a CSV column should become an empty list."""
    table = pa.table(
        {
            "uuid": ["u1"],
            "colors": [""],
            "types": [""],
            "subtypes": [""],
        }
    )
    parquet_conn._write_parquet("cards", table)
    parquet_conn.ensure_views("cards")

    rows = parquet_conn.execute("SELECT * FROM cards")
    assert rows[0]["colors"] == []
    assert rows[0]["types"] == []
    assert rows[0]["subtypes"] == []


def test_csv_null_becomes_empty_list(parquet_conn):
    """NULL in a CSV column should become an empty list."""
    table = pa.table(
        {
            "uuid": ["u1"],
            "colors": pa.array([None], type=pa.string()),
            "types": pa.array([None], type=pa.string()),
            "subtypes": pa.array([None], type=pa.string()),
        }
    )
    parquet_conn._write_parquet("cards", table)
    parquet_conn.ensure_views("cards")

    rows = parquet_conn.execute("SELECT * FROM cards")
    assert rows[0]["colors"] == []
    assert rows[0]["types"] == []


# === JSON cast tests ===


def test_json_cast_columns(parquet_conn):
    """VARCHAR columns in _JSON_CAST_COLUMNS are cast to JSON type."""
    table = pa.table(
        {
            "uuid": ["u1"],
            "name": ["Bolt"],
            "identifiers": ['{"scryfallId":"abc-123","mtgoId":"456"}'],
            "legalities": ['{"modern":"Legal","vintage":"Restricted"}'],
        }
    )
    parquet_conn._write_parquet("cards", table)
    parquet_conn.ensure_views("cards")

    # DuckDB JSON extraction should work
    rows = parquet_conn.execute("SELECT identifiers->>'scryfallId' AS sid FROM cards")
    assert rows[0]["sid"] == "abc-123"

    rows = parquet_conn.execute("SELECT legalities->>'modern' AS status FROM cards")
    assert rows[0]["status"] == "Legal"


# === Ignored columns tests ===


def test_ignored_columns_not_split(parquet_conn):
    """Columns in _IGNORED_COLUMNS remain scalar strings even if plural."""
    table = pa.table(
        {
            "uuid": ["u1"],
            "toughness": ["3"],
            "text": ["deals 3 damage to any target"],
            "status": ["Legal"],
        }
    )
    parquet_conn._write_parquet("cards", table)
    parquet_conn.ensure_views("cards")

    rows = parquet_conn.execute("SELECT * FROM cards")
    # These must remain strings, NOT split into lists
    assert rows[0]["toughness"] == "3"
    assert rows[0]["text"] == "deals 3 damage to any target"
    assert rows[0]["status"] == "Legal"


# === Legalities UNPIVOT tests ===


def test_legalities_unpivot(parquet_conn):
    """Wide-format legalities are UNPIVOTed to (uuid, format, status) rows."""
    table = pa.table(
        {
            "uuid": ["u1", "u2"],
            "modern": ["Legal", "Banned"],
            "legacy": ["Legal", "Legal"],
            "vintage": ["Restricted", "Legal"],
        }
    )
    parquet_conn._write_parquet("card_legalities", table)
    parquet_conn.ensure_views("card_legalities")

    rows = parquet_conn.execute("SELECT * FROM card_legalities ORDER BY uuid, format")
    # u1 has 3 rows, u2 has 3 rows = 6 total
    assert len(rows) == 6
    # Verify structure
    assert all("uuid" in r and "format" in r and "status" in r for r in rows)
    # Verify specific values
    u1_vintage = [r for r in rows if r["uuid"] == "u1" and r["format"] == "vintage"]
    assert len(u1_vintage) == 1
    assert u1_vintage[0]["status"] == "Restricted"


def test_legalities_null_filtered(parquet_conn):
    """NULL status values are excluded from the UNPIVOTed legalities."""
    table = pa.table(
        {
            "uuid": ["u1"],
            "modern": ["Legal"],
            "legacy": pa.array([None], type=pa.string()),
            "vintage": ["Restricted"],
        }
    )
    parquet_conn._write_parquet("card_legalities", table)
    parquet_conn.ensure_views("card_legalities")

    rows = parquet_conn.execute("SELECT * FROM card_legalities ORDER BY format")
    # legacy is NULL so should be excluded — only modern + vintage = 2 rows
    assert len(rows) == 2
    formats = {r["format"] for r in rows}
    assert formats == {"modern", "vintage"}


# === Dynamic heuristic test ===


def test_dynamic_heuristic_new_plural(parquet_conn):
    """New VARCHAR columns ending in 's' are auto-detected as lists."""
    table = pa.table(
        {
            "uuid": ["u1"],
            "name": ["Test"],
            "attractions": ["Light1, Light2, Light3"],
        }
    )
    parquet_conn._write_parquet("cards", table)
    parquet_conn.ensure_views("cards")

    rows = parquet_conn.execute("SELECT * FROM cards")
    # 'attractions' ends in 's', is VARCHAR, not in _IGNORED_COLUMNS
    # → should be auto-split
    assert rows[0]["attractions"] == ["Light1", "Light2", "Light3"]
