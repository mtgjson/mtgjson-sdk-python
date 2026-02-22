"""DuckDB connection wrapper with view registration and query execution."""

from __future__ import annotations

import logging
from typing import Any

import duckdb

from .cache import CacheManager

logger = logging.getLogger("mtgjson_sdk")

# Known list columns that don't follow the plural naming convention
# (e.g. colorIdentity, availability, producedMana). Always converted
# to arrays regardless of heuristic detection.
_STATIC_LIST_COLUMNS: dict[str, frozenset[str]] = {
    "cards": frozenset(
        {
            "artistIds",
            "attractionLights",
            "availability",
            "boosterTypes",
            "cardParts",
            "colorIdentity",
            "colorIndicator",
            "colors",
            "finishes",
            "frameEffects",
            "keywords",
            "originalPrintings",
            "otherFaceIds",
            "printings",
            "producedMana",
            "promoTypes",
            "rebalancedPrintings",
            "subsets",
            "subtypes",
            "supertypes",
            "types",
            "variations",
        }
    ),
    "tokens": frozenset(
        {
            "artistIds",
            "availability",
            "boosterTypes",
            "colorIdentity",
            "colorIndicator",
            "colors",
            "finishes",
            "frameEffects",
            "keywords",
            "otherFaceIds",
            "producedMana",
            "promoTypes",
            "reverseRelated",
            "subtypes",
            "supertypes",
            "types",
        }
    ),
}

# VARCHAR columns that are definitely NOT lists, even if they match the
# plural-name heuristic. Prevents splitting text fields that contain commas,
# JSON struct fields, and other scalar strings.
_IGNORED_COLUMNS = frozenset(
    {
        "text",
        "originalText",
        "flavorText",
        "printedText",
        "identifiers",
        "legalities",
        "leadershipSkills",
        "purchaseUrls",
        "relatedCards",
        "rulings",
        "sourceProducts",
        "foreignData",
        "translations",
        "toughness",
        "status",
        "format",
        "uris",
        "scryfallUri",
    }
)

# VARCHAR columns containing JSON strings that should be cast to DuckDB's
# JSON type.  This enables SQL operators like ->>, json_extract(), etc.
# Example:  SELECT identifiers->>'scryfallId' FROM cards
_JSON_CAST_COLUMNS = frozenset(
    {
        "identifiers",
        "legalities",
        "leadershipSkills",
        "purchaseUrls",
        "relatedCards",
        "rulings",
        "sourceProducts",
        "foreignData",
        "translations",
    }
)


class Connection:
    """Wraps a DuckDB connection and registers parquet files as views.

    Uses schema introspection to adapt views dynamically:
    - CSV VARCHAR columns are auto-detected and converted to arrays
    - Wide-format legalities are auto-UNPIVOTed to (uuid, format, status) rows
    """

    def __init__(self, cache: CacheManager) -> None:
        """Create a connection backed by the given cache.

        Args:
            cache: CacheManager used to download/locate parquet files.
        """
        self.cache = cache
        self._conn: duckdb.DuckDBPyConnection = duckdb.connect(":memory:")
        self._registered_views: set[str] = set()

    def close(self) -> None:
        """Close the underlying DuckDB connection and free resources."""
        if self._conn:
            self._conn.close()

    def _ensure_view(self, view_name: str) -> None:
        """Lazily register a parquet file as a DuckDB view.

        Introspects the parquet schema on first registration and builds
        the view SQL dynamically, so the SDK adapts to upstream schema
        changes without code updates.
        """
        if view_name in self._registered_views:
            return
        path = self.cache.ensure_parquet(view_name)
        # Use forward slashes for DuckDB compatibility
        path_str = str(path).replace("\\", "/")

        if view_name == "card_legalities":
            self._register_legalities_view(path_str)
            return

        # Hybrid CSV→array detection: static baseline + dynamic heuristic
        replace_clause = self._build_csv_replace(path_str, view_name)

        self._conn.execute(
            f"CREATE OR REPLACE VIEW {view_name} AS "
            f"SELECT *{replace_clause} FROM read_parquet('{path_str}')"
        )
        self._registered_views.add(view_name)
        logger.debug("Registered view: %s -> %s", view_name, path_str)

    def _build_csv_replace(self, path_str: str, view_name: str) -> str:
        """Build a REPLACE clause using a hybrid static + dynamic approach.

        Four layers:
        1. Static baseline: _STATIC_LIST_COLUMNS for known non-plural lists
           (colorIdentity, availability, producedMana, etc.)
        2. Dynamic heuristic: VARCHAR columns ending in 's' are likely lists
           (auto-catches new columns like stickerSheets, attractions, etc.)
        3. Safety blocklist: _IGNORED_COLUMNS prevents splitting text fields
           and JSON struct strings that happen to match the heuristic.
        4. JSON casting: _JSON_CAST_COLUMNS are cast to DuckDB JSON type,
           enabling ->>, json_extract(), etc.

        Only reads the parquet footer (DESCRIBE) — no data scanning needed.
        """
        result = self._conn.execute(
            f"SELECT column_name, column_type FROM "
            f"(DESCRIBE SELECT * FROM read_parquet('{path_str}'))"
        )
        schema = {row[0]: row[1] for row in result.fetchall()}

        # Build candidate set from both layers
        candidates: set[str] = set()

        # Layer 1: Static baseline (the "knowns")
        if view_name in _STATIC_LIST_COLUMNS:
            candidates.update(_STATIC_LIST_COLUMNS[view_name])

        # Layer 2: Dynamic heuristic (the "unknowns")
        for col, dtype in schema.items():
            if dtype != "VARCHAR":
                continue
            if col in _IGNORED_COLUMNS:
                continue
            if col.endswith("s"):
                candidates.add(col)

        # Filter to columns that actually exist as VARCHAR in this file
        final_cols = sorted(
            col for col in candidates if col in schema and schema[col] == "VARCHAR"
        )

        exprs = []
        for col in final_cols:
            exprs.append(
                f"""CASE WHEN "{col}" IS NULL OR TRIM("{col}") = '' """
                f"""THEN []::VARCHAR[] """
                f"""ELSE string_split("{col}", ', ') END AS "{col}" """
            )

        # Layer 4: JSON casting for struct-like VARCHAR columns
        for col in sorted(_JSON_CAST_COLUMNS):
            if col in schema and schema[col] == "VARCHAR":
                exprs.append(f'TRY_CAST("{col}" AS JSON) AS "{col}"')

        if not exprs:
            return ""
        return " REPLACE (" + ", ".join(exprs) + ")"

    def _register_legalities_view(self, path_str: str) -> None:
        """Register card_legalities by dynamically UNPIVOTing wide format.

        Introspects the parquet schema and UNPIVOTs all columns except 'uuid'
        into (uuid, format, status) rows. Automatically picks up new formats
        (e.g. 'timeless', 'oathbreaker') as they appear in the data.
        """
        schema_info = self._conn.execute(
            f"SELECT column_name FROM "
            f"(DESCRIBE SELECT * FROM read_parquet('{path_str}'))"
        ).fetchall()
        all_cols = [row[0] for row in schema_info]

        # Everything except 'uuid' is a format column
        static_cols = {"uuid"}
        format_cols = [c for c in all_cols if c not in static_cols]

        if not format_cols:
            # Fallback: assume row format (test data or different schema)
            self._conn.execute(
                f"CREATE OR REPLACE VIEW card_legalities AS "
                f"SELECT * FROM read_parquet('{path_str}')"
            )
        else:
            cols_sql = ", ".join(f'"{c}"' for c in format_cols)
            self._conn.execute(
                f"CREATE OR REPLACE VIEW card_legalities AS "
                f"SELECT uuid, format, status FROM ("
                f"  UNPIVOT (SELECT * FROM read_parquet('{path_str}'))"
                f"  ON {cols_sql}"
                f"  INTO NAME format VALUE status"
                f") WHERE status IS NOT NULL"
            )
        self._registered_views.add("card_legalities")
        logger.debug(
            "Registered legalities view (UNPIVOT %d formats): %s",
            len(format_cols),
            path_str,
        )

    def register_table_from_data(
        self, table_name: str, data: list[dict[str, Any]]
    ) -> None:
        """Create a DuckDB table from a list of dicts.

        Writes data as a temporary JSON array file and reads it with DuckDB.
        Primarily used by unit tests with small sample data.
        For large datasets, prefer register_table_from_ndjson().

        Args:
            table_name: Name for the new DuckDB table.
            data: List of row dicts to load.
        """
        if not data:
            return
        self._conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        import json as _json
        import os
        import tempfile

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as f:
            _json.dump(data, f)
            tmp_path = f.name.replace("\\", "/")
        try:
            self._conn.execute(
                f"CREATE TABLE {table_name} AS "
                f"SELECT * FROM read_json_auto('{tmp_path}')"
            )
        finally:
            os.unlink(f.name)
        self._registered_views.add(table_name)

    def register_table_from_ndjson(self, table_name: str, ndjson_path: str) -> None:
        """Create a DuckDB table from a newline-delimited JSON file.

        More memory-efficient than register_table_from_data for large datasets,
        since data is streamed from disk rather than held in a Python list.

        Args:
            table_name: Name for the new DuckDB table.
            ndjson_path: Path to the NDJSON file.
        """
        self._conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        path_fwd = ndjson_path.replace("\\", "/")
        self._conn.execute(
            f"CREATE TABLE {table_name} AS "
            f"SELECT * FROM read_json_auto('{path_fwd}', format='newline_delimited')"
        )
        self._registered_views.add(table_name)

    def ensure_views(self, *view_names: str) -> None:
        """Ensure one or more views are registered, downloading data if needed.

        Args:
            *view_names: View names to register (e.g. ``"cards"``, ``"sets"``).
        """
        for name in view_names:
            self._ensure_view(name)

    def execute(
        self,
        sql: str,
        params: list[Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute SQL and return results as list of dicts.

        Automatically converts date/datetime objects to ISO format strings
        for compatibility with Pydantic models that expect string dates.
        Handles nested structs and lists recursively.

        Args:
            sql: SQL query string.
            params: Optional positional query parameters.

        Returns:
            List of row dicts.
        """
        if params:
            result = self._conn.execute(sql, params)
        else:
            result = self._conn.execute(sql)
        if result.description is None:
            return []
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            d: dict[str, Any] = {}
            for col, val in zip(columns, row):
                d[col] = _coerce_dates(val)
            out.append(d)
        return out

    def execute_json(
        self,
        sql: str,
        params: list[Any] | None = None,
    ) -> str:
        """Execute SQL and return results as a JSON string.

        Uses DuckDB's native ``to_json(list(...))`` serialization which
        bypasses Python dict construction entirely.  Combine with Pydantic
        V2's ``TypeAdapter.validate_json()`` for 2-5x faster model parsing
        compared to the dict-based ``execute()`` path.

        Args:
            sql: SQL query string.
            params: Optional positional query parameters.

        Returns:
            JSON array string (e.g. ``'[{"name":"Bolt",...},...]'``).
            Returns ``'[]'`` for empty result sets.
        """
        wrapped = f"SELECT to_json(list(sub)) FROM ({sql}) sub"
        if params:
            result = self._conn.execute(wrapped, params)
        else:
            result = self._conn.execute(wrapped)
        row = result.fetchone()
        # DuckDB returns None for empty result sets
        if row is None or row[0] is None:
            return "[]"
        return row[0]

    def execute_models(
        self,
        sql: str,
        params: list[Any] | None = None,
        *,
        adapter: Any = None,
    ) -> list[Any]:
        """Execute SQL and parse results directly into Pydantic models.

        Combines ``execute_json()`` with Pydantic V2's
        ``TypeAdapter.validate_json()`` for 2-5x faster model construction
        on large result sets — bypasses Python dict construction entirely.

        Args:
            sql: SQL query string.
            params: Optional query parameters.
            adapter: A ``pydantic.TypeAdapter`` instance for the target
                list type (e.g. ``TypeAdapter(list[CardSet])``).

        Returns:
            List of validated Pydantic model instances.
        """
        json_str = self.execute_json(sql, params)
        return adapter.validate_json(json_str)

    def execute_scalar(self, sql: str, params: list[Any] | None = None) -> Any:
        """Execute SQL and return a single scalar value.

        Args:
            sql: SQL query that returns one row, one column.
            params: Optional query parameters.

        Returns:
            The scalar value, or None if the result set is empty.
        """
        if params:
            result = self._conn.execute(sql, params)
        else:
            result = self._conn.execute(sql)
        row = result.fetchone()
        return row[0] if row else None

    def execute_df(self, sql: str, params: list[Any] | None = None) -> Any:
        """Execute SQL and return a Polars DataFrame.

        Args:
            sql: SQL query string.
            params: Optional query parameters.

        Returns:
            A ``polars.DataFrame``.

        Raises:
            ImportError: If ``polars`` is not installed.
        """
        try:
            import polars as pl
        except ImportError as err:
            raise ImportError(
                "polars is required for DataFrame output. "
                "Install with: pip install mtgjson-sdk[polars]"
            ) from err
        if params:
            result = self._conn.execute(sql, params)
        else:
            result = self._conn.execute(sql)
        return pl.from_arrow(result.fetch_arrow_table())

    @property
    def raw(self) -> duckdb.DuckDBPyConnection:
        """Access the underlying DuckDB connection for advanced usage.

        Use this when you need DuckDB features not exposed by the SDK
        (e.g. ``COPY``, ``EXPORT``, custom extensions).
        """
        return self._conn


def _coerce_dates(val: Any) -> Any:
    """Recursively convert date/datetime objects to ISO strings."""
    import datetime

    if isinstance(val, (datetime.date, datetime.datetime)):
        return val.isoformat()
    if isinstance(val, dict):
        return {k: _coerce_dates(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_coerce_dates(item) for item in val]
    return val
