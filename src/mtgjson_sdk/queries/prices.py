"""Price query module."""

from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any

from ..cache import CacheManager
from ..connection import Connection

logger = logging.getLogger("mtgjson_sdk")


class PriceQuery:
    """Query interface for card price data.

    Prices come from AllPricesToday.json.gz (not parquet),
    so we download, flatten, and load into a DuckDB table.

    Example::

        prices = sdk.prices.today("uuid-here", provider="tcgplayer")
        trend = sdk.prices.price_trend("uuid-here", finish="foil")
        cheapest = sdk.prices.cheapest_printing("Lightning Bolt")
    """

    def __init__(self, conn: Connection, cache: CacheManager) -> None:
        self._conn = conn
        self._cache = cache
        self._loaded = False

    def _ensure(self) -> None:
        """Load price data into DuckDB if not already done.

        Uses streaming NDJSON to avoid holding the full flattened
        row list in Python memory (~3-4x reduction in peak memory).
        """
        if self._loaded:
            return
        if "prices_today" in self._conn._registered_views:
            self._loaded = True
            return

        try:
            path = self._cache.ensure_json("all_prices_today")
        except FileNotFoundError:
            logger.warning("Price data not available")
            self._loaded = True
            return

        _load_prices_to_duckdb(path, self._conn)
        self._loaded = True

    def get(self, uuid: str) -> dict | None:
        """Get full price data for a card UUID.

        Args:
            uuid: The MTGJSON UUID of the card.

        Returns:
            Nested dict ``{source: {provider: {currency, retail, buylist}}}``
            or None if no price data exists.
        """
        self._ensure()
        if "prices_today" not in self._conn._registered_views:
            return None
        rows = self._conn.execute(
            "SELECT * FROM prices_today WHERE uuid = $1 "
            "ORDER BY source, provider, category, finish, date",
            [uuid],
        )
        if not rows:
            return None
        # Reconstruct nested structure from flat rows
        result: dict = {}
        for r in rows:
            src = result.setdefault(r["source"], {})
            prov = src.setdefault(r["provider"], {"currency": r.get("currency", "USD")})
            cat = prov.setdefault(r["category"], {})
            fin = cat.setdefault(r["finish"], {})
            fin[r["date"]] = r["price"]
        return result

    def today(
        self,
        uuid: str,
        *,
        provider: str | None = None,
        finish: str | None = None,
        category: str | None = None,
        as_dict: bool = False,
        as_dataframe: bool = False,
    ) -> list[dict] | Any:
        """Get the latest prices for a card UUID.

        Returns only the most recent date's prices for each
        provider/finish/category combination.

        Args:
            uuid: Card UUID.
            provider: Filter by provider (e.g. ``"tcgplayer"``, ``"cardmarket"``).
            finish: Filter by finish (e.g. ``"normal"``, ``"foil"``, ``"etched"``).
            category: Filter by category (``"retail"`` or ``"buylist"``).
            as_dict: Return raw dicts (default behavior, included for API consistency).
            as_dataframe: Return a Polars DataFrame.

        Returns:
            List of price row dicts, or a Polars DataFrame.
        """
        self._ensure()
        if "prices_today" not in self._conn._registered_views:
            return [] if not as_dataframe else None
        # Subquery to get the max date per grouping, then filter
        parts = [
            "SELECT * FROM prices_today",
            "WHERE uuid = $1",
            "AND date = (SELECT MAX(p2.date) FROM prices_today p2 WHERE p2.uuid = $1)",
        ]
        params: list[Any] = [uuid]
        idx = 2

        if provider:
            parts.append(f"AND provider = ${idx}")
            params.append(provider)
            idx += 1

        if finish:
            parts.append(f"AND finish = ${idx}")
            params.append(finish)
            idx += 1

        if category:
            parts.append(f"AND category = ${idx}")
            params.append(category)
            idx += 1

        sql = " ".join(parts)
        if as_dataframe:
            return self._conn.execute_df(sql, params)
        rows = self._conn.execute(sql, params)
        if as_dict:
            return rows
        return rows

    def history(
        self,
        uuid: str,
        *,
        provider: str | None = None,
        finish: str | None = None,
        category: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        as_dict: bool = False,
        as_dataframe: bool = False,
    ) -> list[dict] | Any:
        """Get price history for a card UUID.

        Args:
            uuid: Card UUID.
            provider: Filter by provider.
            finish: Filter by finish.
            category: Filter by category (``"retail"`` or ``"buylist"``).
            date_from: Start date (inclusive, ISO format ``YYYY-MM-DD``).
            date_to: End date (inclusive, ISO format ``YYYY-MM-DD``).
            as_dict: Return raw dicts (default behavior).
            as_dataframe: Return a Polars DataFrame.

        Returns:
            List of price row dicts ordered by date, or a DataFrame.
        """
        self._ensure()
        if "prices_today" not in self._conn._registered_views:
            return [] if not as_dataframe else None
        parts = ["SELECT * FROM prices_today WHERE uuid = $1"]
        params: list[Any] = [uuid]
        idx = 2

        if provider:
            parts.append(f"AND provider = ${idx}")
            params.append(provider)
            idx += 1

        if finish:
            parts.append(f"AND finish = ${idx}")
            params.append(finish)
            idx += 1

        if category:
            parts.append(f"AND category = ${idx}")
            params.append(category)
            idx += 1

        if date_from:
            parts.append(f"AND date >= ${idx}")
            params.append(date_from)
            idx += 1

        if date_to:
            parts.append(f"AND date <= ${idx}")
            params.append(date_to)
            idx += 1

        parts.append("ORDER BY date ASC")

        sql = " ".join(parts)
        if as_dataframe:
            return self._conn.execute_df(sql, params)
        rows = self._conn.execute(sql, params)
        if as_dict:
            return rows
        return rows

    def price_trend(
        self,
        uuid: str,
        *,
        provider: str | None = None,
        finish: str | None = None,
        category: str = "retail",
    ) -> dict | None:
        """Get price trend statistics for a card.

        Args:
            uuid: Card UUID.
            provider: Filter by provider (e.g. ``"tcgplayer"``).
            finish: Filter by finish (e.g. ``"normal"``, ``"foil"``).
            category: Price category (default ``"retail"``).

        Returns:
            Dict with ``min_price``, ``max_price``, ``avg_price``,
            ``first_date``, ``last_date``, and ``data_points`` — or
            None if no price data exists.
        """
        self._ensure()
        if "prices_today" not in self._conn._registered_views:
            return None
        parts = [
            "SELECT",
            "  MIN(price) AS min_price,",
            "  MAX(price) AS max_price,",
            "  ROUND(AVG(price), 2) AS avg_price,",
            "  MIN(date) AS first_date,",
            "  MAX(date) AS last_date,",
            "  COUNT(*) AS data_points",
            "FROM prices_today",
            "WHERE uuid = $1 AND category = $2",
        ]
        params: list[Any] = [uuid, category]
        idx = 3

        if provider:
            parts.append(f"AND provider = ${idx}")
            params.append(provider)
            idx += 1

        if finish:
            parts.append(f"AND finish = ${idx}")
            params.append(finish)
            idx += 1

        sql = " ".join(parts)
        rows = self._conn.execute(sql, params)
        if not rows or rows[0].get("data_points", 0) == 0:
            return None
        return rows[0]

    def cheapest_printing(
        self,
        name: str,
        *,
        provider: str = "tcgplayer",
        finish: str = "normal",
        category: str = "retail",
    ) -> dict | None:
        """Find the cheapest printing of a card by name.

        Args:
            name: Exact card name (e.g. ``"Lightning Bolt"``).
            provider: Price provider (default ``"tcgplayer"``).
            finish: Card finish (default ``"normal"``).
            category: Price category (default ``"retail"``).

        Returns:
            Dict with ``uuid``, ``setCode``, ``number``, ``price``,
            ``date`` — or None if no price data exists.
        """
        self._ensure()
        self._conn.ensure_views("cards")
        sql = (
            "SELECT c.uuid, c.setCode, c.number, p.price, p.date "
            "FROM cards c "
            "JOIN prices_today p ON c.uuid = p.uuid "
            "WHERE c.name = $1 AND p.provider = $2 "
            "AND p.finish = $3 AND p.category = $4 "
            "AND p.date = (SELECT MAX(p2.date) FROM prices_today p2 "
            "WHERE p2.uuid = c.uuid AND p2.provider = $2 "
            "AND p2.finish = $3 AND p2.category = $4) "
            "ORDER BY p.price ASC "
            "LIMIT 1"
        )
        rows = self._conn.execute(sql, [name, provider, finish, category])
        return rows[0] if rows else None

    def cheapest_printings(
        self,
        *,
        provider: str = "tcgplayer",
        finish: str = "normal",
        category: str = "retail",
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """Find the cheapest available printing of each card.

        Uses DuckDB's ``arg_min()`` for efficient single-pass aggregation
        — no window functions needed.

        Args:
            provider: Price provider (default ``"tcgplayer"``).
            finish: Card finish (default ``"normal"``).
            category: Price category (default ``"retail"``).
            limit: Maximum results (default 100).
            offset: Result offset for pagination.

        Returns:
            List of dicts with ``name``, ``cheapest_set``,
            ``cheapest_number``, ``cheapest_uuid``, ``min_price``.
        """
        self._ensure()
        self._conn.ensure_views("cards")
        if "prices_today" not in self._conn._registered_views:
            return []

        sql = (
            "SELECT c.name, "
            "  arg_min(c.setCode, p.price) AS cheapest_set, "
            "  arg_min(c.number, p.price) AS cheapest_number, "
            "  arg_min(c.uuid, p.price) AS cheapest_uuid, "
            "  MIN(p.price) AS min_price "
            "FROM cards c "
            "JOIN prices_today p ON c.uuid = p.uuid "
            "WHERE p.provider = $1 AND p.finish = $2 AND p.category = $3 "
            "AND p.date = (SELECT MAX(date) FROM prices_today) "
            "GROUP BY c.name "
            "ORDER BY min_price ASC "
            f"LIMIT {limit} OFFSET {offset}"
        )
        return self._conn.execute(sql, [provider, finish, category])

    def most_expensive_printings(
        self,
        *,
        provider: str = "tcgplayer",
        finish: str = "normal",
        category: str = "retail",
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """Find the most expensive printing of each card.

        Uses DuckDB's ``arg_max()`` for efficient single-pass aggregation.

        Args:
            provider: Price provider (default ``"tcgplayer"``).
            finish: Card finish (default ``"normal"``).
            category: Price category (default ``"retail"``).
            limit: Maximum results (default 100).
            offset: Result offset for pagination.

        Returns:
            List of dicts with ``name``, ``priciest_set``,
            ``priciest_number``, ``priciest_uuid``, ``max_price``.
        """
        self._ensure()
        self._conn.ensure_views("cards")
        if "prices_today" not in self._conn._registered_views:
            return []

        sql = (
            "SELECT c.name, "
            "  arg_max(c.setCode, p.price) AS priciest_set, "
            "  arg_max(c.number, p.price) AS priciest_number, "
            "  arg_max(c.uuid, p.price) AS priciest_uuid, "
            "  MAX(p.price) AS max_price "
            "FROM cards c "
            "JOIN prices_today p ON c.uuid = p.uuid "
            "WHERE p.provider = $1 AND p.finish = $2 AND p.category = $3 "
            "AND p.date = (SELECT MAX(date) FROM prices_today) "
            "GROUP BY c.name "
            "ORDER BY max_price DESC "
            f"LIMIT {limit} OFFSET {offset}"
        )
        return self._conn.execute(sql, [provider, finish, category])


def _load_prices_to_duckdb(path: Path, conn: Connection) -> None:
    """Parse AllPricesToday JSON, stream-flatten to NDJSON, load into DuckDB.

    Memory optimization: instead of accumulating millions of flat dicts in a
    Python list (which doubles peak memory), we stream each row directly to
    an NDJSON temp file. DuckDB then reads the file in a single pass.

    Peak memory: ~1x (parsed JSON dict) instead of ~3-4x (dict + list + temp file).
    """
    import gzip

    # Parse JSON — required for deeply nested structure, can't avoid this
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as f:
            raw = json.load(f)
    else:
        raw = json.loads(path.read_text(encoding="utf-8"))

    data = raw.get("data", {})
    del raw  # Free wrapper dict immediately

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".ndjson")
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8", buffering=1024 * 1024) as ndjson:
            count = _stream_flatten_prices(data, ndjson)
        del data  # Free source dict before DuckDB ingestion

        if count > 0:
            conn.register_table_from_ndjson("prices_today", tmp_path)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def _stream_flatten_prices(data: dict[str, Any], out: Any) -> int:
    """Stream-flatten nested price data to an NDJSON file handle.

    Writes one JSON line per price point, avoiding a large intermediate list.
    Returns the number of rows written.

    Input structure::

        {uuid: {paper: {provider: {currency, buylist, retail}}, mtgo: {...}}}

    Output per line::

        {uuid, source, provider, currency, category, finish, date, price}
    """
    count = 0
    for uuid, formats in data.items():
        if not isinstance(formats, dict):
            continue
        for source, providers in formats.items():  # paper, mtgo
            if not isinstance(providers, dict):
                continue
            for (
                provider,
                price_data,
            ) in providers.items():  # tcgplayer, cardkingdom, etc.
                if not isinstance(price_data, dict):
                    continue
                currency = price_data.get("currency", "USD")
                for category_name in ("buylist", "retail"):
                    category_data = price_data.get(category_name)
                    if not isinstance(category_data, dict):
                        continue
                    for (
                        finish,
                        date_prices,
                    ) in category_data.items():  # normal, foil, etched
                        if not isinstance(date_prices, dict):
                            continue
                        for date, price in date_prices.items():
                            if price is not None:
                                out.write(
                                    json.dumps(
                                        {
                                            "uuid": uuid,
                                            "source": source,
                                            "provider": provider,
                                            "currency": currency,
                                            "category": category_name,
                                            "finish": finish,
                                            "date": date,
                                            "price": float(price),
                                        },
                                        separators=(",", ":"),
                                    )
                                )
                                out.write("\n")
                                count += 1
    return count
