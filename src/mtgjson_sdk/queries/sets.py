"""Set query module."""

from __future__ import annotations

from typing import Any

from pydantic import TypeAdapter

from .._sql import SQLBuilder
from ..connection import Connection
from ..models.sets import SetList

_SET_LIST_ADAPTER = TypeAdapter(list[SetList])


class SetQuery:
    """Query interface for MTG set metadata.

    Example::

        mh3 = sdk.sets.get("MH3")
        expansions = sdk.sets.list(set_type="expansion")
        results = sdk.sets.search(name="Modern", set_type="masters")
    """

    def __init__(self, conn: Connection) -> None:
        self._conn = conn

    def _ensure(self) -> None:
        self._conn.ensure_views("sets")

    def get(
        self,
        code: str,
        *,
        as_dict: bool = False,
        as_dataframe: bool = False,
    ) -> SetList | dict | Any | None:
        """Get a set by its code.

        Args:
            code: The set code (e.g. ``"MH3"``). Case-insensitive.
            as_dict: Return a raw dict instead of a Pydantic model.
            as_dataframe: Return a Polars DataFrame.

        Returns:
            A SetList model, dict, or DataFrame — or None if not found.
        """
        self._ensure()
        sql = "SELECT * FROM sets WHERE code = $1"
        if as_dataframe:
            return self._conn.execute_df(sql, [code.upper()])
        rows = self._conn.execute(sql, [code.upper()])
        if not rows:
            return None
        if as_dict:
            return rows[0]
        return SetList.model_validate(rows[0])

    def list(
        self,
        *,
        set_type: str | None = None,
        name: str | None = None,
        limit: int = 1000,
        offset: int = 0,
        as_dict: bool = False,
        as_dataframe: bool = False,
    ) -> list[SetList] | list[dict] | Any:
        """List sets with optional filters, ordered by release date.

        Args:
            set_type: Filter by set type (e.g. ``"expansion"``, ``"masters"``).
            name: Filter by name (exact match, or LIKE with ``%`` wildcard).
            limit: Maximum results (default 1000).
            offset: Result offset for pagination.
            as_dict: Return raw dicts instead of models.
            as_dataframe: Return a Polars DataFrame.

        Returns:
            List of SetList models, dicts, or a DataFrame.
        """
        self._ensure()
        q = SQLBuilder("sets")

        if set_type:
            q.where_eq("type", set_type)

        if name:
            if "%" in name:
                q.where_like("name", name)
            else:
                q.where_eq("name", name)

        q.order_by("releaseDate DESC")
        q.limit(limit).offset(offset)

        sql, params = q.build()
        if as_dataframe:
            return self._conn.execute_df(sql, params)
        if as_dict:
            return self._conn.execute(sql, params)
        return self._conn.execute_models(sql, params, adapter=_SET_LIST_ADAPTER)

    def search(
        self,
        *,
        name: str | None = None,
        set_type: str | None = None,
        block: str | None = None,
        release_year: int | None = None,
        limit: int = 100,
        as_dict: bool = False,
        as_dataframe: bool = False,
    ) -> list[SetList] | list[dict] | Any:
        """Search sets with flexible filters.

        Args:
            name: Substring search in set name (LIKE ``%name%``).
            set_type: Filter by set type (e.g. ``"expansion"``, ``"masters"``).
            block: Substring search in block name.
            release_year: Filter by release year (e.g. ``2024``).
            limit: Maximum results (default 100).
            as_dict: Return raw dicts instead of models.
            as_dataframe: Return a Polars DataFrame.

        Returns:
            List of matching SetList models, dicts, or a DataFrame.
        """
        self._ensure()
        q = SQLBuilder("sets")

        if name:
            q.where_like("name", f"%{name}%")

        if set_type:
            q.where_eq("type", set_type)

        if block:
            q.where_like("block", f"%{block}%")

        if release_year:
            idx = len(q._params) + 1
            q._where.append(f"EXTRACT(YEAR FROM CAST(releaseDate AS DATE)) = ${idx}")
            q._params.append(release_year)

        q.order_by("releaseDate DESC")
        q.limit(limit)

        sql, params = q.build()
        if as_dataframe:
            return self._conn.execute_df(sql, params)
        if as_dict:
            return self._conn.execute(sql, params)
        return self._conn.execute_models(sql, params, adapter=_SET_LIST_ADAPTER)

    def get_financial_summary(
        self,
        set_code: str,
        *,
        provider: str = "tcgplayer",
        currency: str = "USD",
        finish: str = "normal",
        category: str = "retail",
    ) -> dict | None:
        """Get aggregate price statistics for a set.

        Joins cards with prices and computes totals, averages, min/max,
        and card count — all server-side in DuckDB.  Useful for EV
        calculations and market overview.

        Args:
            set_code: Set code (e.g. "MH3").
            provider: Price provider (default "tcgplayer").
            currency: Currency code (default "USD").
            finish: Card finish (default "normal").
            category: Price category (default "retail").

        Returns:
            Dict with total_value, avg_value, min_value, max_value,
            card_count, and date — or None if no price data exists.
        """
        self._conn.ensure_views("cards")
        # prices_today is a DuckDB table loaded from JSON, not a parquet view.
        # PriceQuery._ensure() handles loading. We can't call it here directly,
        # so we check if the table is registered and guide the user if not.
        if "prices_today" not in self._conn._registered_views:
            return None

        sql = """
            SELECT
                COUNT(DISTINCT c.uuid) AS card_count,
                ROUND(SUM(p.price), 2) AS total_value,
                ROUND(AVG(p.price), 2) AS avg_value,
                MIN(p.price) AS min_value,
                MAX(p.price) AS max_value,
                MAX(p.date) AS date
            FROM cards c
            JOIN prices_today p ON c.uuid = p.uuid
            WHERE c.setCode = $1
              AND p.provider = $2
              AND p.currency = $3
              AND p.finish = $4
              AND p.category = $5
              AND p.date = (
                  SELECT MAX(p2.date) FROM prices_today p2
              )
        """
        rows = self._conn.execute(
            sql, [set_code.upper(), provider, currency, finish, category]
        )
        if not rows or rows[0].get("card_count", 0) == 0:
            return None
        return rows[0]

    def count(self) -> int:
        """Count total number of sets in the dataset.

        Returns:
            Total set count.
        """
        self._ensure()
        return self._conn.execute_scalar("SELECT COUNT(*) FROM sets") or 0
