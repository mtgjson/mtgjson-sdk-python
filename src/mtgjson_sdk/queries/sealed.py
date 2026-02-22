"""Sealed product query module."""

from __future__ import annotations

from typing import Any

from .._sql import SQLBuilder
from ..connection import Connection


class SealedQuery:
    """Query interface for sealed product data (booster boxes, bundles, etc.).

    Sealed product data lives inside the sets parquet as nested structs.
    Uses DuckDB's UNNEST for efficient server-side lookup.

    Example::

        products = sdk.sealed.list(set_code="MH3")
        product = sdk.sealed.get("uuid-here")
    """

    def __init__(self, conn: Connection) -> None:
        self._conn = conn

    def _ensure(self) -> None:
        self._conn.ensure_views("sets")

    def list(
        self,
        *,
        set_code: str | None = None,
        category: str | None = None,
        limit: int = 100,
        as_dict: bool = False,
        as_dataframe: bool = False,
    ) -> list[dict] | Any:
        """List sealed products.

        Note: Sealed products are nested within set data. This method
        queries the sets view and extracts sealed product data.
        Requires the sealedProduct column (present in AllPrintings
        or test data, but NOT in the flat sets.parquet).

        Args:
            set_code: Filter by set code (e.g. ``"MH3"``).
            category: Filter by product category.
            limit: Maximum number of sets to scan (default 100).
            as_dict: Return raw dicts (default behavior).
            as_dataframe: Return a Polars DataFrame.

        Returns:
            List of sealed product dicts with a ``setCode`` key added.
        """
        self._ensure()
        try:
            q = SQLBuilder("sets")
            q.select("code", "name AS setName", "sealedProduct")

            if set_code:
                q.where_eq("code", set_code.upper())

            q.limit(limit)
            sql, params = q.build()

            if as_dataframe:
                return self._conn.execute_df(sql, params)

            rows = self._conn.execute(sql, params)
        except Exception:
            # sealedProduct column may not exist in flat sets.parquet
            return []

        products: list[dict] = []
        for row in rows:
            sealed = row.get("sealedProduct")
            if sealed and isinstance(sealed, list):
                for sp in sealed:
                    if isinstance(sp, dict):
                        if category and sp.get("category") != category:
                            continue
                        sp["setCode"] = row.get("code")
                        products.append(sp)

        if as_dict:
            return products
        return products

    def get(self, uuid: str) -> dict | None:
        """Get a sealed product by UUID.

        Uses DuckDB UNNEST + struct field filtering for efficient
        server-side lookup instead of scanning all sets in Python.

        Args:
            uuid: The sealed product UUID.

        Returns:
            Product dict with a ``setCode`` key added, or None if not found.
        """
        self._ensure()
        try:
            sql = (
                "SELECT sub.code AS setCode, sub.sp "
                "FROM ("
                "  SELECT code, UNNEST(sealedProduct) AS sp "
                "  FROM sets WHERE sealedProduct IS NOT NULL"
                ") sub "
                "WHERE sub.sp.uuid = $1 "
                "LIMIT 1"
            )
            rows = self._conn.execute(sql, [uuid])
        except Exception:
            return None
        if not rows:
            return None
        row = rows[0]
        product = row.get("sp", {})
        if isinstance(product, dict):
            product["setCode"] = row.get("setCode")
            return product
        return None
