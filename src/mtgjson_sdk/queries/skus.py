"""TCGPlayer SKU query module."""

from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path

from ..cache import CacheManager
from ..connection import Connection
from ..models.submodels import TcgplayerSkus

logger = logging.getLogger("mtgjson_sdk")


class SkuQuery:
    """Query interface for TCGPlayer SKU data.

    SKUs represent individual purchasable variants of a card (e.g.
    foil vs non-foil, 1st edition, etc.) on TCGPlayer.

    Example::

        skus = sdk.skus.get("uuid-here")
        sku = sdk.skus.find_by_sku_id(12345)
        product_skus = sdk.skus.find_by_product_id(67890)
    """

    def __init__(self, conn: Connection, cache: CacheManager) -> None:
        self._conn = conn
        self._cache = cache
        self._loaded = False

    def _ensure(self) -> None:
        """Load SKU data into DuckDB if not already done.

        Uses streaming NDJSON to avoid holding the full flattened
        row list in Python memory.
        """
        if self._loaded:
            return
        if "tcgplayer_skus" in self._conn._registered_views:
            self._loaded = True
            return

        try:
            path = self._cache.ensure_json("tcgplayer_skus")
        except FileNotFoundError:
            logger.warning("SKU data not available")
            self._loaded = True
            return

        _load_skus_to_duckdb(path, self._conn)
        self._loaded = True

    def get(
        self,
        uuid: str,
        *,
        as_dict: bool = False,
    ) -> list[TcgplayerSkus] | list[dict]:
        """Get all TCGPlayer SKUs for a card UUID.

        Args:
            uuid: The MTGJSON UUID of the card.
            as_dict: Return raw dicts instead of typed dicts.

        Returns:
            List of SKU entries for the card.
        """
        self._ensure()
        rows = self._conn.execute(
            "SELECT * FROM tcgplayer_skus WHERE uuid = $1", [uuid]
        )
        if as_dict:
            return rows
        return [TcgplayerSkus(**r) for r in rows]  # type: ignore[misc]

    def find_by_sku_id(self, sku_id: int) -> dict | None:
        """Find a SKU by its TCGPlayer SKU ID.

        Args:
            sku_id: The TCGPlayer SKU identifier.

        Returns:
            SKU dict or None if not found.
        """
        self._ensure()
        rows = self._conn.execute(
            "SELECT * FROM tcgplayer_skus WHERE skuId = $1", [sku_id]
        )
        return rows[0] if rows else None

    def find_by_product_id(
        self,
        product_id: int,
        *,
        as_dict: bool = False,
    ) -> list[dict]:
        """Find all SKUs for a TCGPlayer product ID.

        Args:
            product_id: The TCGPlayer product identifier.
            as_dict: Unused (always returns dicts).

        Returns:
            List of SKU dicts for the product.
        """
        self._ensure()
        return self._conn.execute(
            "SELECT * FROM tcgplayer_skus WHERE productId = $1", [product_id]
        )


def _load_skus_to_duckdb(path: Path, conn: Connection) -> None:
    """Parse TcgplayerSkus JSON, stream-flatten to NDJSON, load into DuckDB.

    Same memory optimization as prices: streams rows to NDJSON temp file
    instead of accumulating in a Python list.
    """
    import gzip

    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as f:
            raw = json.load(f)
    else:
        raw = json.loads(path.read_text(encoding="utf-8"))

    data = raw.get("data", {})
    del raw

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".ndjson")
    try:
        count = 0
        with os.fdopen(tmp_fd, "w", encoding="utf-8", buffering=1024 * 1024) as ndjson:
            for uuid, skus in data.items():
                if not isinstance(skus, list):
                    continue
                for sku in skus:
                    if isinstance(sku, dict):
                        row = dict(sku)
                        row["uuid"] = uuid
                        ndjson.write(json.dumps(row, separators=(",", ":")))
                        ndjson.write("\n")
                        count += 1
        del data

        if count > 0:
            conn.register_table_from_ndjson("tcgplayer_skus", tmp_path)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
