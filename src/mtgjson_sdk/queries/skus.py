"""TCGPlayer SKU query module."""

from __future__ import annotations

from ..connection import Connection
from ..models.submodels import TcgplayerSkus


class SkuQuery:
    """Query interface for TCGPlayer SKU data.

    SKUs represent individual purchasable variants of a card (e.g.
    foil vs non-foil, 1st edition, etc.) on TCGPlayer.

    Example::

        skus = sdk.skus.get("uuid-here")
        sku = sdk.skus.find_by_sku_id(12345)
        product_skus = sdk.skus.find_by_product_id(67890)
    """

    def __init__(self, conn: Connection) -> None:
        self._conn = conn

    def _ensure(self) -> None:
        """Register the tcgplayer_skus parquet view if not already done."""
        self._conn.ensure_views("tcgplayer_skus")

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
