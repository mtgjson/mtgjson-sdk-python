"""Format legality query module."""

from __future__ import annotations

from typing import Any

from pydantic import TypeAdapter

from ..connection import Connection
from ..models.cards import CardSet

_CARD_SET_LIST = TypeAdapter(list[CardSet])


class LegalityQuery:
    """Query interface for card format legalities.

    Uses the ``card_legalities`` view, which UNPIVOTs wide-format legality
    columns into ``(uuid, format, status)`` rows.

    Example::

        legal = sdk.legalities.is_legal("uuid-here", "modern")
        banned = sdk.legalities.banned_in("modern")
        formats = sdk.legalities.formats_for_card("uuid-here")
    """

    def __init__(self, conn: Connection) -> None:
        self._conn = conn

    def _ensure(self) -> None:
        self._conn.ensure_views("card_legalities")

    def _cards_by_status(
        self,
        format_name: str,
        status: str,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """Get cards with a specific legality status in a format.

        Args:
            format_name: Format name (e.g. ``"modern"``).
            status: Legality status (e.g. ``"Banned"``, ``"Restricted"``).
            limit: Maximum results.
            offset: Result offset for pagination.

        Returns:
            List of dicts with ``name`` and ``uuid``.
        """
        self._ensure()
        self._conn.ensure_views("cards")
        return self._conn.execute(
            "SELECT c.name, c.uuid FROM cards c "
            "JOIN card_legalities cl ON c.uuid = cl.uuid "
            "WHERE cl.format = $1 AND cl.status = $2 "
            "ORDER BY c.name ASC "
            f"LIMIT {limit} OFFSET {offset}",
            [format_name, status],
        )

    def formats_for_card(self, uuid: str) -> dict[str, str]:
        """Get all format legalities for a card UUID.

        Args:
            uuid: The MTGJSON UUID of the card.

        Returns:
            Dict mapping format name to status
            (e.g. ``{"modern": "Legal", "standard": "Not Legal"}``).
        """
        self._ensure()
        rows = self._conn.execute(
            "SELECT format, status FROM card_legalities WHERE uuid = $1", [uuid]
        )
        return {r["format"]: r["status"] for r in rows}

    def legal_in(
        self,
        format_name: str,
        *,
        limit: int = 100,
        offset: int = 0,
        as_dict: bool = False,
        as_dataframe: bool = False,
    ) -> list[CardSet] | list[dict] | Any:
        """Get all cards legal in a specific format.

        Args:
            format_name: Format name (e.g. ``"modern"``, ``"standard"``).
            limit: Maximum results (default 100).
            offset: Result offset for pagination.
            as_dict: Return raw dicts instead of models.
            as_dataframe: Return a Polars DataFrame.

        Returns:
            List of CardSet models, dicts, or a DataFrame.
        """
        self._conn.ensure_views("cards", "card_legalities")
        sql = (
            "SELECT DISTINCT c.* FROM cards c "
            "JOIN card_legalities cl ON c.uuid = cl.uuid "
            "WHERE cl.format = $1 AND cl.status = 'Legal' "
            "ORDER BY c.name ASC "
            f"LIMIT {limit} OFFSET {offset}"
        )
        if as_dataframe:
            return self._conn.execute_df(sql, [format_name])
        if as_dict:
            return self._conn.execute(sql, [format_name])
        return self._conn.execute_models(sql, [format_name], adapter=_CARD_SET_LIST)

    def is_legal(self, uuid: str, format_name: str) -> bool:
        """Check if a specific card is legal in a format.

        Args:
            uuid: The MTGJSON UUID of the card.
            format_name: Format name (e.g. ``"modern"``).

        Returns:
            True if the card has ``"Legal"`` status in the format.
        """
        self._ensure()
        result = self._conn.execute_scalar(
            "SELECT COUNT(*) FROM card_legalities "
            "WHERE uuid = $1 AND format = $2 AND status = 'Legal'",
            [uuid, format_name],
        )
        return (result or 0) > 0

    def banned_in(self, format_name: str, **kwargs: Any) -> list[dict]:
        """Get all cards banned in a specific format.

        Args:
            format_name: Format name (e.g. ``"modern"``).
            **kwargs: Forwarded to query (``limit``, ``offset``).

        Returns:
            List of dicts with ``name`` and ``uuid``.
        """
        return self._cards_by_status(format_name, "Banned", **kwargs)

    def restricted_in(self, format_name: str, **kwargs: Any) -> list[dict]:
        """Get all cards restricted in a specific format.

        Args:
            format_name: Format name (e.g. ``"vintage"``).
            **kwargs: Forwarded to query (``limit``, ``offset``).

        Returns:
            List of dicts with ``name`` and ``uuid``.
        """
        return self._cards_by_status(format_name, "Restricted", **kwargs)

    def suspended_in(self, format_name: str, **kwargs: Any) -> list[dict]:
        """Get all cards suspended in a specific format.

        Args:
            format_name: Format name (e.g. ``"historic"``).
            **kwargs: Forwarded to query (``limit``, ``offset``).

        Returns:
            List of dicts with ``name`` and ``uuid``.
        """
        return self._cards_by_status(format_name, "Suspended", **kwargs)

    def not_legal_in(self, format_name: str, **kwargs: Any) -> list[dict]:
        """Get all cards not legal in a specific format.

        Args:
            format_name: Format name (e.g. ``"standard"``).
            **kwargs: Forwarded to query (``limit``, ``offset``).

        Returns:
            List of dicts with ``name`` and ``uuid``.
        """
        return self._cards_by_status(format_name, "Not Legal", **kwargs)
