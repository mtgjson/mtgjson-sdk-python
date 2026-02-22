"""Deck query module."""

from __future__ import annotations

from ..cache import CacheManager
from ..models.decks import DeckList


class DeckQuery:
    """Query interface for preconstructed deck data.

    Decks are loaded from ``DeckList.json`` on the CDN (not parquet).

    Example::

        decks = sdk.decks.list(set_code="MH3")
        results = sdk.decks.search(name="Commander")
        total = sdk.decks.count()
    """

    def __init__(self, cache: CacheManager) -> None:
        self._cache = cache
        self._data: list[dict] | None = None

    def _ensure(self) -> None:
        if self._data is not None:
            return
        try:
            raw = self._cache.load_json("deck_list")
            self._data = raw.get("data", [])
        except FileNotFoundError:
            self._data = []

    def list(
        self,
        *,
        set_code: str | None = None,
        deck_type: str | None = None,
        as_dict: bool = False,
    ) -> list[DeckList] | list[dict]:
        """List available decks with optional filters.

        Args:
            set_code: Filter by set code (e.g. ``"MH3"``).
            deck_type: Filter by deck type (e.g. ``"Commander Deck"``).
            as_dict: Return raw dicts instead of models.

        Returns:
            List of DeckList models or dicts.
        """
        self._ensure()
        assert self._data is not None
        results = self._data

        if set_code:
            code_upper = set_code.upper()
            results = [d for d in results if d.get("code", "").upper() == code_upper]

        if deck_type:
            results = [d for d in results if d.get("type") == deck_type]

        if as_dict:
            return results
        return [DeckList.model_validate(d) for d in results]

    def search(
        self,
        *,
        name: str | None = None,
        set_code: str | None = None,
        as_dict: bool = False,
    ) -> list[DeckList] | list[dict]:
        """Search decks by name substring.

        Args:
            name: Case-insensitive substring to match against deck names.
            set_code: Filter by set code.
            as_dict: Return raw dicts instead of models.

        Returns:
            List of matching DeckList models or dicts.
        """
        self._ensure()
        assert self._data is not None
        results = self._data

        if name:
            name_lower = name.lower()
            results = [d for d in results if name_lower in d.get("name", "").lower()]

        if set_code:
            code_upper = set_code.upper()
            results = [d for d in results if d.get("code", "").upper() == code_upper]

        if as_dict:
            return results
        return [DeckList.model_validate(d) for d in results]

    def count(self) -> int:
        """Count total number of available decks.

        Returns:
            Total deck count.
        """
        self._ensure()
        assert self._data is not None
        return len(self._data)
