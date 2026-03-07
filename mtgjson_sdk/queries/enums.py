"""Enum/keyword query module."""

from __future__ import annotations

from typing import Any

from ..cache import CacheManager
from ..models.submodels import CardTypes, Keywords


class EnumQuery:
    """Query interface for MTGJSON keywords, card types, and enum values.

    Data is loaded from JSON files on the CDN (not parquet).

    Example::

        kw = sdk.enums.keywords()          # {"abilityWords": [...], ...}
        types = sdk.enums.card_types()      # {"creature": {...}, ...}
        enums = sdk.enums.enum_values()     # {"colors": [...], ...}
    """

    def __init__(self, cache: CacheManager) -> None:
        self._cache = cache

    def keywords(self) -> Keywords:
        """Get all MTG keyword categories and their values.

        Returns:
            Dict mapping keyword category to list of keywords
            (e.g. ``{"abilityWords": ["Addendum", ...], "keywordActions": [...]}``).
        """
        raw = self._cache.load_json("keywords")
        return raw.get("data", {})

    def card_types(self) -> CardTypes:
        """Get all card types with their valid sub- and supertypes.

        Returns:
            Dict mapping card type to its valid subtypes and supertypes
            (e.g. ``{"creature": {"subTypes": [...], "superTypes": [...]}}``).
        """
        raw = self._cache.load_json("card_types")
        return raw.get("data", {})

    def enum_values(self) -> dict[str, Any]:
        """Get all enumerated values used by MTGJSON fields.

        Returns:
            Dict mapping field name to its valid values
            (e.g. ``{"colors": ["B", "G", "R", "U", "W"], ...}``).
        """
        raw = self._cache.load_json("enum_values")
        return raw.get("data", {})
