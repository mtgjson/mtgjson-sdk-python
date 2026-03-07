"""Cross-reference query module for external identifiers."""

from __future__ import annotations

from typing import Any

from pydantic import TypeAdapter

from ..connection import Connection
from ..models.cards import CardSet

_CARD_SET_LIST = TypeAdapter(list[CardSet])

# All known identifier columns in the card_identifiers table
KNOWN_ID_COLUMNS = frozenset(
    {
        "cardKingdomEtchedId",
        "cardKingdomFoilId",
        "cardKingdomId",
        "cardsphereId",
        "cardsphereFoilId",
        "mcmId",
        "mcmMetaId",
        "mtgArenaId",
        "mtgoFoilId",
        "mtgoId",
        "multiverseId",
        "scryfallId",
        "scryfallIllustrationId",
        "scryfallOracleId",
        "tcgplayerEtchedProductId",
        "tcgplayerProductId",
    }
)


class IdentifierQuery:
    """Cross-reference cards by external identifiers (Scryfall, TCGPlayer, etc.).

    Joins the ``card_identifiers`` table with ``cards`` to look up cards
    by any external ID.

    Example::

        cards = sdk.identifiers.find_by_scryfall_id("abc-123")
        cards = sdk.identifiers.find_by_tcgplayer_id("12345")
        all_ids = sdk.identifiers.get_identifiers("uuid-here")
    """

    def __init__(self, conn: Connection) -> None:
        self._conn = conn

    def _ensure(self) -> None:
        self._conn.ensure_views("cards", "card_identifiers")

    def _find_by(
        self,
        id_column: str,
        value: str,
        *,
        as_dict: bool = False,
        as_dataframe: bool = False,
    ) -> list[CardSet] | list[dict] | Any:
        self._ensure()
        sql = (
            "SELECT c.* FROM cards c "
            "JOIN card_identifiers ci ON c.uuid = ci.uuid "
            f"WHERE ci.{id_column} = $1"
        )
        if as_dataframe:
            return self._conn.execute_df(sql, [value])
        if as_dict:
            return self._conn.execute(sql, [value])
        return self._conn.execute_models(sql, [value], adapter=_CARD_SET_LIST)

    def find_by(
        self,
        id_type: str,
        value: str,
        **kwargs: Any,
    ) -> list[CardSet] | list[dict] | Any:
        """Generic identifier lookup by column name.

        Args:
            id_type: Identifier column name
                (e.g. ``"scryfallId"``, ``"mtgArenaId"``).
            value: The identifier value to search for.
            **kwargs: Forwarded to the query (``as_dict``, ``as_dataframe``).

        Returns:
            List of matching cards.

        Raises:
            ValueError: If *id_type* is not a known identifier column.
        """
        if id_type not in KNOWN_ID_COLUMNS:
            raise ValueError(
                f"Unknown identifier type '{id_type}'. "
                f"Known types: {sorted(KNOWN_ID_COLUMNS)}"
            )
        return self._find_by(id_type, value, **kwargs)

    # === Named convenience methods ===

    def find_by_scryfall_id(
        self, scryfall_id: str, **kwargs: Any
    ) -> list[CardSet] | list[dict] | Any:
        """Find cards by Scryfall UUID."""
        return self._find_by("scryfallId", scryfall_id, **kwargs)

    def find_by_scryfall_oracle_id(
        self, oracle_id: str, **kwargs: Any
    ) -> list[CardSet] | list[dict] | Any:
        """Find cards by Scryfall Oracle ID (shared across printings)."""
        return self._find_by("scryfallOracleId", oracle_id, **kwargs)

    def find_by_scryfall_illustration_id(
        self, illustration_id: str, **kwargs: Any
    ) -> list[CardSet] | list[dict] | Any:
        """Find cards by Scryfall Illustration ID (shared across art reprints)."""
        return self._find_by("scryfallIllustrationId", illustration_id, **kwargs)

    def find_by_tcgplayer_id(
        self, tcgplayer_id: str, **kwargs: Any
    ) -> list[CardSet] | list[dict] | Any:
        """Find cards by TCGPlayer product ID."""
        return self._find_by("tcgplayerProductId", tcgplayer_id, **kwargs)

    def find_by_tcgplayer_etched_id(
        self, tcgplayer_etched_id: str, **kwargs: Any
    ) -> list[CardSet] | list[dict] | Any:
        """Find cards by TCGPlayer etched product ID."""
        return self._find_by("tcgplayerEtchedProductId", tcgplayer_etched_id, **kwargs)

    def find_by_mtgo_id(
        self, mtgo_id: str, **kwargs: Any
    ) -> list[CardSet] | list[dict] | Any:
        """Find cards by Magic: The Gathering Online ID."""
        return self._find_by("mtgoId", mtgo_id, **kwargs)

    def find_by_mtgo_foil_id(
        self, mtgo_foil_id: str, **kwargs: Any
    ) -> list[CardSet] | list[dict] | Any:
        """Find cards by MTGO foil ID."""
        return self._find_by("mtgoFoilId", mtgo_foil_id, **kwargs)

    def find_by_mtg_arena_id(
        self, arena_id: str, **kwargs: Any
    ) -> list[CardSet] | list[dict] | Any:
        """Find cards by MTG Arena ID."""
        return self._find_by("mtgArenaId", arena_id, **kwargs)

    def find_by_multiverse_id(
        self, multiverse_id: str, **kwargs: Any
    ) -> list[CardSet] | list[dict] | Any:
        """Find cards by Gatherer multiverse ID."""
        return self._find_by("multiverseId", multiverse_id, **kwargs)

    def find_by_mcm_id(
        self, mcm_id: str, **kwargs: Any
    ) -> list[CardSet] | list[dict] | Any:
        """Find cards by Cardmarket (MCM) product ID."""
        return self._find_by("mcmId", mcm_id, **kwargs)

    def find_by_mcm_meta_id(
        self, mcm_meta_id: str, **kwargs: Any
    ) -> list[CardSet] | list[dict] | Any:
        """Find cards by Cardmarket (MCM) meta ID."""
        return self._find_by("mcmMetaId", mcm_meta_id, **kwargs)

    def find_by_card_kingdom_id(
        self, ck_id: str, **kwargs: Any
    ) -> list[CardSet] | list[dict] | Any:
        """Find cards by Card Kingdom product ID."""
        return self._find_by("cardKingdomId", ck_id, **kwargs)

    def find_by_card_kingdom_foil_id(
        self, ck_foil_id: str, **kwargs: Any
    ) -> list[CardSet] | list[dict] | Any:
        """Find cards by Card Kingdom foil product ID."""
        return self._find_by("cardKingdomFoilId", ck_foil_id, **kwargs)

    def find_by_card_kingdom_etched_id(
        self, ck_etched_id: str, **kwargs: Any
    ) -> list[CardSet] | list[dict] | Any:
        """Find cards by Card Kingdom etched product ID."""
        return self._find_by("cardKingdomEtchedId", ck_etched_id, **kwargs)

    def find_by_cardsphere_id(
        self, cs_id: str, **kwargs: Any
    ) -> list[CardSet] | list[dict] | Any:
        """Find cards by Cardsphere ID."""
        return self._find_by("cardsphereId", cs_id, **kwargs)

    def find_by_cardsphere_foil_id(
        self, cs_foil_id: str, **kwargs: Any
    ) -> list[CardSet] | list[dict] | Any:
        """Find cards by Cardsphere foil ID."""
        return self._find_by("cardsphereFoilId", cs_foil_id, **kwargs)

    def get_identifiers(self, uuid: str, *, as_dict: bool = False) -> dict | None:
        """Get all external identifiers for a card UUID.

        Args:
            uuid: The MTGJSON UUID of the card.
            as_dict: Unused (always returns a dict).

        Returns:
            Dict of all identifier columns for the card, or None if not found.
        """
        self._conn.ensure_views("card_identifiers")
        rows = self._conn.execute(
            "SELECT * FROM card_identifiers WHERE uuid = $1", [uuid]
        )
        if not rows:
            return None
        return rows[0]
