"""Card query module."""

from __future__ import annotations

from typing import Any

from pydantic import TypeAdapter

from .._sql import SQLBuilder
from ..connection import Connection
from ..models.cards import CardAtomic, CardSet

_CARD_SET_LIST = TypeAdapter(list[CardSet])
_CARD_ATOMIC_LIST = TypeAdapter(list[CardAtomic])


class CardQuery:
    """Query interface for MTG card data.

    Provides methods to search, filter, and retrieve cards from the
    MTGJSON dataset using DuckDB-backed queries.

    Example::

        cards = sdk.cards.search(name="Lightning%", legal_in="modern")
        card = sdk.cards.get_by_uuid("abc-123")
        count = sdk.cards.count(setCode="MH3")
    """

    def __init__(self, conn: Connection) -> None:
        self._conn = conn

    def _ensure(self) -> None:
        self._conn.ensure_views("cards")

    def get_by_uuid(
        self,
        uuid: str,
        *,
        as_dict: bool = False,
        as_dataframe: bool = False,
    ) -> CardSet | dict | Any | None:
        """Get a single card by its MTGJSON UUID.

        Args:
            uuid: The MTGJSON v5 UUID of the card.
            as_dict: Return a raw dict instead of a Pydantic model.
            as_dataframe: Return a Polars DataFrame.

        Returns:
            A CardSet model, dict, or DataFrame — or None if not found.
        """
        self._ensure()
        sql = "SELECT * FROM cards WHERE uuid = $1"
        if as_dataframe:
            return self._conn.execute_df(sql, [uuid])
        rows = self._conn.execute(sql, [uuid])
        if not rows:
            return None
        if as_dict:
            return rows[0]
        return CardSet.model_validate(rows[0])

    def get_by_uuids(
        self,
        uuids: list[str],
        *,
        as_dict: bool = False,
        as_dataframe: bool = False,
    ) -> list[CardSet] | list[dict] | Any:
        """Fetch multiple cards by UUID in a single query.

        Much faster than looping get_by_uuid() — DuckDB handles large
        IN clauses efficiently.  Useful for rendering decklists, batch
        lookups, and bulk operations.

        Args:
            uuids: List of card UUIDs to fetch.
            as_dict: Return raw dicts instead of models.
            as_dataframe: Return a Polars DataFrame.

        Returns:
            List of CardSet models, dicts, or a DataFrame.
        """
        if not uuids:
            return (
                []
                if not as_dataframe
                else self._conn.execute_df("SELECT * FROM cards WHERE FALSE")
            )
        self._ensure()
        q = SQLBuilder("cards").where_in("uuid", uuids)
        sql, params = q.build()
        if as_dataframe:
            return self._conn.execute_df(sql, params)
        if as_dict:
            return self._conn.execute(sql, params)
        return self._conn.execute_models(sql, params, adapter=_CARD_SET_LIST)

    def get_by_name(
        self,
        name: str,
        *,
        set_code: str | None = None,
        as_dict: bool = False,
        as_dataframe: bool = False,
    ) -> list[CardSet] | list[dict] | Any:
        """Get all printings of a card by exact name.

        Args:
            name: Exact card name (e.g. ``"Lightning Bolt"``).
            set_code: Optional set code to narrow results.
            as_dict: Return raw dicts instead of models.
            as_dataframe: Return a Polars DataFrame.

        Returns:
            List of matching cards across all printings.
        """
        self._ensure()
        q = SQLBuilder("cards").where_eq("name", name)
        if set_code:
            q.where_eq("setCode", set_code)
        q.order_by("setCode DESC", "number ASC")
        sql, params = q.build()
        if as_dataframe:
            return self._conn.execute_df(sql, params)
        if as_dict:
            return self._conn.execute(sql, params)
        return self._conn.execute_models(sql, params, adapter=_CARD_SET_LIST)

    def search(
        self,
        *,
        name: str | None = None,
        fuzzy_name: str | None = None,
        localized_name: str | None = None,
        set_code: str | None = None,
        colors: list[str] | None = None,
        color_identity: list[str] | None = None,
        types: str | None = None,
        rarity: str | None = None,
        legal_in: str | None = None,
        mana_value: float | None = None,
        mana_value_lte: float | None = None,
        mana_value_gte: float | None = None,
        text: str | None = None,
        text_regex: str | None = None,
        power: str | None = None,
        toughness: str | None = None,
        artist: str | None = None,
        keyword: str | None = None,
        is_promo: bool | None = None,
        availability: str | None = None,
        language: str | None = None,
        layout: str | None = None,
        set_type: str | None = None,
        limit: int = 100,
        offset: int = 0,
        as_dict: bool = False,
        as_dataframe: bool = False,
    ) -> list[CardSet] | list[dict] | Any:
        """Search cards with flexible filters.

        All filter parameters are optional and combined with AND logic.

        Args:
            name: Card name pattern (supports ``%`` wildcard for LIKE matching).
            fuzzy_name: Typo-tolerant name search using Jaro-Winkler similarity.
                Results are ordered by similarity descending. Example:
                ``search(fuzzy_name="Ligtning Bolt")`` finds "Lightning Bolt".
            localized_name: Search by foreign-language name (e.g. "Blitzschlag"
                for Lightning Bolt in German). Joins card_foreign_data.
                Supports ``%`` wildcards for LIKE matching.
            set_code: Filter by set code.
            colors: Filter by colors (cards containing all specified colors).
            color_identity: Filter by color identity (cards containing all specified).
            types: Filter by type line (LIKE pattern).
            rarity: Filter by rarity.
            legal_in: Filter by format legality (requires card_legalities view).
            mana_value: Exact mana value.
            mana_value_lte: Mana value less than or equal to.
            mana_value_gte: Mana value greater than or equal to.
            text: Text search in rules text (LIKE pattern).
            text_regex: Regex search in rules text (DuckDB regexp_matches).
            power: Exact power value.
            toughness: Exact toughness value.
            artist: Filter by artist name (LIKE pattern).
            keyword: Filter by keyword ability (e.g. "Flying", "Trample").
            is_promo: Filter by promo status.
            availability: Filter by availability (e.g. "paper", "mtgo").
            language: Filter by language (e.g. "English").
            layout: Filter by card layout (e.g. "normal", "split", "adventure").
            set_type: Filter by set type (e.g. "expansion", "masters").
                Requires sets view.
            limit: Maximum results (default 100).
            offset: Result offset for pagination.
            as_dict: Return raw dicts instead of models.
            as_dataframe: Return a Polars DataFrame.

        Returns:
            List of matching CardSet models, dicts, or a DataFrame.

        Example::

            # Red one-drops legal in Modern
            sdk.cards.search(
                colors=["R"], mana_value=1, legal_in="modern", limit=20
            )
        """
        self._ensure()
        q = SQLBuilder("cards")

        if name:
            if "%" in name:
                q.where_like("name", name)
            else:
                q.where_eq("name", name)

        if fuzzy_name:
            q.where_fuzzy("cards.name", fuzzy_name, threshold=0.8)

        if set_code:
            q.where_eq("setCode", set_code)

        if rarity:
            q.where_eq("rarity", rarity)

        if mana_value is not None:
            q.where_eq("manaValue", mana_value)

        if mana_value_lte is not None:
            q.where_lte("manaValue", mana_value_lte)

        if mana_value_gte is not None:
            q.where_gte("manaValue", mana_value_gte)

        if text:
            q.where_like("text", f"%{text}%")

        if text_regex:
            q.where_regex("text", text_regex)

        if types:
            q.where_like("type", f"%{types}%")

        if power:
            q.where_eq("power", power)

        if toughness:
            q.where_eq("toughness", toughness)

        if artist:
            q.where_like("artist", f"%{artist}%")

        if language:
            q.where_eq("language", language)

        if layout:
            q.where_eq("layout", layout)

        if is_promo is not None:
            q.where_eq("isPromo", is_promo)

        if colors:
            for color in colors:
                idx = len(q._params) + 1
                q._where.append(f"list_contains(colors, ${idx})")
                q._params.append(color)

        if color_identity:
            for color in color_identity:
                idx = len(q._params) + 1
                q._where.append(f"list_contains(colorIdentity, ${idx})")
                q._params.append(color)

        if keyword:
            idx = len(q._params) + 1
            q._where.append(f"list_contains(keywords, ${idx})")
            q._params.append(keyword)

        if availability:
            idx = len(q._params) + 1
            q._where.append(f"list_contains(availability, ${idx})")
            q._params.append(availability)

        if localized_name:
            self._conn.ensure_views("card_foreign_data")
            q.select("cards.*")
            q.join("JOIN card_foreign_data cfd ON cards.uuid = cfd.uuid")
            if "%" in localized_name:
                q.where_like("cfd.name", localized_name)
            else:
                q.where_eq("cfd.name", localized_name)

        if legal_in:
            self._conn.ensure_views("card_legalities")
            q.join("JOIN card_legalities cl ON cards.uuid = cl.uuid")
            q.where_eq("cl.format", legal_in)
            q.where_eq("cl.status", "Legal")

        if set_type:
            self._conn.ensure_views("sets")
            q.select("cards.*")
            q.join("JOIN sets s ON cards.setCode = s.code")
            q.where_eq("s.type", set_type)

        if fuzzy_name:
            # Sort by similarity descending — add value again for ORDER BY
            sim_idx = len(q._params) + 1
            q._params.append(fuzzy_name)
            q.order_by(
                f"jaro_winkler_similarity(cards.name, ${sim_idx}) DESC",
                "cards.number ASC",
            )
        else:
            q.order_by("cards.name ASC", "cards.number ASC")
        q.limit(limit).offset(offset)

        sql, params = q.build()
        if as_dataframe:
            return self._conn.execute_df(sql, params)
        if as_dict:
            return self._conn.execute(sql, params)
        return self._conn.execute_models(sql, params, adapter=_CARD_SET_LIST)

    def get_printings(
        self,
        name: str,
        *,
        as_dict: bool = False,
        as_dataframe: bool = False,
    ) -> list[CardSet] | list[dict] | Any:
        """Get all printings of a card across all sets.

        Convenience alias for :meth:`get_by_name`.

        Args:
            name: Exact card name.
            as_dict: Return raw dicts instead of models.
            as_dataframe: Return a Polars DataFrame.

        Returns:
            List of all printings of the named card.
        """
        return self.get_by_name(name, as_dict=as_dict, as_dataframe=as_dataframe)

    def get_atomic(
        self,
        name: str,
        *,
        as_dict: bool = False,
    ) -> list[CardAtomic] | list[dict]:
        """Get atomic (oracle) card data by name.

        Returns de-duplicated card data without printing-specific fields.
        Also searches by faceName for split/adventure/MDFC cards.

        Args:
            name: Exact card name or face name.
            as_dict: Return raw dicts instead of models.

        Returns:
            List of unique atomic card entries (one per face).
        """
        self._ensure()
        atomic_cols = (
            "name",
            "asciiName",
            "faceName",
            "type",
            "types",
            "subtypes",
            "supertypes",
            "colors",
            "colorIdentity",
            "colorIndicator",
            "producedMana",
            "manaCost",
            "text",
            "layout",
            "side",
            "power",
            "toughness",
            "loyalty",
            "keywords",
            "isFunny",
            "edhrecSaltiness",
            "subsets",
            "manaValue",
            "faceConvertedManaCost",
            "faceManaValue",
            "defense",
            "hand",
            "life",
            "edhrecRank",
            "hasAlternativeDeckLimit",
            "isReserved",
            "isGameChanger",
            "printings",
            "leadershipSkills",
            "relatedCards",
        )
        q = SQLBuilder("cards")
        q.select(*atomic_cols)
        q.where_eq("name", name)
        q.order_by(
            "isFunny ASC NULLS FIRST",
            "isOnlineOnly ASC NULLS FIRST",
            "side ASC NULLS FIRST",
        )
        sql, params = q.build()
        rows = self._conn.execute(sql, params)

        # Fallback: search by faceName for split/adventure/MDFC cards
        if not rows:
            q2 = SQLBuilder("cards")
            q2.select(*atomic_cols)
            q2.where("CAST(faceName AS VARCHAR) = $1", name)
            q2.order_by(
                "isFunny ASC NULLS FIRST",
                "isOnlineOnly ASC NULLS FIRST",
                "side ASC NULLS FIRST",
            )
            sql2, params2 = q2.build()
            rows = self._conn.execute(sql2, params2)

        if not rows:
            return []
        # De-duplicate by name+faceName
        seen: set[tuple[str, str | None]] = set()
        unique: list[dict] = []
        for r in rows:
            key = (r.get("name", ""), r.get("faceName"))
            if key not in seen:
                seen.add(key)
                unique.append(r)
        if as_dict:
            return unique
        return [CardAtomic.model_validate(r) for r in unique]

    def find_by_scryfall_id(
        self,
        scryfall_id: str,
        *,
        as_dict: bool = False,
        as_dataframe: bool = False,
    ) -> list[CardSet] | list[dict] | Any:
        """Find cards by their Scryfall ID.

        Joins ``card_identifiers`` to cross-reference the Scryfall ID.

        Args:
            scryfall_id: The Scryfall UUID to look up.
            as_dict: Return raw dicts instead of models.
            as_dataframe: Return a Polars DataFrame.

        Returns:
            List of matching cards (usually one).
        """
        self._conn.ensure_views("cards", "card_identifiers")
        sql = (
            "SELECT c.* FROM cards c "
            "JOIN card_identifiers ci ON c.uuid = ci.uuid "
            "WHERE ci.scryfallId = $1"
        )
        if as_dataframe:
            return self._conn.execute_df(sql, [scryfall_id])
        if as_dict:
            return self._conn.execute(sql, [scryfall_id])
        return self._conn.execute_models(sql, [scryfall_id], adapter=_CARD_SET_LIST)

    def random(
        self,
        count: int = 1,
        *,
        as_dict: bool = False,
        as_dataframe: bool = False,
    ) -> list[CardSet] | list[dict] | Any:
        """Get random cards from the dataset.

        Args:
            count: Number of random cards to return (default 1).
            as_dict: Return raw dicts instead of models.
            as_dataframe: Return a Polars DataFrame.

        Returns:
            List of randomly sampled cards.
        """
        self._ensure()
        sql = f"SELECT * FROM cards USING SAMPLE {count}"
        if as_dataframe:
            return self._conn.execute_df(sql)
        if as_dict:
            return self._conn.execute(sql)
        return self._conn.execute_models(sql, adapter=_CARD_SET_LIST)

    def count(self, **filters: Any) -> int:
        """Count cards matching optional column filters.

        Args:
            **filters: Column name/value pairs (e.g. ``setCode="MH3"``).

        Returns:
            Number of matching cards.

        Example::

            total = sdk.cards.count()
            mh3_rares = sdk.cards.count(setCode="MH3", rarity="rare")
        """
        self._ensure()
        if not filters:
            return self._conn.execute_scalar("SELECT COUNT(*) FROM cards") or 0
        q = SQLBuilder("cards").select("COUNT(*)")
        for col, val in filters.items():
            q.where_eq(col, val)
        sql, params = q.build()
        return self._conn.execute_scalar(sql, params) or 0
