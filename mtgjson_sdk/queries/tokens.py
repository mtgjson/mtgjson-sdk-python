"""Token query module."""

from __future__ import annotations

from typing import Any

from pydantic import TypeAdapter

from .._sql import SQLBuilder
from ..connection import Connection
from ..models.cards import CardToken

_CARD_TOKEN_LIST = TypeAdapter(list[CardToken])


class TokenQuery:
    """Query interface for MTG token card data.

    Example::

        token = sdk.tokens.get_by_uuid("abc-123")
        soldiers = sdk.tokens.search(name="Soldier%")
        mh3_tokens = sdk.tokens.for_set("MH3")
    """

    def __init__(self, conn: Connection) -> None:
        self._conn = conn

    def _ensure(self) -> None:
        self._conn.ensure_views("tokens")

    def get_by_uuid(
        self,
        uuid: str,
        *,
        as_dict: bool = False,
        as_dataframe: bool = False,
    ) -> CardToken | dict | Any | None:
        """Get a single token by its MTGJSON UUID.

        Args:
            uuid: The MTGJSON v5 UUID of the token.
            as_dict: Return a raw dict instead of a Pydantic model.
            as_dataframe: Return a Polars DataFrame.

        Returns:
            A CardToken model, dict, or DataFrame — or None if not found.
        """
        self._ensure()
        sql = "SELECT * FROM tokens WHERE uuid = $1"
        if as_dataframe:
            return self._conn.execute_df(sql, [uuid])
        rows = self._conn.execute(sql, [uuid])
        if not rows:
            return None
        if as_dict:
            return rows[0]
        return CardToken.model_validate(rows[0])

    def get_by_uuids(
        self,
        uuids: list[str],
        *,
        as_dict: bool = False,
        as_dataframe: bool = False,
    ) -> list[CardToken] | list[dict] | Any:
        """Fetch multiple tokens by UUID in a single query.

        Args:
            uuids: List of token UUIDs to fetch.
            as_dict: Return raw dicts instead of models.
            as_dataframe: Return a Polars DataFrame.

        Returns:
            List of CardToken models, dicts, or a DataFrame.
        """
        if not uuids:
            return (
                []
                if not as_dataframe
                else self._conn.execute_df("SELECT * FROM tokens WHERE FALSE")
            )
        self._ensure()
        q = SQLBuilder("tokens").where_in("uuid", uuids)
        sql, params = q.build()
        if as_dataframe:
            return self._conn.execute_df(sql, params)
        if as_dict:
            return self._conn.execute(sql, params)
        return self._conn.execute_models(sql, params, adapter=_CARD_TOKEN_LIST)

    def get_by_name(
        self,
        name: str,
        *,
        set_code: str | None = None,
        as_dict: bool = False,
        as_dataframe: bool = False,
    ) -> list[CardToken] | list[dict] | Any:
        """Get all tokens matching an exact name.

        Args:
            name: Exact token name (e.g. ``"Soldier"``).
            set_code: Optional set code to narrow results.
            as_dict: Return raw dicts instead of models.
            as_dataframe: Return a Polars DataFrame.

        Returns:
            List of matching tokens across all sets.
        """
        self._ensure()
        q = SQLBuilder("tokens").where_eq("name", name)
        if set_code:
            q.where_eq("setCode", set_code)
        q.order_by("setCode DESC", "number ASC")
        sql, params = q.build()
        if as_dataframe:
            return self._conn.execute_df(sql, params)
        if as_dict:
            return self._conn.execute(sql, params)
        return self._conn.execute_models(sql, params, adapter=_CARD_TOKEN_LIST)

    def search(
        self,
        *,
        name: str | None = None,
        set_code: str | None = None,
        colors: list[str] | None = None,
        types: str | None = None,
        artist: str | None = None,
        limit: int = 100,
        offset: int = 0,
        as_dict: bool = False,
        as_dataframe: bool = False,
    ) -> list[CardToken] | list[dict] | Any:
        """Search tokens with flexible filters.

        All filter parameters are optional and combined with AND logic.

        Args:
            name: Token name pattern (supports ``%`` wildcard for LIKE).
            set_code: Filter by set code.
            colors: Filter by colors (tokens containing all specified).
            types: Filter by type line (LIKE pattern).
            artist: Filter by artist name (LIKE pattern).
            limit: Maximum results (default 100).
            offset: Result offset for pagination.
            as_dict: Return raw dicts instead of models.
            as_dataframe: Return a Polars DataFrame.

        Returns:
            List of matching CardToken models, dicts, or a DataFrame.
        """
        self._ensure()
        q = SQLBuilder("tokens")

        if name:
            if "%" in name:
                q.where_like("name", name)
            else:
                q.where_eq("name", name)

        if set_code:
            q.where_eq("setCode", set_code)

        if types:
            q.where_like("type", f"%{types}%")

        if artist:
            q.where_like("artist", f"%{artist}%")

        if colors:
            for color in colors:
                idx = len(q._params) + 1
                q._where.append(f"list_contains(colors, ${idx})")
                q._params.append(color)

        q.order_by("name ASC", "number ASC")
        q.limit(limit).offset(offset)

        sql, params = q.build()
        if as_dataframe:
            return self._conn.execute_df(sql, params)
        if as_dict:
            return self._conn.execute(sql, params)
        return self._conn.execute_models(sql, params, adapter=_CARD_TOKEN_LIST)

    def for_set(
        self,
        set_code: str,
        *,
        as_dict: bool = False,
        as_dataframe: bool = False,
    ) -> list[CardToken] | list[dict] | Any:
        """Get all tokens for a specific set.

        Args:
            set_code: The set code (e.g. ``"MH3"``).
            as_dict: Return raw dicts instead of models.
            as_dataframe: Return a Polars DataFrame.

        Returns:
            List of tokens belonging to the specified set.
        """
        return self.search(
            set_code=set_code, limit=1000, as_dict=as_dict, as_dataframe=as_dataframe
        )

    def count(self, **filters: Any) -> int:
        """Count tokens matching optional column filters.

        Args:
            **filters: Column name/value pairs (e.g. ``setCode="MH3"``).

        Returns:
            Number of matching tokens.
        """
        self._ensure()
        if not filters:
            return self._conn.execute_scalar("SELECT COUNT(*) FROM tokens") or 0
        q = SQLBuilder("tokens").select("COUNT(*)")
        for col, val in filters.items():
            q.where_eq(col, val)
        sql, params = q.build()
        return self._conn.execute_scalar(sql, params) or 0
