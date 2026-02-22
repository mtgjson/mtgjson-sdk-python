"""MtgjsonSDK main entry point."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .booster.simulator import BoosterSimulator
from .cache import CacheManager
from .connection import Connection
from .queries.cards import CardQuery
from .queries.decks import DeckQuery
from .queries.enums import EnumQuery
from .queries.identifiers import IdentifierQuery
from .queries.legalities import LegalityQuery
from .queries.prices import PriceQuery
from .queries.sealed import SealedQuery
from .queries.sets import SetQuery
from .queries.skus import SkuQuery
from .queries.tokens import TokenQuery


class MtgjsonSDK:
    """Query client for MTGJSON card data.

    Auto-downloads Parquet data from the MTGJSON CDN and provides a typed,
    queryable Python API for the full dataset.

    Usage::

        sdk = MtgjsonSDK()

        # Cards
        card = sdk.cards.get_by_uuid("abc-123")
        results = sdk.cards.search(name="Lightning%", legal_in="modern")

        # Sets
        mh3 = sdk.sets.get("MH3")

        # Prices
        prices = sdk.prices.today("uuid", provider="tcgplayer")

        # Raw SQL
        rows = sdk.sql("SELECT * FROM cards WHERE manaValue = 1 LIMIT 10")

        sdk.close()
    """

    def __init__(
        self,
        cache_dir: Path | str | None = None,
        *,
        offline: bool = False,
        timeout: float = 120.0,
        on_progress: Any | None = None,
    ) -> None:
        """Initialize the SDK.

        Args:
            cache_dir: Directory for cached data files. Defaults to platform cache dir.
            offline: If True, never download from CDN (use cached files only).
            timeout: HTTP request timeout in seconds.
            on_progress: Optional callback ``(filename, bytes_downloaded, total_bytes)``
                called during file downloads. Use with tqdm or a custom progress bar.
        """
        self._cache = CacheManager(
            cache_dir, offline=offline, timeout=timeout, on_progress=on_progress
        )
        self._conn = Connection(self._cache)

        # Query interfaces (lazy — views registered on first use)
        self._cards: CardQuery | None = None
        self._sets: SetQuery | None = None
        self._prices: PriceQuery | None = None
        self._decks: DeckQuery | None = None
        self._sealed: SealedQuery | None = None
        self._skus: SkuQuery | None = None
        self._identifiers: IdentifierQuery | None = None
        self._legalities: LegalityQuery | None = None
        self._tokens: TokenQuery | None = None
        self._enums: EnumQuery | None = None
        self._booster: BoosterSimulator | None = None

    @property
    def cards(self) -> CardQuery:
        """Search, filter, and retrieve MTG card data.

        Example::

            card = sdk.cards.get_by_uuid("abc-123")
            bolts = sdk.cards.search(name="Lightning%", legal_in="modern")
        """
        if self._cards is None:
            self._cards = CardQuery(self._conn)
        return self._cards

    @property
    def sets(self) -> SetQuery:
        """Search and retrieve MTG set metadata.

        Example::

            mh3 = sdk.sets.get("MH3")
            expansions = sdk.sets.list(set_type="expansion")
        """
        if self._sets is None:
            self._sets = SetQuery(self._conn)
        return self._sets

    @property
    def prices(self) -> PriceQuery:
        """Query card prices from TCGPlayer, CardMarket, CardKingdom, etc.

        Example::

            prices = sdk.prices.today("uuid-here", provider="tcgplayer")
            trend = sdk.prices.price_trend("uuid-here")
        """
        if self._prices is None:
            self._prices = PriceQuery(self._conn, self._cache)
        return self._prices

    @property
    def decks(self) -> DeckQuery:
        """Search and list preconstructed deck data.

        Example::

            decks = sdk.decks.list(set_code="MH3")
            results = sdk.decks.search(name="Commander")
        """
        if self._decks is None:
            self._decks = DeckQuery(self._cache)
        return self._decks

    @property
    def sealed(self) -> SealedQuery:
        """Query sealed product data (booster boxes, bundles, etc.).

        Example::

            products = sdk.sealed.list(set_code="MH3")
        """
        if self._sealed is None:
            self._sealed = SealedQuery(self._conn)
        return self._sealed

    @property
    def skus(self) -> SkuQuery:
        """Query TCGPlayer SKU data for card variants.

        Example::

            skus = sdk.skus.get("uuid-here")
            sku = sdk.skus.find_by_sku_id(12345)
        """
        if self._skus is None:
            self._skus = SkuQuery(self._conn, self._cache)
        return self._skus

    @property
    def identifiers(self) -> IdentifierQuery:
        """Cross-reference cards by external IDs (Scryfall, TCGPlayer, etc.).

        Example::

            cards = sdk.identifiers.find_by_scryfall_id("abc-123")
            ids = sdk.identifiers.get_identifiers("uuid-here")
        """
        if self._identifiers is None:
            self._identifiers = IdentifierQuery(self._conn)
        return self._identifiers

    @property
    def legalities(self) -> LegalityQuery:
        """Query card format legalities (Modern, Standard, etc.).

        Example::

            legal = sdk.legalities.is_legal("uuid-here", "modern")
            banned = sdk.legalities.banned_in("modern")
        """
        if self._legalities is None:
            self._legalities = LegalityQuery(self._conn)
        return self._legalities

    @property
    def tokens(self) -> TokenQuery:
        """Search and retrieve MTG token card data.

        Example::

            token = sdk.tokens.get_by_uuid("abc-123")
            soldiers = sdk.tokens.search(name="Soldier%")
        """
        if self._tokens is None:
            self._tokens = TokenQuery(self._conn)
        return self._tokens

    @property
    def enums(self) -> EnumQuery:
        """Access MTGJSON enumerated values, keywords, and card types.

        Example::

            kw = sdk.enums.keywords()
            types = sdk.enums.card_types()
        """
        if self._enums is None:
            self._enums = EnumQuery(self._cache)
        return self._enums

    @property
    def booster(self) -> BoosterSimulator:
        """Simulate opening booster packs using set configuration data.

        Example::

            pack = sdk.booster.open_pack("MH3", "draft")
            types = sdk.booster.available_types("MH3")
        """
        if self._booster is None:
            self._booster = BoosterSimulator(self._conn)
        return self._booster

    @property
    def meta(self) -> dict:
        """Get MTGJSON build metadata.

        Returns:
            Dict with ``version`` and ``date`` keys, or empty dict
            if metadata is not yet cached.
        """
        try:
            return self._cache.load_json("meta")
        except FileNotFoundError:
            return {}

    @property
    def views(self) -> list[str]:
        """List all currently registered DuckDB views/tables.

        Views are registered lazily as query properties are accessed.
        """
        return sorted(self._conn._registered_views)

    def sql(
        self,
        query: str,
        params: list[Any] | None = None,
        *,
        as_dataframe: bool = False,
    ) -> list[dict] | Any:
        """Execute raw SQL against the DuckDB database.

        Views are registered lazily, so make sure you've accessed the
        relevant query property first, or register views manually via
        ``sdk._conn.ensure_views("cards", "sets")``.

        Args:
            query: SQL query string.
            params: Optional query parameters.
            as_dataframe: Return a Polars DataFrame instead of dicts.

        Returns:
            List of row dicts, or a Polars DataFrame if *as_dataframe* is True.

        Example::

            rows = sdk.sql("SELECT name FROM cards WHERE manaValue = 1 LIMIT 5")
        """
        if as_dataframe:
            return self._conn.execute_df(query, params)
        return self._conn.execute(query, params)

    def refresh(self) -> bool:
        """Check for new MTGJSON data and reset internal state if stale.

        Compares the local cached version against Meta.json on the CDN.
        If a newer version is available, clears the DuckDB view registry
        and resets all lazy query objects so the next access re-downloads
        and re-registers fresh data.

        Returns True if data was stale (and state was reset), False if
        already up to date.  Safe to call in long-running processes
        (web servers, bots) to pick up new MTGJSON releases without
        restarting.
        """
        if not self._cache.is_stale():
            return False

        # Clear view registry — next access re-registers from fresh parquet
        self._conn._registered_views.clear()

        # Reset lazy query objects so they re-run _ensure() on next access
        self._cards = None
        self._sets = None
        self._prices = None
        self._decks = None
        self._sealed = None
        self._skus = None
        self._identifiers = None
        self._legalities = None
        self._tokens = None
        self._enums = None
        self._booster = None

        return True

    def export_db(self, path: Path | str) -> Path:
        """Export all loaded data to a persistent DuckDB file.

        Creates a standalone ``.duckdb`` file containing all registered
        views and tables.  The exported file can be queried directly with
        the DuckDB CLI, Python ``duckdb.connect()``, or any DuckDB client
        — no SDK required.

        Args:
            path: Output path for the ``.duckdb`` file.

        Returns:
            The resolved output path.
        """
        path = Path(path)
        if path.exists():
            path.unlink()
        path_str = str(path).replace("\\", "/")
        self._conn._conn.execute(f"ATTACH '{path_str}' AS export_db")
        try:
            for view_name in sorted(self._conn._registered_views):
                self._conn._conn.execute(
                    f"CREATE TABLE export_db.{view_name} AS SELECT * FROM {view_name}"
                )
        finally:
            self._conn._conn.execute("DETACH export_db")
        return path

    def close(self) -> None:
        """Close the DuckDB connection and HTTP client, freeing resources.

        Called automatically when using the SDK as a context manager.
        """
        self._conn.close()
        self._cache.close()

    def __enter__(self) -> MtgjsonSDK:
        """Enter context manager.

        Example::

            with MtgjsonSDK() as sdk:
                cards = sdk.cards.search(name="Lightning Bolt")
        """
        return self

    def __exit__(self, *args: Any) -> None:
        """Exit context manager and close all resources."""
        self.close()

    def __repr__(self) -> str:
        return f"MtgjsonSDK(cache_dir={self._cache.cache_dir!r})"
