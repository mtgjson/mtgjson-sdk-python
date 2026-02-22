"""Async wrapper for MtgjsonSDK.

Runs all DuckDB queries in a thread pool executor, making it safe
to use from async frameworks (FastAPI, Django, etc.) without blocking
the event loop.  DuckDB releases the GIL during query execution,
so thread pool concurrency works well.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

from .client import MtgjsonSDK

T = TypeVar("T")


class AsyncMtgjsonSDK:
    """Async wrapper around :class:`MtgjsonSDK`.

    Usage::

        async with AsyncMtgjsonSDK() as sdk:
            cards = await sdk.run(sdk.inner.cards.search, name="Lightning%")
            result = await sdk.sql("SELECT COUNT(*) FROM cards")
    """

    def __init__(
        self,
        *,
        max_workers: int = 4,
        **kwargs: Any,
    ) -> None:
        """Initialize the async SDK.

        Args:
            max_workers: Thread pool size for concurrent queries.
            **kwargs: Forwarded to :class:`MtgjsonSDK` (cache_dir, offline, etc.).
        """
        self._sdk = MtgjsonSDK(**kwargs)
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    @property
    def inner(self) -> MtgjsonSDK:
        """Access the underlying sync SDK for property access."""
        return self._sdk

    async def run(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Run any sync SDK method asynchronously.

        Example::

            cards = await sdk.run(sdk.inner.cards.search, name="Lightning%")
            sets = await sdk.run(sdk.inner.sets.list, set_type="masters")
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, lambda: fn(*args, **kwargs))

    async def sql(
        self,
        query: str,
        params: list[Any] | None = None,
        **kwargs: Any,
    ) -> list[dict] | Any:
        """Execute raw SQL asynchronously.

        Args:
            query: SQL query string.
            params: Optional query parameters.
            **kwargs: Forwarded to :meth:`MtgjsonSDK.sql` (e.g. ``as_dataframe``).

        Returns:
            List of row dicts, or a Polars DataFrame if ``as_dataframe=True``.
        """
        return await self.run(self._sdk.sql, query, params, **kwargs)

    async def close(self) -> None:
        """Close the underlying SDK and shut down the thread pool executor."""
        self._sdk.close()
        self._executor.shutdown(wait=False)

    async def __aenter__(self) -> AsyncMtgjsonSDK:
        """Enter async context manager.

        Example::

            async with AsyncMtgjsonSDK() as sdk:
                cards = await sdk.run(sdk.inner.cards.search, name="Bolt%")
        """
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Exit async context manager and close all resources."""
        await self.close()
