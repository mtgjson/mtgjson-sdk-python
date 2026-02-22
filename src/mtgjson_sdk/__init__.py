"""MTGJSON SDK — DuckDB-backed query client for Magic: The Gathering card data."""

from .async_client import AsyncMtgjsonSDK
from .client import MtgjsonSDK

__all__ = ["AsyncMtgjsonSDK", "MtgjsonSDK"]
__version__ = "0.1.0"
