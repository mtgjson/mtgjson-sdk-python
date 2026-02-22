"""CDN URLs, cache paths, and SDK defaults.

This module defines the MTGJSON API endpoints, file mappings, and
platform-specific cache directory logic used by the SDK.
"""

from __future__ import annotations

import platform
from pathlib import Path

#: Base URL for the MTGJSON v5 API / CDN.
CDN_BASE = "https://mtgjson.com/api/v5"

#: Mapping of logical view names to CDN parquet file paths.
PARQUET_FILES: dict[str, str] = {
    # Flat normalized tables
    "cards": "parquet/cards.parquet",
    "tokens": "parquet/tokens.parquet",
    "sets": "parquet/sets.parquet",
    "card_identifiers": "parquet/cardIdentifiers.parquet",
    "card_legalities": "parquet/cardLegalities.parquet",
    "card_foreign_data": "parquet/cardForeignData.parquet",
    "card_rulings": "parquet/cardRulings.parquet",
    "card_purchase_urls": "parquet/cardPurchaseUrls.parquet",
    "set_translations": "parquet/setTranslations.parquet",
    "token_identifiers": "parquet/tokenIdentifiers.parquet",
    # Booster tables
    "set_booster_content_weights": "parquet/setBoosterContentWeights.parquet",
    "set_booster_contents": "parquet/setBoosterContents.parquet",
    "set_booster_sheet_cards": "parquet/setBoosterSheetCards.parquet",
    "set_booster_sheets": "parquet/setBoosterSheets.parquet",
    # Full nested
    "all_printings": "parquet/AllPrintings.parquet",
}

#: Mapping of logical data names to CDN JSON file paths.
JSON_FILES: dict[str, str] = {
    "all_prices_today": "AllPricesToday.json.gz",
    "tcgplayer_skus": "TcgplayerSkus.json.gz",
    "keywords": "Keywords.json",
    "card_types": "CardTypes.json",
    "deck_list": "DeckList.json",
    "enum_values": "EnumValues.json",
    "meta": "Meta.json",
}

#: URL for the MTGJSON version metadata endpoint.
META_URL = f"{CDN_BASE}/Meta.json"


def default_cache_dir() -> Path:
    """Platform-appropriate cache directory.

    Returns:
        ``~/AppData/Local/mtgjson-sdk`` on Windows,
        ``~/Library/Caches/mtgjson-sdk`` on macOS,
        ``~/.cache/mtgjson-sdk`` on Linux.
    """
    system = platform.system()
    if system == "Windows":
        base = Path.home() / "AppData" / "Local"
    elif system == "Darwin":
        base = Path.home() / "Library" / "Caches"
    else:
        base = Path.home() / ".cache"
    return base / "mtgjson-sdk"
