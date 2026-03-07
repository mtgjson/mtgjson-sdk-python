"""Weighted random booster pack simulation."""

from __future__ import annotations

import random
from typing import Any

from ..connection import Connection
from ..models.cards import CardSet
from ..models.submodels import BoosterConfig, BoosterPack, BoosterSheet

# Views needed for flat booster tables (available from CDN)
_BOOSTER_VIEWS = (
    "set_booster_content_weights",
    "set_booster_contents",
    "set_booster_sheets",
    "set_booster_sheet_cards",
)


class BoosterSimulator:
    """Simulates opening booster packs using set booster configuration data.

    Uses weighted random selection based on official MTGJSON booster
    configuration.  Loads data from flat booster parquet files (CDN) or
    falls back to the nested ``booster`` column in AllPrintings.

    Example::

        types = sdk.booster.available_types("MH3")  # ["draft", "collector"]
        pack = sdk.booster.open_pack("MH3", "draft")
        box = sdk.booster.open_box("MH3", "draft", packs=36)
    """

    def __init__(self, conn: Connection) -> None:
        self._conn = conn
        self._config_cache: dict[str, dict[str, BoosterConfig] | None] = {}
        # Card data cache: (set_code, booster_type) -> {uuid: row_dict}
        self._card_cache: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}

    def _ensure_booster_views(self) -> None:
        self._conn.ensure_views(*_BOOSTER_VIEWS)

    def _has_flat_views(self) -> bool:
        return all(v in self._conn._registered_views for v in _BOOSTER_VIEWS)

    def _get_booster_config(self, set_code: str) -> dict[str, BoosterConfig] | None:
        """Get booster configuration for a set.

        Tries flat booster tables first, then falls
        back to the nested booster column.
        Results are cached per set code.
        """
        code = set_code.upper()
        if code in self._config_cache:
            return self._config_cache[code]

        config = self._get_config_from_flat(code)
        if not config:
            config = self._get_config_from_nested(code)

        self._config_cache[code] = config
        return config

    def _get_config_from_flat(self, set_code: str) -> dict[str, BoosterConfig] | None:
        """Build booster config from the flat normalized parquet tables."""
        self._ensure_booster_views()
        if not self._has_flat_views():
            return None

        weights = self._conn.execute(
            "SELECT boosterName, boosterIndex, boosterWeight "
            "FROM set_booster_content_weights WHERE setCode = $1",
            [set_code],
        )
        if not weights:
            return None

        contents = self._conn.execute(
            "SELECT boosterName, boosterIndex, sheetName, sheetPicks "
            "FROM set_booster_contents WHERE setCode = $1",
            [set_code],
        )
        sheets_meta = self._conn.execute(
            "SELECT boosterName, sheetName, sheetIsFoil, "
            "sheetHasBalanceColors, sheetTotalWeight "
            "FROM set_booster_sheets WHERE setCode = $1",
            [set_code],
        )
        sheet_cards = self._conn.execute(
            "SELECT boosterName, sheetName, cardUuid, cardWeight "
            "FROM set_booster_sheet_cards WHERE setCode = $1",
            [set_code],
        )

        # Reconstruct nested BoosterConfig from flat rows
        result: dict[str, BoosterConfig] = {}
        booster_names = {r["boosterName"] for r in weights}

        for bname in sorted(booster_names):
            # --- Pack templates ---
            bw = [r for r in weights if r["boosterName"] == bname]
            bc = [r for r in contents if r["boosterName"] == bname]

            # Group sheet picks by booster index
            idx_contents: dict[int, dict[str, int]] = {}
            for r in bc:
                idx_contents.setdefault(r["boosterIndex"], {})[r["sheetName"]] = (
                    r["sheetPicks"]
                )

            boosters: list[BoosterPack] = []
            total_weight = 0
            for r in bw:
                boosters.append(
                    {
                        "contents": idx_contents.get(r["boosterIndex"], {}),
                        "weight": r["boosterWeight"],
                    }
                )
                total_weight += r["boosterWeight"]

            # --- Sheets ---
            sm = [r for r in sheets_meta if r["boosterName"] == bname]
            sc = [r for r in sheet_cards if r["boosterName"] == bname]

            sheets: dict[str, BoosterSheet] = {}
            for r in sm:
                sname = r["sheetName"]
                cards = {
                    c["cardUuid"]: c["cardWeight"]
                    for c in sc
                    if c["sheetName"] == sname
                }
                sheet: BoosterSheet = {
                    "cards": cards,
                    "foil": r["sheetIsFoil"],
                    "totalWeight": r["sheetTotalWeight"],
                }
                if r["sheetHasBalanceColors"]:
                    sheet["balanceColors"] = True
                sheets[sname] = sheet

            result[bname] = {
                "boosters": boosters,
                "boostersTotalWeight": total_weight,
                "sheets": sheets,
                "sourceSetCodes": [set_code],
            }

        return result if result else None

    def _get_config_from_nested(
        self, set_code: str
    ) -> dict[str, BoosterConfig] | None:
        """Fall back to the nested booster column in AllPrintings / test data."""
        self._conn.ensure_views("sets")
        try:
            rows = self._conn.execute(
                "SELECT booster FROM sets WHERE code = $1", [set_code]
            )
        except Exception:
            return None
        if not rows or not rows[0].get("booster"):
            return None
        return rows[0]["booster"]

    def available_types(self, set_code: str) -> list[str]:
        """List available booster types for a set.

        Args:
            set_code: The set code (e.g. ``"MH3"``).

        Returns:
            List of booster type names (e.g. ``["draft", "collector"]``),
            or empty list if no booster data exists.
        """
        config = self._get_booster_config(set_code)
        if not config:
            return []
        return list(config.keys())

    def _ensure_card_cache(self, set_code: str, booster_type: str) -> None:
        """Pre-fetch all cards that can appear in a booster type.

        Collects every UUID across all sheets for the given booster type
        and fetches their card data in a single DuckDB query.  Subsequent
        ``open_pack`` calls resolve cards from this cache instead of
        issuing per-pack queries.
        """
        key = (set_code.upper(), booster_type)
        if key in self._card_cache:
            return

        config = self._get_booster_config(set_code)
        if not config or booster_type not in config:
            return

        all_uuids: set[str] = set()
        for sheet in config[booster_type]["sheets"].values():
            all_uuids.update(sheet["cards"].keys())

        if not all_uuids:
            self._card_cache[key] = {}
            return

        self._conn.ensure_views("cards")
        uuid_list = list(all_uuids)
        placeholders = ", ".join(f"${i + 1}" for i in range(len(uuid_list)))
        sql = f"SELECT * FROM cards WHERE uuid IN ({placeholders})"
        rows = self._conn.execute(sql, uuid_list)
        self._card_cache[key] = {r["uuid"]: r for r in rows}

    def open_pack(
        self,
        set_code: str,
        booster_type: str = "draft",
        *,
        as_dict: bool = False,
    ) -> list[CardSet] | list[dict]:
        """Simulate opening a single booster pack.

        Each card in the returned list includes ``isFoil`` (whether the
        card was pulled from a foil sheet) and ``boosterSheet`` (the
        sheet name it came from).

        On the first call for a given set/booster-type pair, all eligible
        cards are pre-fetched in a single query.  Subsequent packs
        resolve cards from the in-memory cache, making bulk simulation
        (e.g. 1000+ packs) fast.

        Args:
            set_code: The set code (e.g., "MH3").
            booster_type: Booster type (e.g., "draft", "collector").
            as_dict: Return raw dicts instead of models.

        Returns:
            List of cards in the pack.

        Raises:
            ValueError: If no booster config exists for the set/type.
        """
        configs = self._get_booster_config(set_code)
        if not configs or booster_type not in configs:
            raise ValueError(
                f"No booster config for set '{set_code}' type '{booster_type}'. "
                f"Available: {list(configs.keys()) if configs else []}"
            )

        # Pre-fetch card data on first call (single DuckDB query)
        self._ensure_card_cache(set_code, booster_type)
        card_data = self._card_cache.get((set_code.upper(), booster_type), {})

        config = configs[booster_type]
        pack_template = _pick_pack(config["boosters"])
        sheets = config["sheets"]

        # Track (uuid, sheet_name, is_foil) per pick so downstream
        # code can distinguish foil vs non-foil slots.
        picks: list[tuple[str, str, bool]] = []
        for sheet_name, count in pack_template["contents"].items():
            if sheet_name not in sheets:
                continue
            sheet = sheets[sheet_name]
            picked = _pick_from_sheet(sheet, count)
            is_foil = sheet.get("foil", False)
            picks.extend((uuid, sheet_name, is_foil) for uuid in picked)

        if not picks:
            return []

        # Resolve cards from cache and inject sheet metadata
        ordered = []
        for uuid, sheet_name, is_foil in picks:
            if uuid in card_data:
                card = dict(card_data[uuid])
                card["isFoil"] = is_foil
                card["boosterSheet"] = sheet_name
                ordered.append(card)

        if as_dict:
            return ordered
        return [CardSet.model_validate(r) for r in ordered]

    def open_box(
        self,
        set_code: str,
        booster_type: str = "draft",
        packs: int = 36,
        *,
        as_dict: bool = False,
    ) -> list[list[CardSet]] | list[list[dict]]:
        """Simulate opening a booster box.

        Args:
            set_code: The set code.
            booster_type: Booster type.
            packs: Number of packs in the box (default 36).
            as_dict: Return raw dicts instead of models.

        Returns:
            List of packs, each containing a list of cards.
        """
        return [
            self.open_pack(set_code, booster_type, as_dict=as_dict)
            for _ in range(packs)
        ]

    def sheet_contents(
        self,
        set_code: str,
        booster_type: str,
        sheet_name: str,
    ) -> dict[str, int] | None:
        """Get the card UUIDs and weights for a specific booster sheet.

        Args:
            set_code: The set code (e.g. ``"MH3"``).
            booster_type: Booster type (e.g. ``"draft"``).
            sheet_name: Sheet name (e.g. ``"common"``, ``"rare"``).

        Returns:
            Dict mapping card UUID to weight, or None if not found.
        """
        configs = self._get_booster_config(set_code)
        if not configs or booster_type not in configs:
            return None
        sheets = configs[booster_type].get("sheets", {})
        sheet = sheets.get(sheet_name)
        if not sheet:
            return None
        return sheet.get("cards")


def _pick_pack(boosters: list[BoosterPack]) -> BoosterPack:
    """Weighted random pick of a pack template."""
    weights = [b["weight"] for b in boosters]
    return random.choices(boosters, weights=weights, k=1)[0]


def _pick_from_sheet(sheet: BoosterSheet, count: int) -> list[str]:
    """Weighted random pick of cards from a sheet."""
    cards = sheet["cards"]
    uuids = list(cards.keys())
    weights = list(cards.values())
    allow_duplicates = sheet.get("allowDuplicates", False)

    if allow_duplicates:
        return random.choices(uuids, weights=weights, k=count)

    if count >= len(uuids):
        # Need all cards, just shuffle them
        result = list(uuids)
        random.shuffle(result)
        return result

    # Pick without replacement using weighted sampling
    picked: list[str] = []
    remaining_uuids = list(uuids)
    remaining_weights = list(weights)

    for _ in range(min(count, len(remaining_uuids))):
        choice = random.choices(remaining_uuids, weights=remaining_weights, k=1)[0]
        picked.append(choice)
        idx = remaining_uuids.index(choice)
        remaining_uuids.pop(idx)
        remaining_weights.pop(idx)

    return picked
