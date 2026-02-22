"""Weighted random booster pack simulation."""

from __future__ import annotations

import random

from ..connection import Connection
from ..models.cards import CardSet
from ..models.submodels import BoosterConfig, BoosterPack, BoosterSheet


class BoosterSimulator:
    """Simulates opening booster packs using set booster configuration data.

    Uses weighted random selection based on the ``booster`` field in set
    data (sheet weights and card weights). Requires the ``booster`` column
    (present in AllPrintings, but NOT in the flat ``sets.parquet`` from CDN).

    Example::

        types = sdk.booster.available_types("MH3")  # ["draft", "collector"]
        pack = sdk.booster.open_pack("MH3", "draft")
        box = sdk.booster.open_box("MH3", "draft", packs=36)
    """

    def __init__(self, conn: Connection) -> None:
        self._conn = conn

    def _ensure(self) -> None:
        self._conn.ensure_views("sets", "cards")

    def _get_booster_config(self, set_code: str) -> dict[str, BoosterConfig] | None:
        """Get booster configuration for a set.

        Requires the booster column (present in AllPrintings or test data,
        but NOT in the flat sets.parquet from CDN).
        """
        self._ensure()
        try:
            rows = self._conn.execute(
                "SELECT booster FROM sets WHERE code = $1", [set_code.upper()]
            )
        except Exception:
            # booster column may not exist in flat sets.parquet
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

    def open_pack(
        self,
        set_code: str,
        booster_type: str = "draft",
        *,
        as_dict: bool = False,
    ) -> list[CardSet] | list[dict]:
        """Simulate opening a single booster pack.

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

        config = configs[booster_type]
        pack_template = _pick_pack(config["boosters"])
        sheets = config["sheets"]

        card_uuids: list[str] = []
        for sheet_name, count in pack_template["contents"].items():
            if sheet_name not in sheets:
                continue
            sheet = sheets[sheet_name]
            picked = _pick_from_sheet(sheet, count)
            card_uuids.extend(picked)

        if not card_uuids:
            return []

        # Fetch card data
        self._conn.ensure_views("cards")
        placeholders = ", ".join(f"${i + 1}" for i in range(len(card_uuids)))
        sql = f"SELECT * FROM cards WHERE uuid IN ({placeholders})"
        rows = self._conn.execute(sql, card_uuids)

        # Preserve pack order
        uuid_to_row: dict[str, dict] = {r["uuid"]: r for r in rows}
        ordered = [uuid_to_row[u] for u in card_uuids if u in uuid_to_row]

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
