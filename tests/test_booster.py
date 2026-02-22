"""Tests for the booster simulator."""

from mtgjson_sdk.booster.simulator import _pick_from_sheet, _pick_pack


def test_pick_pack_weighted():
    boosters = [
        {"contents": {"rare": 1, "common": 10}, "weight": 7},
        {"contents": {"mythic": 1, "common": 10}, "weight": 1},
    ]
    # Just verify it returns a valid pack
    pack = _pick_pack(boosters)
    assert "contents" in pack
    assert "weight" in pack


def test_pick_from_sheet_basic():
    sheet = {
        "cards": {"uuid-a": 10, "uuid-b": 5, "uuid-c": 1},
        "foil": False,
        "totalWeight": 16,
    }
    picked = _pick_from_sheet(sheet, 2)
    assert len(picked) == 2
    assert all(u in ("uuid-a", "uuid-b", "uuid-c") for u in picked)


def test_pick_from_sheet_no_duplicates():
    sheet = {
        "cards": {"uuid-a": 1, "uuid-b": 1, "uuid-c": 1},
        "foil": False,
        "totalWeight": 3,
    }
    picked = _pick_from_sheet(sheet, 3)
    assert len(picked) == 3
    assert len(set(picked)) == 3  # All unique


def test_pick_from_sheet_with_duplicates():
    sheet = {
        "cards": {"uuid-a": 1},
        "foil": False,
        "totalWeight": 1,
        "allowDuplicates": True,
    }
    picked = _pick_from_sheet(sheet, 3)
    assert len(picked) == 3
    assert all(u == "uuid-a" for u in picked)
