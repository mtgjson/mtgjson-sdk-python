"""Tests for the identifier query module."""

import pytest


def test_find_by_scryfall_id(sdk_offline):
    cards = sdk_offline.identifiers.find_by_scryfall_id("scryfall-001")
    assert len(cards) >= 1
    assert cards[0].name == "Lightning Bolt"


def test_find_by_scryfall_oracle_id(sdk_offline):
    cards = sdk_offline.identifiers.find_by_scryfall_oracle_id("oracle-001")
    assert len(cards) >= 1
    assert cards[0].name == "Lightning Bolt"


def test_find_by_scryfall_illustration_id(sdk_offline):
    cards = sdk_offline.identifiers.find_by_scryfall_illustration_id("illust-002")
    assert len(cards) >= 1
    assert cards[0].name == "Counterspell"


def test_find_by_tcgplayer_id(sdk_offline):
    cards = sdk_offline.identifiers.find_by_tcgplayer_id("12345")
    assert len(cards) >= 1
    assert cards[0].name == "Lightning Bolt"


def test_find_by_mtgo_id(sdk_offline):
    cards = sdk_offline.identifiers.find_by_mtgo_id("mtgo-001")
    assert len(cards) >= 1


def test_find_by_mtgo_foil_id(sdk_offline):
    cards = sdk_offline.identifiers.find_by_mtgo_foil_id("mtgo-foil-001")
    assert len(cards) >= 1
    assert cards[0].name == "Lightning Bolt"


def test_find_by_mtg_arena_id(sdk_offline):
    cards = sdk_offline.identifiers.find_by_mtg_arena_id("arena-002")
    assert len(cards) >= 1
    assert cards[0].name == "Counterspell"


def test_find_by_multiverse_id(sdk_offline):
    cards = sdk_offline.identifiers.find_by_multiverse_id("442130")
    assert len(cards) >= 1


def test_find_by_mcm_id(sdk_offline):
    cards = sdk_offline.identifiers.find_by_mcm_id("mcm-001")
    assert len(cards) >= 1


def test_find_by_mcm_meta_id(sdk_offline):
    cards = sdk_offline.identifiers.find_by_mcm_meta_id("mcm-meta-001")
    assert len(cards) >= 1
    assert cards[0].name == "Lightning Bolt"


def test_find_by_card_kingdom_id(sdk_offline):
    cards = sdk_offline.identifiers.find_by_card_kingdom_id("ck-001")
    assert len(cards) >= 1


def test_find_by_card_kingdom_foil_id(sdk_offline):
    cards = sdk_offline.identifiers.find_by_card_kingdom_foil_id("ck-foil-001")
    assert len(cards) >= 1
    assert cards[0].name == "Lightning Bolt"


def test_find_by_cardsphere_id(sdk_offline):
    cards = sdk_offline.identifiers.find_by_cardsphere_id("cs-001")
    assert len(cards) >= 1


def test_find_by_cardsphere_foil_id(sdk_offline):
    cards = sdk_offline.identifiers.find_by_cardsphere_foil_id("cs-foil-001")
    assert len(cards) >= 1


def test_find_by_generic(sdk_offline):
    cards = sdk_offline.identifiers.find_by("scryfallId", "scryfall-001")
    assert len(cards) >= 1
    assert cards[0].name == "Lightning Bolt"


def test_find_by_generic_invalid_type(sdk_offline):
    with pytest.raises(ValueError, match="Unknown identifier type"):
        sdk_offline.identifiers.find_by("invalidColumn", "123")


def test_get_identifiers(sdk_offline):
    ids = sdk_offline.identifiers.get_identifiers("card-uuid-001")
    assert ids is not None
    assert ids["scryfallId"] == "scryfall-001"
    assert ids["mtgArenaId"] == "arena-001"


def test_find_by_not_found(sdk_offline):
    cards = sdk_offline.identifiers.find_by_scryfall_id("nonexistent")
    assert cards == []
