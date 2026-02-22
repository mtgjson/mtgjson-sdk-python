"""Tests for card queries."""

from mtgjson_sdk.models.cards import CardSet


def test_get_by_uuid(sdk_offline):
    card = sdk_offline.cards.get_by_uuid("card-uuid-001")
    assert isinstance(card, CardSet)
    assert card.name == "Lightning Bolt"
    assert card.uuid == "card-uuid-001"


def test_get_by_uuid_not_found(sdk_offline):
    assert sdk_offline.cards.get_by_uuid("nonexistent") is None


def test_get_by_uuid_as_dict(sdk_offline):
    result = sdk_offline.cards.get_by_uuid("card-uuid-001", as_dict=True)
    assert isinstance(result, dict)
    assert result["name"] == "Lightning Bolt"


def test_get_by_name(sdk_offline):
    cards = sdk_offline.cards.get_by_name("Lightning Bolt")
    assert len(cards) == 1
    assert cards[0].name == "Lightning Bolt"


def test_search_by_name_like(sdk_offline):
    cards = sdk_offline.cards.search(name="Lightning%")
    assert len(cards) >= 1
    assert all("Lightning" in c.name for c in cards)


def test_search_by_rarity(sdk_offline):
    cards = sdk_offline.cards.search(rarity="uncommon")
    assert len(cards) == 3


def test_search_by_mana_value(sdk_offline):
    cards = sdk_offline.cards.search(mana_value=1.0)
    assert len(cards) == 1
    assert cards[0].name == "Lightning Bolt"


def test_search_by_colors(sdk_offline):
    cards = sdk_offline.cards.search(colors=["U"])
    assert len(cards) >= 1
    assert all("U" in c.colors for c in cards)


def test_search_by_text(sdk_offline):
    cards = sdk_offline.cards.search(text="Counter target spell")
    assert len(cards) >= 1


def test_search_with_limit(sdk_offline):
    cards = sdk_offline.cards.search(limit=1)
    assert len(cards) == 1


def test_search_as_dict(sdk_offline):
    cards = sdk_offline.cards.search(rarity="uncommon", as_dict=True)
    assert all(isinstance(c, dict) for c in cards)


def test_search_legal_in(sdk_offline):
    cards = sdk_offline.cards.search(legal_in="modern")
    assert len(cards) == 2
    names = {c.name for c in cards}
    assert "Lightning Bolt" in names
    assert "Counterspell" in names


def test_get_printings(sdk_offline):
    cards = sdk_offline.cards.get_printings("Counterspell")
    assert len(cards) >= 1


def test_random(sdk_offline):
    cards = sdk_offline.cards.random(1)
    assert len(cards) == 1
    assert isinstance(cards[0], CardSet)


def test_count(sdk_offline):
    assert sdk_offline.cards.count() == 3


def test_find_by_scryfall_id(sdk_offline):
    cards = sdk_offline.cards.find_by_scryfall_id("scryfall-001")
    assert len(cards) >= 1
    assert cards[0].name == "Lightning Bolt"


# === New filter tests ===


def test_search_by_artist(sdk_offline):
    cards = sdk_offline.cards.search(artist="Christopher Moeller")
    assert len(cards) == 1
    assert cards[0].name == "Lightning Bolt"


def test_search_by_artist_like(sdk_offline):
    cards = sdk_offline.cards.search(artist="Zack")
    assert len(cards) == 1
    assert cards[0].name == "Counterspell"


def test_search_by_color_identity(sdk_offline):
    cards = sdk_offline.cards.search(color_identity=["R"])
    assert len(cards) >= 1
    assert all("R" in c.color_identity for c in cards)


def test_search_by_availability(sdk_offline):
    cards = sdk_offline.cards.search(availability="paper")
    assert len(cards) == 3


def test_search_by_language(sdk_offline):
    cards = sdk_offline.cards.search(language="English")
    assert len(cards) == 3


def test_search_by_layout(sdk_offline):
    cards = sdk_offline.cards.search(layout="normal")
    assert len(cards) == 2  # Lightning Bolt + Counterspell (Fire//Ice is "split")


def test_search_by_layout_split(sdk_offline):
    cards = sdk_offline.cards.search(layout="split")
    assert len(cards) == 1
    assert cards[0].name == "Fire // Ice"


def test_search_by_set_type(sdk_offline):
    cards = sdk_offline.cards.search(set_type="masters")
    assert len(cards) == 2  # Lightning Bolt + Fire // Ice (both in A25)


def test_search_by_mana_value_range(sdk_offline):
    cards = sdk_offline.cards.search(mana_value_gte=1.0, mana_value_lte=1.5)
    assert len(cards) == 1
    assert cards[0].name == "Lightning Bolt"


def test_get_atomic_by_face_name(sdk_offline):
    """get_atomic should fallback to searching by faceName for split cards."""
    result = sdk_offline.cards.get_atomic("Fire")
    assert len(result) >= 1
    assert result[0].face_name == "Fire"
    assert result[0].layout == "split"


def test_get_atomic_by_face_name_not_found(sdk_offline):
    result = sdk_offline.cards.get_atomic("Nonexistent Card Face")
    assert result == []


# === Bulk lookup tests ===


def test_get_by_uuids(sdk_offline):
    """Fetch multiple cards in one query."""
    cards = sdk_offline.cards.get_by_uuids(["card-uuid-001", "card-uuid-002"])
    assert len(cards) == 2
    names = {c.name for c in cards}
    assert names == {"Lightning Bolt", "Counterspell"}


def test_get_by_uuids_empty_list(sdk_offline):
    assert sdk_offline.cards.get_by_uuids([]) == []


def test_get_by_uuids_nonexistent(sdk_offline):
    cards = sdk_offline.cards.get_by_uuids(["no-such-uuid"])
    assert cards == []


def test_get_by_uuids_as_dict(sdk_offline):
    cards = sdk_offline.cards.get_by_uuids(
        ["card-uuid-001", "card-uuid-002"], as_dict=True
    )
    assert all(isinstance(c, dict) for c in cards)
    assert len(cards) == 2


def test_get_by_uuids_partial_match(sdk_offline):
    """Mix of existing and nonexistent UUIDs returns only found cards."""
    cards = sdk_offline.cards.get_by_uuids(["card-uuid-001", "no-such-uuid"])
    assert len(cards) == 1
    assert cards[0].name == "Lightning Bolt"


# === Foreign language search tests ===


def test_search_localized_name_exact(sdk_offline):
    """Search by exact foreign name."""
    cards = sdk_offline.cards.search(localized_name="Blitzschlag")
    assert len(cards) == 1
    assert cards[0].name == "Lightning Bolt"


def test_search_localized_name_like(sdk_offline):
    """Search by foreign name with wildcard."""
    cards = sdk_offline.cards.search(localized_name="Blitz%")
    assert len(cards) >= 1
    assert cards[0].name == "Lightning Bolt"


def test_search_localized_name_not_found(sdk_offline):
    cards = sdk_offline.cards.search(localized_name="Nonexistent")
    assert cards == []


def test_search_localized_name_french(sdk_offline):
    """Search French name 'Foudre' for Lightning Bolt."""
    cards = sdk_offline.cards.search(localized_name="Foudre")
    assert len(cards) == 1
    assert cards[0].name == "Lightning Bolt"


# === Regex text search tests ===


def test_search_text_regex(sdk_offline):
    """Search rules text with regex."""
    cards = sdk_offline.cards.search(text_regex="deals \\d+ damage")
    assert len(cards) >= 1
    names = {c.name for c in cards}
    assert "Lightning Bolt" in names


def test_search_text_regex_no_match(sdk_offline):
    cards = sdk_offline.cards.search(text_regex="^This card does nothing$")
    assert cards == []


# === Fuzzy name search tests ===


def test_search_fuzzy_name(sdk_offline):
    """Fuzzy search finds card despite typo."""
    cards = sdk_offline.cards.search(fuzzy_name="Ligtning Bolt")
    assert len(cards) >= 1
    assert cards[0].name == "Lightning Bolt"


def test_search_fuzzy_name_exact(sdk_offline):
    """Fuzzy search works with exact name too."""
    cards = sdk_offline.cards.search(fuzzy_name="Lightning Bolt")
    assert len(cards) >= 1
    assert cards[0].name == "Lightning Bolt"


def test_search_fuzzy_name_no_match(sdk_offline):
    """Fuzzy search returns empty for completely unrelated strings."""
    cards = sdk_offline.cards.search(fuzzy_name="zzzzzzzzzzzzz")
    assert cards == []


def test_search_fuzzy_name_ordered_by_similarity(sdk_offline):
    """Fuzzy results are ordered by similarity descending."""
    cards = sdk_offline.cards.search(fuzzy_name="Countrsepll")
    assert len(cards) >= 1
    assert cards[0].name == "Counterspell"
