"""Tests for the token query module."""

from mtgjson_sdk.models.cards import CardToken


def test_token_get_by_uuid(sdk_offline):
    token = sdk_offline.tokens.get_by_uuid("token-uuid-001")
    assert token is not None
    assert isinstance(token, CardToken)
    assert token.name == "Soldier Token"


def test_token_get_by_uuid_not_found(sdk_offline):
    assert sdk_offline.tokens.get_by_uuid("nonexistent") is None


def test_token_get_by_uuid_as_dict(sdk_offline):
    token = sdk_offline.tokens.get_by_uuid("token-uuid-001", as_dict=True)
    assert isinstance(token, dict)
    assert token["name"] == "Soldier Token"


def test_token_get_by_name(sdk_offline):
    tokens = sdk_offline.tokens.get_by_name("Soldier Token")
    assert len(tokens) == 1
    assert tokens[0].name == "Soldier Token"


def test_token_search_by_name(sdk_offline):
    tokens = sdk_offline.tokens.search(name="%Token")
    assert len(tokens) == 2


def test_token_search_by_set(sdk_offline):
    tokens = sdk_offline.tokens.search(set_code="A25")
    assert len(tokens) == 1
    assert tokens[0].set_code == "A25"


def test_token_search_by_colors(sdk_offline):
    tokens = sdk_offline.tokens.search(colors=["G"])
    assert len(tokens) == 1
    assert tokens[0].name == "Beast Token"


def test_token_for_set(sdk_offline):
    tokens = sdk_offline.tokens.for_set("MH2")
    assert len(tokens) == 1
    assert tokens[0].name == "Beast Token"


def test_token_count(sdk_offline):
    assert sdk_offline.tokens.count() == 2


def test_token_count_filtered(sdk_offline):
    assert sdk_offline.tokens.count(setCode="A25") == 1


# === Bulk lookup tests ===


def test_token_get_by_uuids(sdk_offline):
    tokens = sdk_offline.tokens.get_by_uuids(["token-uuid-001", "token-uuid-002"])
    assert len(tokens) == 2
    names = {t.name for t in tokens}
    assert names == {"Soldier Token", "Beast Token"}


def test_token_get_by_uuids_empty(sdk_offline):
    assert sdk_offline.tokens.get_by_uuids([]) == []


def test_token_get_by_uuids_as_dict(sdk_offline):
    tokens = sdk_offline.tokens.get_by_uuids(["token-uuid-001"], as_dict=True)
    assert len(tokens) == 1
    assert isinstance(tokens[0], dict)
