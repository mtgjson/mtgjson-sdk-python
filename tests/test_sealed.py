"""Tests for the sealed product query module."""


def test_sealed_list(sdk_offline):
    products = sdk_offline.sealed.list()
    assert len(products) == 3


def test_sealed_list_by_set(sdk_offline):
    products = sdk_offline.sealed.list(set_code="A25")
    assert len(products) == 2
    assert all(p["setCode"] == "A25" for p in products)


def test_sealed_list_by_category(sdk_offline):
    products = sdk_offline.sealed.list(category="booster_box")
    assert len(products) == 2


def test_sealed_get(sdk_offline):
    product = sdk_offline.sealed.get("sealed-uuid-001")
    assert product is not None
    assert product["name"] == "Masters 25 Booster Box"
    assert product["setCode"] == "A25"


def test_sealed_get_not_found(sdk_offline):
    product = sdk_offline.sealed.get("nonexistent-uuid")
    assert product is None
