"""Tests for set_decks table (Connection layer — JSON column parsing)."""


def test_main_board_parsed_as_array(sample_db):
    rows = sample_db.execute(
        "SELECT mainBoard FROM set_decks WHERE code = $1",
        ["A25_DECK1"],
    )
    assert len(rows) == 1
    board = rows[0]["mainBoard"]
    assert isinstance(board, list)
    assert board[0]["uuid"] == "card-uuid-001"
    assert board[0]["count"] == 4


def test_side_board_parsed_as_array(sample_db):
    rows = sample_db.execute(
        "SELECT sideBoard FROM set_decks WHERE code = $1",
        ["A25_DECK1"],
    )
    assert len(rows) == 1
    board = rows[0]["sideBoard"]
    assert isinstance(board, list)
    assert len(board) == 1


def test_tokens_parsed_as_array(sample_db):
    rows = sample_db.execute(
        "SELECT tokens FROM set_decks WHERE code = $1",
        ["A25_DECK1"],
    )
    assert len(rows) == 1
    tokens = rows[0]["tokens"]
    assert isinstance(tokens, list)
    assert tokens[0]["uuid"] == "token-uuid-001"


def test_sealed_product_uuids_parsed_as_array(sample_db):
    rows = sample_db.execute(
        "SELECT sealedProductUuids FROM set_decks WHERE code = $1",
        ["A25_DECK1"],
    )
    assert len(rows) == 1
    uuids = rows[0]["sealedProductUuids"]
    assert isinstance(uuids, list)
    assert uuids[0] == "sealed-uuid-001"


def test_source_set_codes_parsed_as_array(sample_db):
    rows = sample_db.execute(
        "SELECT sourceSetCodes FROM set_decks WHERE code = $1",
        ["A25_DECK1"],
    )
    assert len(rows) == 1
    codes = rows[0]["sourceSetCodes"]
    assert isinstance(codes, list)
    assert codes[0] == "A25"


def test_commander_parsed_as_array(sample_db):
    rows = sample_db.execute(
        "SELECT commander FROM set_decks WHERE code = $1",
        ["A25_DECK1"],
    )
    assert len(rows) == 1
    assert isinstance(rows[0]["commander"], list)


def test_filter_by_set_code(sample_db):
    rows = sample_db.execute(
        "SELECT * FROM set_decks WHERE setCode = $1",
        ["MH2"],
    )
    assert len(rows) == 1
    assert rows[0]["name"] == "Modern Horizons 2 Theme Deck"
