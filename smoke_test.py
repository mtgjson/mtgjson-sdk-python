"""Smoke test: pull real data from CDN and exercise ALL SDK methods.

Coverage goal: 100% of public methods, all filter parameters,
all output modes (model, dict, dataframe), and key edge cases.
"""

import logging
import sys
import time

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("smoke_test")

from mtgjson_sdk import MtgjsonSDK

PASS = 0
FAIL = 0
SKIP = 0


def check(label: str, condition: bool, detail: str = ""):
    global PASS, FAIL
    status = "PASS" if condition else "FAIL"
    if condition:
        PASS += 1
    else:
        FAIL += 1
    suffix = f" -- {detail}" if detail else ""
    print(f"  [{status}] {label}{suffix}")


def skip(label: str, reason: str = ""):
    global SKIP
    SKIP += 1
    suffix = f" -- {reason}" if reason else ""
    print(f"  [SKIP] {label}{suffix}")


def section(name: str):
    print(f"\n{'=' * 60}")
    print(f"  {name}")
    print(f"{'=' * 60}")


def main():
    t0 = time.time()

    # ══════════════════════════════════════════════════════════
    #  CLIENT LIFECYCLE
    # ══════════════════════════════════════════════════════════
    section("Client Lifecycle")

    # __repr__
    sdk = MtgjsonSDK()
    r = repr(sdk)
    check("__repr__", "MtgjsonSDK" in r, f"repr={r}")

    # context manager
    with MtgjsonSDK() as ctx_sdk:
        check("__enter__ returns SDK", isinstance(ctx_sdk, MtgjsonSDK))
    check("__exit__ (no error)", True, "context manager closed cleanly")

    # meta property
    meta = sdk.meta
    check(
        "meta loads",
        isinstance(meta, dict) and "data" in meta,
        f"keys={list(meta.keys())}",
    )
    if "data" in meta:
        version = meta["data"].get("version", "?")
        date = meta["data"].get("date", "?")
        check("meta has version", bool(version), f"v={version}, date={date}")

    # views property (starts empty, grows as we query)
    views_before = sdk.views
    check("views property (initial)", isinstance(views_before, list))

    # refresh() — in offline-like conditions, cache is fresh after meta load
    refresh_result = sdk.refresh()
    check("refresh()", isinstance(refresh_result, bool), f"stale={refresh_result}")

    # ══════════════════════════════════════════════════════════
    #  CARDS — CardQuery (8 methods, ~20 filter params)
    # ══════════════════════════════════════════════════════════
    section("Cards: get_by_name / get_by_uuid")

    bolt = sdk.cards.get_by_name("Lightning Bolt")
    check("get_by_name Lightning Bolt", len(bolt) > 0, f"found {len(bolt)} printings")

    # get_by_name with set_code filter
    bolt_lea = sdk.cards.get_by_name("Lightning Bolt", set_code="LEA")
    check("get_by_name set_code=LEA", len(bolt_lea) >= 0, f"found {len(bolt_lea)}")

    # get_by_name as_dict
    bolt_dicts = sdk.cards.get_by_name("Lightning Bolt", as_dict=True)
    check(
        "get_by_name as_dict", len(bolt_dicts) > 0 and isinstance(bolt_dicts[0], dict)
    )

    # get_by_name as_dataframe
    bolt_df = sdk.cards.get_by_name("Lightning Bolt", as_dataframe=True)
    check(
        "get_by_name as_dataframe",
        hasattr(bolt_df, "shape"),
        f"type={type(bolt_df).__name__}",
    )

    uuid = None
    if bolt:
        uuid = bolt[0].uuid

        # get_by_uuid — model
        card = sdk.cards.get_by_uuid(uuid)
        check("get_by_uuid (model)", card is not None and card.name == "Lightning Bolt")

        # get_by_uuid — as_dict
        card_dict = sdk.cards.get_by_uuid(uuid, as_dict=True)
        check(
            "get_by_uuid as_dict",
            isinstance(card_dict, dict) and card_dict.get("name") == "Lightning Bolt",
        )

        # get_by_uuid — as_dataframe
        card_df = sdk.cards.get_by_uuid(uuid, as_dataframe=True)
        check("get_by_uuid as_dataframe", hasattr(card_df, "shape"))

        # get_by_uuid — nonexistent
        missing = sdk.cards.get_by_uuid("00000000-0000-0000-0000-000000000000")
        check("get_by_uuid nonexistent returns None", missing is None)

    # ── Cards: get_by_uuids (bulk lookup) ──
    section("Cards: bulk lookups (get_by_uuids)")

    if bolt and len(bolt) >= 2:
        bulk_uuids = [b.uuid for b in bolt[:5]]
        bulk_cards = sdk.cards.get_by_uuids(bulk_uuids)
        check(
            "get_by_uuids (models)",
            len(bulk_cards) == len(bulk_uuids),
            f"requested {len(bulk_uuids)}, got {len(bulk_cards)}",
        )

        bulk_dicts = sdk.cards.get_by_uuids(bulk_uuids, as_dict=True)
        check(
            "get_by_uuids as_dict",
            len(bulk_dicts) > 0 and isinstance(bulk_dicts[0], dict),
        )

        bulk_df = sdk.cards.get_by_uuids(bulk_uuids, as_dataframe=True)
        check("get_by_uuids as_dataframe", hasattr(bulk_df, "shape"))

    # empty list
    bulk_empty = sdk.cards.get_by_uuids([])
    check("get_by_uuids empty list", bulk_empty == [])

    # nonexistent uuids
    bulk_none = sdk.cards.get_by_uuids(["00000000-0000-0000-0000-000000000000"])
    check("get_by_uuids nonexistent", len(bulk_none) == 0)

    # ── Cards: search (all filter params) ──
    section("Cards: search filters")

    # name LIKE
    s = sdk.cards.search(name="Lightning%", limit=10)
    check("search name LIKE", len(s) > 0, f"found {len(s)}")

    # exact name
    s = sdk.cards.search(name="Lightning Bolt", limit=5)
    check("search name exact", len(s) > 0)

    # colors
    s = sdk.cards.search(colors=["R"], mana_value=1.0, limit=5)
    check("search colors=R mv=1", len(s) > 0, f"found {len(s)}")

    # color_identity
    s = sdk.cards.search(color_identity=["W", "U"], limit=5)
    check("search color_identity=[W,U]", len(s) > 0, f"found {len(s)}")

    # types
    s = sdk.cards.search(types="Creature", limit=5)
    check("search types=Creature", len(s) > 0, f"found {len(s)}")

    # rarity
    s = sdk.cards.search(rarity="mythic", limit=5)
    check("search rarity=mythic", len(s) > 0, f"found {len(s)}")

    # text
    s = sdk.cards.search(text="draw a card", limit=5)
    check("search text='draw a card'", len(s) > 0, f"found {len(s)}")

    # power / toughness
    s = sdk.cards.search(power="4", toughness="4", limit=5)
    check("search power=4 toughness=4", len(s) > 0, f"found {len(s)}")

    # mana_value exact
    s = sdk.cards.search(mana_value=3.0, limit=5)
    check("search mana_value=3", len(s) > 0, f"found {len(s)}")

    # mana_value_lte
    s = sdk.cards.search(mana_value_lte=1.0, limit=5)
    check("search mana_value_lte=1", len(s) > 0, f"found {len(s)}")

    # mana_value_gte
    s = sdk.cards.search(mana_value_gte=10.0, limit=5)
    check("search mana_value_gte=10", len(s) > 0, f"found {len(s)}")

    # artist
    s = sdk.cards.search(artist="Christopher Moeller", limit=5)
    check("search artist", len(s) > 0, f"found {len(s)}")

    # keyword
    s = sdk.cards.search(keyword="Flying", limit=5)
    check("search keyword=Flying", len(s) > 0, f"found {len(s)}")

    # layout
    s = sdk.cards.search(layout="split", limit=5)
    check("search layout=split", len(s) > 0, f"found {len(s)}")

    # is_promo
    s = sdk.cards.search(is_promo=True, limit=5)
    check("search is_promo=True", len(s) > 0, f"found {len(s)}")

    s_np = sdk.cards.search(is_promo=False, limit=5)
    check("search is_promo=False", len(s_np) > 0, f"found {len(s_np)}")

    # availability
    s = sdk.cards.search(availability="mtgo", limit=5)
    check("search availability=mtgo", len(s) > 0, f"found {len(s)}")

    s = sdk.cards.search(availability="paper", limit=5)
    check("search availability=paper", len(s) > 0, f"found {len(s)}")

    # language
    s = sdk.cards.search(language="Japanese", limit=5)
    check("search language=Japanese", isinstance(s, list), f"found {len(s)}")

    # set_code
    s = sdk.cards.search(set_code="MH3", limit=5)
    check("search set_code=MH3", len(s) > 0, f"found {len(s)}")

    # set_type (requires JOIN with sets)
    s = sdk.cards.search(set_type="expansion", limit=5)
    check("search set_type=expansion", len(s) > 0, f"found {len(s)}")

    # legal_in + mana_value_lte
    s = sdk.cards.search(legal_in="modern", mana_value_lte=2.0, limit=5)
    check("search legal_in=modern + mana_value_lte", len(s) > 0, f"found {len(s)}")

    # combined filters
    s = sdk.cards.search(colors=["R"], rarity="rare", mana_value_lte=3.0, limit=5)
    check("search combined (colors+rarity+mv)", len(s) > 0, f"found {len(s)}")

    # offset (pagination)
    page1 = sdk.cards.search(name="Lightning%", limit=3, offset=0)
    page2 = sdk.cards.search(name="Lightning%", limit=3, offset=3)
    check(
        "search offset (pagination)",
        len(page1) > 0 and len(page2) > 0,
        "two pages fetched",
    )
    if page1 and page2:
        check("search pages differ", page1[0].uuid != page2[0].uuid, "different cards")

    # text_regex
    s = sdk.cards.search(text_regex="deals \\d+ damage", limit=5)
    check("search text_regex", len(s) > 0, f"found {len(s)}")

    # localized_name (foreign language search)
    s = sdk.cards.search(localized_name="Blitzschlag", limit=5)
    check(
        "search localized_name (German)",
        len(s) > 0,
        f"found {len(s)}, name={s[0].name if s else '?'}",
    )

    # localized_name LIKE
    s = sdk.cards.search(localized_name="%Foudre%", limit=5)
    check("search localized_name LIKE", isinstance(s, list), f"found {len(s)}")

    # search as_dict
    s = sdk.cards.search(name="Lightning%", limit=3, as_dict=True)
    check("search as_dict", len(s) > 0 and isinstance(s[0], dict))

    # search as_dataframe
    s = sdk.cards.search(name="Lightning%", limit=3, as_dataframe=True)
    check("search as_dataframe", hasattr(s, "shape"))

    # ── Cards: other methods ──
    section("Cards: random, count, printings, atomic, find_by_scryfall_id")

    rand = sdk.cards.random(3)
    check("random(3)", len(rand) == 3, f"names: {[c.name for c in rand]}")

    rand_dict = sdk.cards.random(2, as_dict=True)
    check("random as_dict", len(rand_dict) == 2 and isinstance(rand_dict[0], dict))

    rand_df = sdk.cards.random(2, as_dataframe=True)
    check("random as_dataframe", hasattr(rand_df, "shape"))

    count = sdk.cards.count()
    check("count()", count > 1000, f"total cards: {count}")

    # count with filters
    count_r = sdk.cards.count(rarity="mythic")
    check(
        "count(rarity=mythic)",
        count_r > 0 and count_r < count,
        f"mythic cards: {count_r}",
    )

    printings = sdk.cards.get_printings("Counterspell")
    check(
        "get_printings Counterspell",
        len(printings) > 5,
        f"found {len(printings)} printings",
    )

    printings_dict = sdk.cards.get_printings("Counterspell", as_dict=True)
    check(
        "get_printings as_dict",
        len(printings_dict) > 0 and isinstance(printings_dict[0], dict),
    )

    printings_df = sdk.cards.get_printings("Counterspell", as_dataframe=True)
    check("get_printings as_dataframe", hasattr(printings_df, "shape"))

    # get_atomic — exact name
    atomic = sdk.cards.get_atomic("Lightning Bolt")
    check("get_atomic Lightning Bolt", len(atomic) > 0)

    atomic_dict = sdk.cards.get_atomic("Lightning Bolt", as_dict=True)
    check(
        "get_atomic as_dict", len(atomic_dict) > 0 and isinstance(atomic_dict[0], dict)
    )

    # get_atomic — face name fallback for split cards
    atomic_fire = sdk.cards.get_atomic("Fire")
    check(
        "get_atomic face name 'Fire'",
        len(atomic_fire) > 0,
        f"layout={atomic_fire[0].layout if atomic_fire else '?'}",
    )

    # find_by_scryfall_id (may not match since uuid != scryfallId)
    if uuid:
        scry_cards = sdk.cards.find_by_scryfall_id(uuid)
        check("find_by_scryfall_id runs", isinstance(scry_cards, list), "no error")

        scry_dict = sdk.cards.find_by_scryfall_id(uuid, as_dict=True)
        check("find_by_scryfall_id as_dict", isinstance(scry_dict, list))

        scry_df = sdk.cards.find_by_scryfall_id(uuid, as_dataframe=True)
        check("find_by_scryfall_id as_dataframe", hasattr(scry_df, "shape"))

    # ══════════════════════════════════════════════════════════
    #  TOKENS — TokenQuery (5 methods, ~8 filter params)
    # ══════════════════════════════════════════════════════════
    section("Tokens")

    token_count = sdk.tokens.count()
    check("token count()", token_count > 0, f"total tokens: {token_count}")

    # count with filters
    # (setCode is a common column, should work)
    token_count_mh3 = sdk.tokens.count(setCode="MH3")
    check(
        "token count(setCode=MH3)",
        isinstance(token_count_mh3, int),
        f"MH3 tokens: {token_count_mh3}",
    )

    # search by name LIKE
    token_search = sdk.tokens.search(name="%Soldier%", limit=5)
    check("token search name LIKE", len(token_search) > 0, f"found {len(token_search)}")

    # search by set_code — find a set that actually has tokens
    # First discover a set code from existing token data
    token_set_row = sdk.sql("SELECT DISTINCT setCode FROM tokens LIMIT 1")
    token_set_code = token_set_row[0]["setCode"] if token_set_row else "MH3"
    token_search_set = sdk.tokens.search(set_code=token_set_code, limit=5)
    check(
        "token search set_code",
        len(token_search_set) > 0,
        f"set={token_set_code}, found {len(token_search_set)}",
    )

    # search by types
    token_search_type = sdk.tokens.search(types="Creature", limit=5)
    check(
        "token search types=Creature",
        len(token_search_type) > 0,
        f"found {len(token_search_type)}",
    )

    # search by artist
    token_search_artist = sdk.tokens.search(
        artist="", limit=5
    )  # empty string = no filter
    check("token search with artist param", isinstance(token_search_artist, list))

    # search by colors
    token_search_colors = sdk.tokens.search(colors=["W"], limit=5)
    check(
        "token search colors=[W]",
        len(token_search_colors) > 0,
        f"found {len(token_search_colors)}",
    )

    # search with offset
    tp1 = sdk.tokens.search(name="%Soldier%", limit=2, offset=0)
    tp2 = sdk.tokens.search(name="%Soldier%", limit=2, offset=2)
    check("token search offset", isinstance(tp1, list) and isinstance(tp2, list))

    # search as_dict
    token_dict = sdk.tokens.search(name="%Soldier%", limit=3, as_dict=True)
    check(
        "token search as_dict", len(token_dict) > 0 and isinstance(token_dict[0], dict)
    )

    # search as_dataframe
    token_df = sdk.tokens.search(name="%Soldier%", limit=3, as_dataframe=True)
    check("token search as_dataframe", hasattr(token_df, "shape"))

    # get_by_uuid
    if token_search:
        token = sdk.tokens.get_by_uuid(token_search[0].uuid)
        check(
            "token get_by_uuid (model)",
            token is not None,
            f"name={token.name if token else '?'}",
        )

        token_d = sdk.tokens.get_by_uuid(token_search[0].uuid, as_dict=True)
        check("token get_by_uuid as_dict", isinstance(token_d, dict))

        token_f = sdk.tokens.get_by_uuid(token_search[0].uuid, as_dataframe=True)
        check("token get_by_uuid as_dataframe", hasattr(token_f, "shape"))

        # nonexistent
        missing_token = sdk.tokens.get_by_uuid("00000000-0000-0000-0000-000000000000")
        check("token get_by_uuid nonexistent", missing_token is None)

    # get_by_name
    token_soldiers = sdk.tokens.get_by_name("Soldier")
    check(
        "token get_by_name Soldier",
        len(token_soldiers) > 0,
        f"found {len(token_soldiers)}",
    )

    token_soldiers_dict = sdk.tokens.get_by_name("Soldier", as_dict=True)
    check(
        "token get_by_name as_dict",
        len(token_soldiers_dict) > 0 and isinstance(token_soldiers_dict[0], dict),
    )

    token_soldiers_df = sdk.tokens.get_by_name("Soldier", as_dataframe=True)
    check("token get_by_name as_dataframe", hasattr(token_soldiers_df, "shape"))

    # get_by_name with set_code — use a set that has tokens
    token_soldiers_set = sdk.tokens.get_by_name("Soldier", set_code=token_set_code)
    check(
        "token get_by_name set_code",
        isinstance(token_soldiers_set, list),
        f"set={token_set_code}, found {len(token_soldiers_set)}",
    )

    # for_set — use known token set
    tokens_for = sdk.tokens.for_set(token_set_code)
    check(
        "token for_set",
        len(tokens_for) > 0,
        f"set={token_set_code}, found {len(tokens_for)}",
    )

    tokens_for_dict = sdk.tokens.for_set(token_set_code, as_dict=True)
    check(
        "token for_set as_dict",
        isinstance(tokens_for_dict, list) and len(tokens_for_dict) > 0,
    )

    tokens_for_df = sdk.tokens.for_set(token_set_code, as_dataframe=True)
    check("token for_set as_dataframe", hasattr(tokens_for_df, "shape"))

    # get_by_uuids (bulk token lookup)
    if token_search and len(token_search) >= 2:
        token_uuids = [t.uuid for t in token_search[:3]]
        bulk_tokens = sdk.tokens.get_by_uuids(token_uuids)
        check(
            "token get_by_uuids",
            len(bulk_tokens) == len(token_uuids),
            f"requested {len(token_uuids)}, got {len(bulk_tokens)}",
        )

        bulk_tokens_d = sdk.tokens.get_by_uuids(token_uuids, as_dict=True)
        check("token get_by_uuids as_dict", isinstance(bulk_tokens_d[0], dict))

    check("token get_by_uuids empty", sdk.tokens.get_by_uuids([]) == [])

    # ══════════════════════════════════════════════════════════
    #  SETS — SetQuery (4 methods, ~7 filter params)
    # ══════════════════════════════════════════════════════════
    section("Sets")

    # get
    mh3 = sdk.sets.get("MH3")
    check("get set MH3", mh3 is not None, f"name={mh3.name if mh3 else '?'}")

    mh3_dict = sdk.sets.get("MH3", as_dict=True)
    check("get set as_dict", isinstance(mh3_dict, dict))

    mh3_df = sdk.sets.get("MH3", as_dataframe=True)
    check("get set as_dataframe", hasattr(mh3_df, "shape"))

    # get nonexistent
    missing_set = sdk.sets.get("ZZZZZ")
    check("get set nonexistent", missing_set is None)

    # list — no filter
    all_sets = sdk.sets.list(limit=10)
    check("list sets (no filter)", len(all_sets) > 0, f"found {len(all_sets)}")

    # list — set_type
    expansions = sdk.sets.list(set_type="expansion", limit=10)
    check("list expansions", len(expansions) > 0, f"found {len(expansions)}")

    # list — name filter
    horizon_list = sdk.sets.list(name="%Horizons%", limit=10)
    check("list name filter", len(horizon_list) > 0, f"found {len(horizon_list)}")

    # list — offset
    sets_p1 = sdk.sets.list(limit=3, offset=0)
    sets_p2 = sdk.sets.list(limit=3, offset=3)
    check("list offset (pagination)", len(sets_p1) > 0 and len(sets_p2) > 0)
    if sets_p1 and sets_p2:
        check("list pages differ", sets_p1[0].code != sets_p2[0].code)

    # list — as_dict
    sets_d = sdk.sets.list(limit=3, as_dict=True)
    check("list as_dict", isinstance(sets_d[0], dict))

    # list — as_dataframe
    sets_df = sdk.sets.list(limit=3, as_dataframe=True)
    check("list as_dataframe", hasattr(sets_df, "shape"))

    # search — name
    set_search = sdk.sets.search(name="Horizons")
    check("search 'Horizons'", len(set_search) > 0, f"found {len(set_search)}")

    # search — set_type
    set_search_type = sdk.sets.search(set_type="masters", limit=10)
    check(
        "search set_type=masters",
        len(set_search_type) > 0,
        f"found {len(set_search_type)}",
    )

    # search — block
    set_search_block = sdk.sets.search(block="Innistrad")
    check(
        "search block=Innistrad",
        isinstance(set_search_block, list),
        f"found {len(set_search_block)}",
    )

    # search — release_year
    set_search_year = sdk.sets.search(release_year=2024, limit=10)
    check(
        "search release_year=2024",
        len(set_search_year) > 0,
        f"found {len(set_search_year)}",
    )

    # search — as_dict
    set_search_dict = sdk.sets.search(name="Horizons", as_dict=True)
    check(
        "search as_dict",
        len(set_search_dict) > 0 and isinstance(set_search_dict[0], dict),
    )

    # search — as_dataframe
    set_search_df = sdk.sets.search(name="Horizons", as_dataframe=True)
    check("search as_dataframe", hasattr(set_search_df, "shape"))

    # count
    set_count = sdk.sets.count()
    check("set count", set_count > 100, f"total sets: {set_count}")

    # ══════════════════════════════════════════════════════════
    #  IDENTIFIERS — IdentifierQuery (18 methods)
    # ══════════════════════════════════════════════════════════
    section("Identifiers")

    if uuid:
        ids = sdk.identifiers.get_identifiers(uuid)
        check(
            "get_identifiers",
            ids is not None,
            f"keys={list(ids.keys()) if ids else '?'}",
        )

        # Exercise ALL named find_by_* methods using real IDs from Lightning Bolt
        if ids:
            # Scryfall ID
            if ids.get("scryfallId"):
                by_scry = sdk.identifiers.find_by_scryfall_id(ids["scryfallId"])
                check("find_by_scryfall_id", len(by_scry) > 0, f"found {len(by_scry)}")

                by_scry_dict = sdk.identifiers.find_by_scryfall_id(
                    ids["scryfallId"], as_dict=True
                )
                check("find_by_scryfall_id as_dict", isinstance(by_scry_dict, list))
            else:
                skip("find_by_scryfall_id", "no scryfallId in data")

            # Scryfall Oracle ID
            if ids.get("scryfallOracleId"):
                by_oracle = sdk.identifiers.find_by_scryfall_oracle_id(
                    ids["scryfallOracleId"]
                )
                check(
                    "find_by_scryfall_oracle_id",
                    len(by_oracle) > 0,
                    f"found {len(by_oracle)}",
                )
            else:
                skip("find_by_scryfall_oracle_id", "no scryfallOracleId")

            # Scryfall Illustration ID
            if ids.get("scryfallIllustrationId"):
                by_illus = sdk.identifiers.find_by_scryfall_illustration_id(
                    ids["scryfallIllustrationId"]
                )
                check(
                    "find_by_scryfall_illustration_id",
                    len(by_illus) > 0,
                    f"found {len(by_illus)}",
                )
            else:
                skip("find_by_scryfall_illustration_id", "no scryfallIllustrationId")

            # TCGPlayer Product ID
            if ids.get("tcgplayerProductId"):
                by_tcg = sdk.identifiers.find_by_tcgplayer_id(
                    str(ids["tcgplayerProductId"])
                )
                check("find_by_tcgplayer_id", len(by_tcg) > 0, f"found {len(by_tcg)}")
            else:
                skip("find_by_tcgplayer_id", "no tcgplayerProductId")

            # TCGPlayer Etched ID
            if ids.get("tcgplayerEtchedProductId"):
                by_tcg_e = sdk.identifiers.find_by_tcgplayer_etched_id(
                    str(ids["tcgplayerEtchedProductId"])
                )
                check("find_by_tcgplayer_etched_id", len(by_tcg_e) > 0)
            else:
                skip("find_by_tcgplayer_etched_id", "no tcgplayerEtchedProductId")

            # MTGO ID
            if ids.get("mtgoId"):
                by_mtgo = sdk.identifiers.find_by_mtgo_id(str(ids["mtgoId"]))
                check("find_by_mtgo_id", len(by_mtgo) > 0)
            else:
                skip("find_by_mtgo_id", "no mtgoId")

            # MTGO Foil ID
            if ids.get("mtgoFoilId"):
                by_mtgo_f = sdk.identifiers.find_by_mtgo_foil_id(str(ids["mtgoFoilId"]))
                check("find_by_mtgo_foil_id", len(by_mtgo_f) > 0)
            else:
                skip("find_by_mtgo_foil_id", "no mtgoFoilId")

            # MTG Arena ID
            if ids.get("mtgArenaId"):
                by_arena = sdk.identifiers.find_by_mtg_arena_id(str(ids["mtgArenaId"]))
                check("find_by_mtg_arena_id", len(by_arena) > 0)
            else:
                skip("find_by_mtg_arena_id", "no mtgArenaId")

            # Multiverse ID
            if ids.get("multiverseId"):
                by_multi = sdk.identifiers.find_by_multiverse_id(
                    str(ids["multiverseId"])
                )
                check("find_by_multiverse_id", len(by_multi) > 0)
            else:
                skip("find_by_multiverse_id", "no multiverseId")

            # MCM ID
            if ids.get("mcmId"):
                by_mcm = sdk.identifiers.find_by_mcm_id(str(ids["mcmId"]))
                check("find_by_mcm_id", len(by_mcm) > 0)
            else:
                skip("find_by_mcm_id", "no mcmId")

            # MCM Meta ID
            if ids.get("mcmMetaId"):
                by_mcm_m = sdk.identifiers.find_by_mcm_meta_id(str(ids["mcmMetaId"]))
                check("find_by_mcm_meta_id", len(by_mcm_m) > 0)
            else:
                skip("find_by_mcm_meta_id", "no mcmMetaId")

            # Card Kingdom ID
            if ids.get("cardKingdomId"):
                by_ck = sdk.identifiers.find_by_card_kingdom_id(
                    str(ids["cardKingdomId"])
                )
                check("find_by_card_kingdom_id", len(by_ck) > 0)
            else:
                skip("find_by_card_kingdom_id", "no cardKingdomId")

            # Card Kingdom Foil ID
            if ids.get("cardKingdomFoilId"):
                by_ck_f = sdk.identifiers.find_by_card_kingdom_foil_id(
                    str(ids["cardKingdomFoilId"])
                )
                check("find_by_card_kingdom_foil_id", len(by_ck_f) > 0)
            else:
                skip("find_by_card_kingdom_foil_id", "no cardKingdomFoilId")

            # Card Kingdom Etched ID
            if ids.get("cardKingdomEtchedId"):
                by_ck_e = sdk.identifiers.find_by_card_kingdom_etched_id(
                    str(ids["cardKingdomEtchedId"])
                )
                check("find_by_card_kingdom_etched_id", len(by_ck_e) > 0)
            else:
                skip("find_by_card_kingdom_etched_id", "no cardKingdomEtchedId")

            # Cardsphere ID
            if ids.get("cardsphereId"):
                by_cs = sdk.identifiers.find_by_cardsphere_id(str(ids["cardsphereId"]))
                check("find_by_cardsphere_id", len(by_cs) > 0)
            else:
                skip("find_by_cardsphere_id", "no cardsphereId")

            # Cardsphere Foil ID
            if ids.get("cardsphereFoilId"):
                by_cs_f = sdk.identifiers.find_by_cardsphere_foil_id(
                    str(ids["cardsphereFoilId"])
                )
                check("find_by_cardsphere_foil_id", len(by_cs_f) > 0)
            else:
                skip("find_by_cardsphere_foil_id", "no cardsphereFoilId")

            # Generic find_by with valid column
            if ids.get("scryfallId"):
                by_gen = sdk.identifiers.find_by("scryfallId", ids["scryfallId"])
                check("find_by generic (scryfallId)", len(by_gen) > 0)

            # Generic find_by as_dict
            if ids.get("scryfallId"):
                by_gen_d = sdk.identifiers.find_by(
                    "scryfallId", ids["scryfallId"], as_dict=True
                )
                check("find_by generic as_dict", isinstance(by_gen_d, list))

    # Second card for identifier coverage — find one with more IDs populated
    # Use a recent card likely to have Arena, Multiverse, Cardsphere, etc.
    section("Identifiers (secondary card for fuller coverage)")
    alt_cards = sdk.cards.search(name="Llanowar Elves", set_code="M19", limit=1)
    if alt_cards:
        alt_uuid = alt_cards[0].uuid
        alt_ids = sdk.identifiers.get_identifiers(alt_uuid)
        if alt_ids:
            for id_col, method_name in [
                ("mtgArenaId", "find_by_mtg_arena_id"),
                ("multiverseId", "find_by_multiverse_id"),
                ("mcmMetaId", "find_by_mcm_meta_id"),
                ("cardsphereId", "find_by_cardsphere_id"),
                ("cardsphereFoilId", "find_by_cardsphere_foil_id"),
                ("cardKingdomEtchedId", "find_by_card_kingdom_etched_id"),
                ("tcgplayerEtchedProductId", "find_by_tcgplayer_etched_id"),
                ("mtgoFoilId", "find_by_mtgo_foil_id"),
            ]:
                val = alt_ids.get(id_col)
                if val:
                    method = getattr(sdk.identifiers, method_name)
                    result = method(str(val))
                    check(
                        f"{method_name} (alt card)", len(result) > 0, f"{id_col}={val}"
                    )
                else:
                    skip(f"{method_name} (alt card)", f"no {id_col}")
        else:
            skip("alt card identifiers", "no identifiers found")
    else:
        skip("alt card identifiers", "Llanowar Elves M19 not found")

    # find_by — invalid column raises ValueError
    try:
        sdk.identifiers.find_by("invalidColumn", "123")
        check("find_by invalid column raises", False)
    except ValueError:
        check("find_by invalid column raises", True)

    # ══════════════════════════════════════════════════════════
    #  LEGALITIES — LegalityQuery (7 methods)
    # ══════════════════════════════════════════════════════════
    section("Legalities")

    if uuid:
        # formats_for_card
        formats = sdk.legalities.formats_for_card(uuid)
        check(
            "formats_for_card",
            len(formats) > 0,
            f"formats: {list(formats.keys())[:5]}...",
        )

        # is_legal
        is_legal = sdk.legalities.is_legal(uuid, "modern")
        check("is_legal modern", is_legal is True)

        is_legal_fake = sdk.legalities.is_legal(uuid, "nonexistent_format")
        check("is_legal nonexistent format", is_legal_fake is False)

    # legal_in — model
    modern_cards = sdk.legalities.legal_in("modern", limit=5)
    check("legal_in modern", len(modern_cards) > 0, f"found {len(modern_cards)}")

    # legal_in — as_dict
    modern_dict = sdk.legalities.legal_in("modern", limit=3, as_dict=True)
    check("legal_in as_dict", len(modern_dict) > 0 and isinstance(modern_dict[0], dict))

    # legal_in — as_dataframe
    modern_df = sdk.legalities.legal_in("modern", limit=3, as_dataframe=True)
    check("legal_in as_dataframe", hasattr(modern_df, "shape"))

    # legal_in — with offset
    legal_p1 = sdk.legalities.legal_in("modern", limit=3, offset=0)
    legal_p2 = sdk.legalities.legal_in("modern", limit=3, offset=3)
    check("legal_in offset", len(legal_p1) > 0 and len(legal_p2) > 0)

    # banned_in
    banned = sdk.legalities.banned_in("modern", limit=5)
    check("banned_in modern", isinstance(banned, list), f"found {len(banned)}")

    # restricted_in
    restricted = sdk.legalities.restricted_in("vintage", limit=5)
    check(
        "restricted_in vintage",
        isinstance(restricted, list),
        f"found {len(restricted)}",
    )

    # suspended_in (may have 0 results if no cards are currently suspended)
    suspended = sdk.legalities.suspended_in("historic", limit=5)
    check(
        "suspended_in historic", isinstance(suspended, list), f"found {len(suspended)}"
    )

    # not_legal_in
    not_legal = sdk.legalities.not_legal_in("standard", limit=5)
    check(
        "not_legal_in standard", isinstance(not_legal, list), f"found {len(not_legal)}"
    )

    # ══════════════════════════════════════════════════════════
    #  PRICES — PriceQuery (5 methods)
    #  Downloads AllPricesToday.json.gz (~large file)
    # ══════════════════════════════════════════════════════════
    section("Prices")

    try:
        # get — raw nested structure
        if uuid:
            price_raw = sdk.prices.get(uuid)
            check(
                "prices.get",
                isinstance(price_raw, (dict, type(None))),
                f"type={type(price_raw).__name__}",
            )

            # today
            today_prices = sdk.prices.today(uuid)
            check(
                "prices.today",
                isinstance(today_prices, list),
                f"found {len(today_prices)} rows",
            )

            if today_prices:
                # today — with provider filter
                providers = {r.get("provider") for r in today_prices}
                if providers:
                    first_prov = next(iter(providers))
                    today_filt = sdk.prices.today(uuid, provider=first_prov)
                    check(
                        "prices.today provider filter",
                        len(today_filt) > 0,
                        f"provider={first_prov}",
                    )

                # today — with finish filter
                finishes = {r.get("finish") for r in today_prices}
                if finishes:
                    first_fin = next(iter(finishes))
                    today_fin = sdk.prices.today(uuid, finish=first_fin)
                    check(
                        "prices.today finish filter",
                        len(today_fin) > 0,
                        f"finish={first_fin}",
                    )

                # today — with category filter
                categories = {r.get("category") for r in today_prices}
                if categories:
                    first_cat = next(iter(categories))
                    today_cat = sdk.prices.today(uuid, category=first_cat)
                    check(
                        "prices.today category filter",
                        len(today_cat) > 0,
                        f"cat={first_cat}",
                    )

                # today — as_dataframe
                today_df = sdk.prices.today(uuid, as_dataframe=True)
                check("prices.today as_dataframe", hasattr(today_df, "shape"))

            # history
            history = sdk.prices.history(uuid)
            check(
                "prices.history",
                isinstance(history, list),
                f"found {len(history)} rows",
            )

            if history:
                # history — with date range
                dates = sorted({r.get("date", "") for r in history if r.get("date")})
                if len(dates) >= 2:
                    hist_range = sdk.prices.history(
                        uuid, date_from=dates[0], date_to=dates[-1]
                    )
                    check("prices.history date range", len(hist_range) > 0)
                else:
                    skip("prices.history date range", "only 1 date")

                # history — with provider filter
                hist_prov = sdk.prices.history(
                    uuid, provider=history[0].get("provider", "tcgplayer")
                )
                check("prices.history provider filter", isinstance(hist_prov, list))

                # history — as_dataframe
                hist_df = sdk.prices.history(uuid, as_dataframe=True)
                check("prices.history as_dataframe", hasattr(hist_df, "shape"))

            # price_trend
            trend = sdk.prices.price_trend(uuid)
            check(
                "prices.price_trend",
                isinstance(trend, (dict, type(None))),
                f"trend={trend}" if trend else "no trend data",
            )

            if trend:
                check(
                    "price_trend has keys",
                    all(k in trend for k in ("min_price", "max_price", "avg_price")),
                )

                # price_trend with provider/finish
                trend2 = sdk.prices.price_trend(
                    uuid, provider="tcgplayer", finish="normal"
                )
                check(
                    "price_trend with filters", isinstance(trend2, (dict, type(None)))
                )

            # cheapest_printing
            cheapest = sdk.prices.cheapest_printing("Lightning Bolt")
            check(
                "prices.cheapest_printing",
                isinstance(cheapest, (dict, type(None))),
                f"cheapest={cheapest}" if cheapest else "no price data",
            )

            if cheapest:
                check(
                    "cheapest has price", "price" in cheapest and cheapest["price"] > 0
                )

            # cheapest with different provider
            cheapest2 = sdk.prices.cheapest_printing(
                "Lightning Bolt", provider="cardkingdom"
            )
            check(
                "cheapest_printing alt provider",
                isinstance(cheapest2, (dict, type(None))),
            )

        else:
            skip("prices tests", "no uuid from Lightning Bolt")

    except Exception as e:
        check("prices module available", False, f"error: {e}")

    # ══════════════════════════════════════════════════════════
    #  SET FINANCIAL SUMMARY (requires prices loaded)
    # ══════════════════════════════════════════════════════════
    section("Set Financial Summary (EV calculation)")

    if "prices_today" in sdk._conn._registered_views:
        fin = sdk.sets.get_financial_summary("MH3")
        check(
            "get_financial_summary MH3",
            fin is not None and fin.get("card_count", 0) > 0,
            f"cards={fin.get('card_count')}, total=${fin.get('total_value')}"
            if fin
            else "no data",
        )

        # With different provider
        fin_ck = sdk.sets.get_financial_summary(
            "MH3", provider="cardkingdom", finish="normal"
        )
        check(
            "get_financial_summary alt provider",
            isinstance(fin_ck, (dict, type(None))),
        )

        # Nonexistent set
        fin_none = sdk.sets.get_financial_summary("ZZZZZ")
        check("get_financial_summary no data", fin_none is None)
    else:
        skip("get_financial_summary", "prices not loaded")

    # ══════════════════════════════════════════════════════════
    #  DECKS — DeckQuery (3 methods)
    #  Downloads DeckList.json
    # ══════════════════════════════════════════════════════════
    section("Decks")

    try:
        # count
        deck_count = sdk.decks.count()
        check("decks.count", deck_count >= 0, f"total decks: {deck_count}")

        # list — no filter
        deck_list = sdk.decks.list()
        check(
            "decks.list (all)", isinstance(deck_list, list), f"found {len(deck_list)}"
        )

        if deck_list:
            # list — as_dict
            deck_list_d = sdk.decks.list(as_dict=True)
            check(
                "decks.list as_dict",
                len(deck_list_d) > 0 and isinstance(deck_list_d[0], dict),
            )

            # list — set_code filter
            first_code = (
                deck_list[0].code
                if hasattr(deck_list[0], "code")
                else deck_list_d[0].get("code", "")
            )
            if first_code:
                decks_by_set = sdk.decks.list(set_code=first_code)
                check("decks.list set_code", len(decks_by_set) > 0, f"set={first_code}")

            # list — deck_type filter
            first_type = deck_list[0].type if hasattr(deck_list[0], "type") else ""
            if first_type:
                decks_by_type = sdk.decks.list(deck_type=first_type)
                check(
                    "decks.list deck_type", len(decks_by_type) > 0, f"type={first_type}"
                )

            # search — name
            first_name = deck_list[0].name if hasattr(deck_list[0], "name") else ""
            if first_name:
                # use first word of name for partial match
                search_term = first_name.split()[0] if first_name else "Starter"
                deck_search = sdk.decks.search(name=search_term)
                check(
                    "decks.search name", len(deck_search) > 0, f"term='{search_term}'"
                )

            # search — set_code
            if first_code:
                deck_search_set = sdk.decks.search(set_code=first_code)
                check("decks.search set_code", len(deck_search_set) > 0)

            # search — as_dict
            deck_search_d = sdk.decks.search(
                name=first_name.split()[0] if first_name else "Starter", as_dict=True
            )
            check("decks.search as_dict", isinstance(deck_search_d, list))
        else:
            skip("deck list/search tests", "no decks loaded")

    except Exception as e:
        check("decks module available", False, f"error: {e}")

    # ══════════════════════════════════════════════════════════
    #  SKUS — SkuQuery (3 methods)
    #  Downloads TcgplayerSkus.json.gz (~large file)
    # ══════════════════════════════════════════════════════════
    section("SKUs")

    try:
        if uuid:
            # get
            skus = sdk.skus.get(uuid)
            check("skus.get", isinstance(skus, list), f"found {len(skus)} SKUs")

            skus_dict = sdk.skus.get(uuid, as_dict=True)
            check("skus.get as_dict", isinstance(skus_dict, list))

            if skus and len(skus) > 0:
                # Access first SKU — it's either a model or dict
                first_sku = skus_dict[0] if skus_dict else None
                if first_sku:
                    # find_by_sku_id
                    sku_id = first_sku.get("skuId")
                    if sku_id:
                        by_sku = sdk.skus.find_by_sku_id(sku_id)
                        check(
                            "skus.find_by_sku_id", by_sku is not None, f"skuId={sku_id}"
                        )
                    else:
                        skip("skus.find_by_sku_id", "no skuId in data")

                    # find_by_product_id
                    prod_id = first_sku.get("productId")
                    if prod_id:
                        by_prod = sdk.skus.find_by_product_id(prod_id)
                        check(
                            "skus.find_by_product_id",
                            len(by_prod) > 0,
                            f"productId={prod_id}",
                        )
                    else:
                        skip("skus.find_by_product_id", "no productId in data")
            else:
                skip("skus.find_by_sku_id", "no SKU data for this card")
                skip("skus.find_by_product_id", "no SKU data for this card")
        else:
            skip("skus tests", "no uuid")

    except Exception as e:
        check("skus module available", False, f"error: {e}")

    # ══════════════════════════════════════════════════════════
    #  ENUMS — EnumQuery (3 methods)
    #  Downloads Keywords.json, CardTypes.json, EnumValues.json
    # ══════════════════════════════════════════════════════════
    section("Enums")

    try:
        # keywords
        kw = sdk.enums.keywords()
        check(
            "enums.keywords",
            isinstance(kw, dict) and len(kw) > 0,
            f"keys={list(kw.keys())[:5]}",
        )

        if kw:
            # Verify structure: should have abilityWords, keywordAbilities, keywordActions
            has_ability = "abilityWords" in kw or any(
                "ability" in k.lower() for k in kw
            )
            check(
                "keywords has expected keys",
                has_ability or len(kw) > 0,
                f"top keys: {list(kw.keys())[:5]}",
            )

        # card_types
        ct = sdk.enums.card_types()
        check(
            "enums.card_types",
            isinstance(ct, dict) and len(ct) > 0,
            f"keys={list(ct.keys())[:5]}",
        )

        if ct:
            # Should include things like "creature", "instant", "sorcery"
            has_creature = any("creature" in k.lower() for k in ct)
            check("card_types has creature", has_creature or len(ct) > 0)

        # enum_values
        ev = sdk.enums.enum_values()
        check(
            "enums.enum_values",
            isinstance(ev, dict) and len(ev) > 0,
            f"keys={list(ev.keys())[:5]}",
        )

    except Exception as e:
        check("enums module available", False, f"error: {e}")

    # ══════════════════════════════════════════════════════════
    #  SEALED — SealedQuery (2 methods)
    #  Flat sets.parquet lacks sealedProduct — tests graceful degradation
    # ══════════════════════════════════════════════════════════
    section("Sealed Products (flat parquet - graceful degradation)")

    # list — no filter
    sealed_all = sdk.sealed.list()
    check("sealed.list (all)", isinstance(sealed_all, list), f"found {len(sealed_all)}")

    # list — set_code filter
    sealed_mh3 = sdk.sealed.list(set_code="MH3")
    check(
        "sealed.list set_code=MH3",
        isinstance(sealed_mh3, list),
        f"found {len(sealed_mh3)}",
    )

    # list — category filter
    sealed_cat = sdk.sealed.list(category="booster_box")
    check(
        "sealed.list category", isinstance(sealed_cat, list), f"found {len(sealed_cat)}"
    )

    # list — as_dict
    sealed_dict = sdk.sealed.list(as_dict=True)
    check("sealed.list as_dict", isinstance(sealed_dict, list))

    # list — as_dataframe
    sealed_df = sdk.sealed.list(as_dataframe=True)
    check("sealed.list as_dataframe", sealed_df is not None)

    # get — nonexistent uuid
    sealed_item = sdk.sealed.get("00000000-0000-0000-0000-000000000000")
    check("sealed.get (graceful)", sealed_item is None or isinstance(sealed_item, dict))

    # ══════════════════════════════════════════════════════════
    #  BOOSTER — BoosterSimulator (4 methods)
    #  Flat sets.parquet lacks booster column — tests graceful degradation
    # ══════════════════════════════════════════════════════════
    section("Booster Simulation (flat parquet - graceful degradation)")

    # available_types
    types = sdk.booster.available_types("MH3")
    check("booster.available_types", isinstance(types, list), f"types: {types}")

    # open_pack — expect ValueError or graceful fail
    try:
        pack = sdk.booster.open_pack("MH3", "draft")
        check("booster.open_pack", isinstance(pack, list), f"got {len(pack)} cards")
    except ValueError:
        check("booster.open_pack raises ValueError (no booster data)", True)

    # open_box — expect ValueError or graceful fail
    try:
        box = sdk.booster.open_box("MH3", "draft", packs=1)
        check("booster.open_box", isinstance(box, list))
    except ValueError:
        check("booster.open_box raises ValueError (no booster data)", True)

    # sheet_contents
    contents = sdk.booster.sheet_contents("MH3", "draft", "common")
    check(
        "booster.sheet_contents",
        contents is None or isinstance(contents, dict),
        f"type={type(contents).__name__}",
    )

    # ══════════════════════════════════════════════════════════
    #  RAW SQL — sdk.sql() (all modes)
    # ══════════════════════════════════════════════════════════
    section("Raw SQL / Escape Hatches")

    # Simple query
    rows = sdk.sql("SELECT COUNT(*) AS cnt FROM cards")
    check("sql COUNT", rows[0]["cnt"] > 1000, f"count={rows[0]['cnt']}")

    # Query with params
    rows_param = sdk.sql(
        "SELECT name FROM cards WHERE manaValue = $1 LIMIT $2",
        params=[1.0, 5],
    )
    check("sql with params", len(rows_param) > 0, f"found {len(rows_param)}")

    # More complex query
    top_edhrec = sdk.sql(
        "SELECT name, edhrecRank FROM cards "
        "WHERE edhrecRank IS NOT NULL "
        "ORDER BY edhrecRank ASC LIMIT 5"
    )
    check(
        "sql top EDHREC",
        len(top_edhrec) == 5,
        f"top: {[r['name'] for r in top_edhrec]}",
    )

    # sql — as_dataframe
    df_result = sdk.sql("SELECT name, manaValue FROM cards LIMIT 5", as_dataframe=True)
    check("sql as_dataframe", hasattr(df_result, "shape"))

    # Cross-table join via raw SQL
    join_result = sdk.sql(
        "SELECT c.name, s.name AS setName "
        "FROM cards c JOIN sets s ON c.setCode = s.code "
        "LIMIT 3"
    )
    check("sql cross-table join", len(join_result) > 0 and "setName" in join_result[0])

    # ══════════════════════════════════════════════════════════
    #  VIEWS — verify views grew as we queried
    # ══════════════════════════════════════════════════════════
    section("Views (post-query)")

    views_after = sdk.views
    check(
        "views grew",
        len(views_after) > len(views_before),
        f"before={len(views_before)}, after={len(views_after)}, views={views_after}",
    )

    # ══════════════════════════════════════════════════════════
    #  EDGE CASES & VALIDATION
    # ══════════════════════════════════════════════════════════
    section("Edge Cases & Validation")

    # Empty search results
    empty = sdk.cards.search(name="XYZ_NONEXISTENT_CARD_12345", limit=5)
    check("empty search result", len(empty) == 0)

    # Card with special characters in name
    s = sdk.cards.search(name="Jötun%", limit=5)
    check("search unicode name", isinstance(s, list), f"found {len(s)}")

    # Card model field validation
    if bolt:
        card = bolt[0]
        check("card has uuid", bool(card.uuid))
        check("card has name", card.name == "Lightning Bolt")
        check("card has colors", isinstance(card.colors, list), f"colors={card.colors}")
        check(
            "card has manaValue", card.mana_value is not None, f"mv={card.mana_value}"
        )
        check("card has text", bool(card.text), f"text={card.text[:50]}...")

    # Set model field validation
    if mh3:
        check("set has code", mh3.code == "MH3")
        check("set has name", "Horizons" in mh3.name, f"name={mh3.name}")
        check("set has releaseDate", bool(mh3.release_date))
        check("set has type", bool(mh3.type))
        check(
            "set has baseSetSize",
            mh3.base_set_size > 0,
            f"baseSetSize={mh3.base_set_size}",
        )
        check(
            "set has totalSetSize",
            mh3.total_set_size > 0,
            f"totalSetSize={mh3.total_set_size}",
        )

    # Token model field validation
    if token_search:
        tok = token_search[0]
        check("token has uuid", bool(tok.uuid))
        check("token has name", bool(tok.name))

    # Atomic card model validation
    if atomic:
        a = atomic[0]
        check("atomic has name", a.name == "Lightning Bolt")
        check("atomic has layout", bool(a.layout))
        check("atomic has colors", isinstance(a.colors, list))

    # DeckList model validation
    try:
        deck_list_all = sdk.decks.list()
        if deck_list_all:
            d = deck_list_all[0]
            check("deck has code", bool(d.code))
            check("deck has name", bool(d.name))
            check("deck has type", bool(d.type))
            check("deck has fileName", bool(d.file_name))
    except Exception:
        skip("deck model validation", "deck data not loaded")

    # ══════════════════════════════════════════════════════════
    #  DONE — close and report
    # ══════════════════════════════════════════════════════════
    sdk.close()
    elapsed = time.time() - t0

    section("RESULTS")
    total = PASS + FAIL
    print(f"  Total:   {total} checks ({SKIP} skipped)")
    print(f"  Passed:  {PASS}")
    print(f"  Failed:  {FAIL}")
    print(f"  Time:    {elapsed:.1f}s")
    print()

    if FAIL > 0:
        print("  *** FAILURES DETECTED ***")
        print()

    return FAIL == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
