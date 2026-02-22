# mtgjson-sdk

A DuckDB-backed Python query client for [MTGJSON](https://mtgjson.com) card data. Auto-downloads Parquet data from the MTGJSON CDN and exposes the full Magic: The Gathering dataset through an ergonomic, fully-typed Python API.

## Install

TODO: PYPI STUFF

## Quick Start

```python
from mtgjson_sdk import MtgjsonSDK

with MtgjsonSDK() as sdk:
    # Search for cards
    bolts = sdk.cards.search(name="Lightning Bolt")
    print(f"Found {len(bolts)} printings of Lightning Bolt")

    # Get a specific set
    mh3 = sdk.sets.get("MH3")
    print(f"{mh3.name} -- {mh3.totalSetSize} cards")

    # Check format legality
    is_legal = sdk.legalities.is_legal(bolts[0].uuid, "modern")
    print(f"Modern legal: {is_legal}")

    # Find the cheapest printing
    cheapest = sdk.prices.cheapest_printing("Lightning Bolt")
    if cheapest:
        print(f"Cheapest: ${cheapest['price']} ({cheapest['setCode']})")

    # Raw SQL for anything else
    rows = sdk.sql("SELECT name, manaValue FROM cards WHERE manaValue = 0 LIMIT 5")
```

## Use Cases

### Price Tracking

```python
with MtgjsonSDK() as sdk:
    # Find the cheapest printing of any card
    cheapest = sdk.prices.cheapest_printing("Ragavan, Nimble Pilferer")

    # Price trend over time
    trend = sdk.prices.price_trend(
        cheapest["uuid"], provider="tcgplayer", finish="normal"
    )
    print(f"Range: ${trend['min_price']} - ${trend['max_price']}")
    print(f"Average: ${trend['avg_price']} over {trend['data_points']} data points")

    # Full price history with date range
    history = sdk.prices.history(
        cheapest["uuid"],
        provider="tcgplayer",
        date_from="2024-01-01",
        date_to="2024-12-31",
    )

    # Most expensive printings across the entire dataset
    priciest = sdk.prices.most_expensive_printings(limit=10)
```

### Deck Building Helper

```python
with MtgjsonSDK() as sdk:
    # Find modern-legal red creatures with CMC <= 2
    aggro_creatures = sdk.cards.search(
        colors=["R"],
        types="Creature",
        mana_value_lte=2.0,
        legal_in="modern",
        limit=50,
    )

    # Check what's banned
    banned = sdk.legalities.banned_in("modern")
    print(f"{len(banned)} cards banned in Modern")

    # Search by keyword ability
    flyers = sdk.cards.search(keyword="Flying", colors=["W", "U"], legal_in="standard")

    # Fuzzy search -- handles typos
    results = sdk.cards.search(fuzzy_name="Ligtning Bolt")  # still finds it!

    # Find cards by foreign-language name
    blitz = sdk.cards.search(localized_name="Blitzschlag")  # German for Lightning Bolt
```

### Collection Management

```python
with MtgjsonSDK() as sdk:
    # Cross-reference by Scryfall ID
    cards = sdk.identifiers.find_by_scryfall_id("f7a21fe4-...")

    # Look up by TCGPlayer product ID
    cards = sdk.identifiers.find_by_tcgplayer_id("12345")

    # Get all identifiers for a card (Scryfall, TCGPlayer, MTGO, Arena, etc.)
    all_ids = sdk.identifiers.get_identifiers("card-uuid-here")

    # Export to a standalone DuckDB file for offline analysis
    sdk.export_db("my_collection.duckdb")
    # Now query with: duckdb my_collection.duckdb "SELECT * FROM cards LIMIT 5"
```

### Discord Bot / Web API

```python
from mtgjson_sdk import AsyncMtgjsonSDK

# FastAPI example
from fastapi import FastAPI

app = FastAPI()
sdk = AsyncMtgjsonSDK()

@app.get("/card/{name}")
async def get_card(name: str):
    cards = await sdk.run(sdk.inner.cards.get_by_name, name)
    return [c.model_dump() for c in cards]

@app.on_event("startup")
async def refresh_data():
    """Check for new MTGJSON data on startup."""
    stale = sdk.inner.refresh()
    if stale:
        print("New MTGJSON data available -- cache refreshed")

@app.on_event("shutdown")
async def shutdown():
    await sdk.close()
```

### Booster Pack Simulation

```python
with MtgjsonSDK() as sdk:
    # See what booster types are available
    types = sdk.booster.available_types("MH3")  # ["draft", "collector", ...]

    # Open a single draft pack
    pack = sdk.booster.open_pack("MH3", "draft")
    for card in pack:
        print(f"  {card.name} ({card.rarity})")

    # Open an entire box
    box = sdk.booster.open_box("MH3", "draft", packs=36)
    print(f"Opened {len(box)} packs, {sum(len(p) for p in box)} total cards")
```

## API Reference

### Cards

```python
sdk.cards.get_by_uuid("uuid")              # -> CardSet | None
sdk.cards.get_by_uuids(["uuid1", "uuid2"]) # -> list[CardSet] (batch lookup)
sdk.cards.get_by_name("Lightning Bolt")     # -> list[CardSet]
sdk.cards.search(
    name="Lightning%",                      # name pattern (% = wildcard)
    fuzzy_name="Ligtning Bolt",             # typo-tolerant (Jaro-Winkler)
    localized_name="Blitzschlag",           # foreign-language name search
    colors=["R"],                           # cards containing these colors
    color_identity=["R", "U"],              # filter by color identity
    legal_in="modern",                      # format legality
    rarity="rare",                          # rarity filter
    mana_value=1.0,                         # exact mana value
    mana_value_lte=3.0,                     # mana value range
    mana_value_gte=1.0,
    text="damage",                          # rules text search
    text_regex=r"deals? \d+ damage",        # regex rules text search
    types="Creature",                       # type line search
    artist="Christopher Moeller",           # artist name search
    keyword="Flying",                       # keyword ability
    is_promo=False,                         # promo status
    availability="paper",                   # paper, mtgo
    language="English",                     # language filter
    layout="normal",                        # card layout
    set_code="MH3",                         # filter by set
    set_type="expansion",                   # set type (joins sets table)
    power="3", toughness="3",               # P/T filter
    limit=100, offset=0,                    # pagination
)
sdk.cards.get_printings("Lightning Bolt")   # all printings across sets
sdk.cards.get_atomic("Lightning Bolt")      # oracle data (no printing info)
sdk.cards.get_atomic("Fire")               # also works with face names (split/MDFC)
sdk.cards.find_by_scryfall_id("...")        # cross-reference
sdk.cards.random(5)                         # random cards
sdk.cards.count()                           # total count
sdk.cards.count(setCode="MH3", rarity="rare")  # filtered count
```

### Tokens

```python
sdk.tokens.get_by_uuid("uuid")             # -> CardToken | None
sdk.tokens.get_by_name("Soldier Token")     # -> list[CardToken]
sdk.tokens.search(name="%Token", set_code="MH3", colors=["W"])
sdk.tokens.for_set("MH3")                  # all tokens for a set
sdk.tokens.count()
```

### Sets

```python
sdk.sets.get("MH3")                        # -> SetList | None
sdk.sets.list(set_type="expansion")         # -> list[SetList]
sdk.sets.search(name="Horizons", release_year=2024)
sdk.sets.count()
```

### Identifiers

```python
sdk.identifiers.find_by_scryfall_id("...")
sdk.identifiers.find_by_tcgplayer_id("...")
sdk.identifiers.find_by_mtgo_id("...")
sdk.identifiers.find_by_mtgo_foil_id("...")
sdk.identifiers.find_by_mtg_arena_id("...")
sdk.identifiers.find_by_multiverse_id("...")
sdk.identifiers.find_by_mcm_id("...")
sdk.identifiers.find_by_card_kingdom_id("...")
sdk.identifiers.find_by_card_kingdom_foil_id("...")
sdk.identifiers.find_by_cardsphere_id("...")
sdk.identifiers.find_by_scryfall_oracle_id("...")
sdk.identifiers.find_by_scryfall_illustration_id("...")
sdk.identifiers.find_by("scryfallId", "...")  # generic lookup
sdk.identifiers.get_identifiers("uuid")       # all IDs for a card
```

### Legalities

```python
sdk.legalities.formats_for_card("uuid")    # -> {"modern": "Legal", ...}
sdk.legalities.legal_in("modern")          # all modern-legal cards
sdk.legalities.is_legal("uuid", "modern")  # -> bool
sdk.legalities.banned_in("modern")         # banned cards
sdk.legalities.restricted_in("vintage")    # restricted cards
sdk.legalities.suspended_in("historic")    # suspended cards
sdk.legalities.not_legal_in("standard")    # not-legal cards
```

### Prices

```python
sdk.prices.get("uuid")                     # full nested price data
sdk.prices.today("uuid", provider="tcgplayer", finish="foil")  # latest prices
sdk.prices.history("uuid", provider="tcgplayer", date_from="2024-01-01")
sdk.prices.price_trend("uuid", provider="tcgplayer", finish="normal")  # min/max/avg
sdk.prices.cheapest_printing("Lightning Bolt")   # cheapest printing by name
sdk.prices.cheapest_printings(limit=10)          # cheapest cards overall
sdk.prices.most_expensive_printings(limit=10)    # most expensive cards
```

### Decks

```python
sdk.decks.list(set_code="MH3")
sdk.decks.search(name="Eldrazi")
```

### Sealed Products

```python
sdk.sealed.list(set_code="MH3")
sdk.sealed.get("uuid")                    # efficient UNNEST lookup
```

### SKUs

```python
sdk.skus.get("uuid")                       # TCGPlayer SKUs for a card
sdk.skus.find_by_sku_id(123456)
sdk.skus.find_by_product_id(789)
```

### Booster Simulation

```python
sdk.booster.available_types("MH3")         # -> ["draft", "collector"]
sdk.booster.open_pack("MH3", "draft")      # -> list[CardSet]
sdk.booster.open_box("MH3", packs=36)      # -> list[list[CardSet]]
sdk.booster.sheet_contents("MH3", "draft", "common")  # card weights
```

### Enums

```python
sdk.enums.keywords()                       # -> Keywords
sdk.enums.card_types()                     # -> CardTypes
sdk.enums.enum_values()                    # all enum values
```

### Metadata & Utilities

```python
sdk.meta                                   # -> {"data": {"version": "...", "date": "..."}}
sdk.views                                  # -> ["cards", "sets", ...] registered views
sdk.refresh()                              # check for new data, reset if stale -> bool
sdk.export_db("output.duckdb")             # export to persistent DuckDB file
sdk.close()                                # release resources
```

## Advanced Usage

### Async Support

`AsyncMtgjsonSDK` wraps the sync client in a thread pool executor, making it safe to use from async frameworks without blocking the event loop. DuckDB releases the GIL during query execution, so thread pool concurrency works well.

```python
from mtgjson_sdk import AsyncMtgjsonSDK

async with AsyncMtgjsonSDK(max_workers=4) as sdk:
    # Run any sync method asynchronously
    cards = await sdk.run(sdk.inner.cards.search, name="Lightning%")
    sets = await sdk.run(sdk.inner.sets.list, set_type="masters")

    # Raw SQL shortcut
    result = await sdk.sql("SELECT COUNT(*) FROM cards")
```

### DataFrame Output

Every query method supports `as_dataframe=True` to return a Polars DataFrame (requires `pip install mtgjson-sdk[polars]`):

```python
import polars as pl

with MtgjsonSDK() as sdk:
    # Get a DataFrame of all Modern-legal creatures
    df = sdk.cards.search(legal_in="modern", types="Creature", limit=5000, as_dataframe=True)

    # Analyze with Polars
    avg_by_color = (
        df.explode("colors")
        .group_by("colors")
        .agg(pl.col("manaValue").mean().alias("avg_cmc"))
        .sort("avg_cmc")
    )
    print(avg_by_color)
```

### Database Export

Export all loaded data to a standalone DuckDB file that can be queried without the SDK:

```python
with MtgjsonSDK() as sdk:
    # Touch the query modules you want exported
    _ = sdk.cards.count()
    _ = sdk.sets.count()

    # Export to file
    sdk.export_db("mtgjson.duckdb")

# Now use it standalone:
# $ duckdb mtgjson.duckdb "SELECT name, setCode FROM cards LIMIT 10"
```

### Auto-Refresh for Long-Running Services

The `refresh()` method checks the CDN for new MTGJSON releases. If a newer version is available, it clears internal state so the next query re-downloads fresh data:

```python
sdk = MtgjsonSDK()

# In a scheduled task or health check:
if sdk.refresh():
    print("New MTGJSON data detected -- cache refreshed")
```

### Custom Cache Directory & Progress

```python
from pathlib import Path

def on_progress(filename: str, downloaded: int, total: int):
    pct = (downloaded / total * 100) if total else 0
    print(f"\r{filename}: {pct:.1f}%", end="", flush=True)

sdk = MtgjsonSDK(
    cache_dir=Path("/data/mtgjson-cache"),
    timeout=300.0,
    on_progress=on_progress,
)
```

### Raw SQL

All user input goes through DuckDB parameter binding (`$1`, `$2`, ...) to prevent SQL injection:

```python
with MtgjsonSDK() as sdk:
    # Ensure views are registered before querying
    _ = sdk.cards.count()

    # Parameterized queries
    rows = sdk.sql(
        "SELECT name, setCode, rarity FROM cards WHERE manaValue <= $1 AND rarity = $2",
        [2, "mythic"],
    )

    # Complex analytics
    rows = sdk.sql("""
        SELECT setCode, COUNT(*) as card_count, AVG(manaValue) as avg_cmc
        FROM cards
        GROUP BY setCode
        ORDER BY card_count DESC
        LIMIT 10
    """)
```

## Architecture

```
MTGJSON CDN (Parquet + JSON files)
        |
        | auto-download on first access
        v
Local Cache (platform-specific directory)
        |
        | lazy view registration
        v
DuckDB In-Memory Database
        |
        | parameterized SQL queries
        v
Typed Python API (Pydantic models / dicts / Polars DataFrames)
```

**How it works:**

1. **Auto-download**: On first use, the SDK downloads ~15 Parquet files and ~7 JSON files from the MTGJSON CDN to a platform-specific cache directory (`~/.cache/mtgjson-sdk` on Linux, `~/Library/Caches/mtgjson-sdk` on macOS, `AppData/Local/mtgjson-sdk` on Windows).

2. **Lazy loading**: DuckDB views are registered on-demand -- accessing `sdk.cards` triggers the cards view, `sdk.prices` triggers price data loading, etc. Only the data you use gets loaded into memory.

3. **Schema adaptation**: The SDK auto-detects array columns in parquet files using a hybrid heuristic (static baseline + dynamic plural detection + blocklist), so it adapts to upstream MTGJSON schema changes without code updates.

4. **Legality UNPIVOT**: Format legality columns are dynamically detected from the parquet schema and UNPIVOTed to `(uuid, format, status)` rows -- automatically scales to new formats.

5. **Price flattening**: Deeply nested JSON price data is streamed to NDJSON and bulk-loaded into DuckDB, minimizing memory overhead.

## Development

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

### Setup

```bash
git clone https://github.com/the-muppet2/mtgjson-sdk.git
cd mtgjson-sdk
uv sync --group dev
```

### Running Tests

```bash
uv run pytest
```

### Code Style

```bash
uv run ruff check src/ tests/
uv run ruff format src/ tests/
```

## License

MIT
