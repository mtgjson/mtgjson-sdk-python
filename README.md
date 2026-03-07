# mtgjson-sdk

A high-performance, DuckDB-backed Python query client for [MTGJSON](https://mtgjson.com).

Unlike traditional SDKs that rely on rate-limited REST APIs, `mtgjson-sdk` implements a local data warehouse architecture. It synchronizes optimized Parquet data from the MTGJSON CDN to your local machine, utilizing DuckDB to execute complex analytics, fuzzy searches, and booster simulations with sub-millisecond latency.

## Key Features

*   **Vectorized Execution**: Powered by DuckDB for high-speed OLAP queries on the full MTG dataset.
*   **Offline-First**: Data is cached locally, allowing for full functionality without an active internet connection.
*   **Fuzzy Search**: Built-in Jaro-Winkler similarity matching to handle typos and approximate name lookups.
*   **Data Science Integration**: Native support for Polars DataFrames and Arrow-based zero-copy data transfer.
*   **Fully Async**: Thread-safe async wrapper designed for high-concurrency environments like FastAPI or Discord bots.
*   **Booster Simulation**: Accurate pack opening logic using official MTGJSON weights and sheet configurations.

## Install

```bash
pip install mtgjson-sdk
```

With optional extras:

```bash
pip install mtgjson-sdk[polars]   # Polars DataFrame support
pip install mtgjson-sdk[all]      # All optional dependencies
```

## Quick Start

```python
from mtgjson_sdk import MtgjsonSDK

with MtgjsonSDK() as sdk:
    # Search for cards (returns Pydantic models)
    bolts = sdk.cards.search(name="Lightning Bolt")
    print(f"Found {len(bolts)} printings of Lightning Bolt")

    # Get set metadata
    mh3 = sdk.sets.get("MH3")
    print(f"{mh3.name} -- {mh3.totalSetSize} cards")

    # Check format legality
    if bolts:
        print(f"Modern legal: {sdk.legalities.is_legal(bolts[0].uuid, 'modern')}")

    # Find the cheapest printing
    cheapest = sdk.prices.cheapest_printing("Lightning Bolt")
    if cheapest:
        print(f"Cheapest: ${cheapest['price']} ({cheapest['setCode']})")

    # Performance tip: use as_dataframe=True for bulk analysis (1000+ rows)
    df = sdk.cards.search(set_code="MH3", as_dataframe=True)

    # Execute raw SQL with parameter binding
    rows = sdk.sql("SELECT name FROM cards WHERE manaValue = $1 LIMIT 5", [0])
```

## Architecture

By using DuckDB, the SDK leverages columnar storage and vectorized execution, making it significantly faster than SQLite or standard JSON parsing for MTG's relational dataset.

1.  **Synchronization**: On first use, the SDK lazily downloads Parquet and JSON files from the MTGJSON CDN to a platform-specific cache directory (`~/.cache/mtgjson-sdk` on Linux, `~/Library/Caches/mtgjson-sdk` on macOS, `AppData/Local/mtgjson-sdk` on Windows).
2.  **Virtual Schema**: DuckDB views are registered on-demand. Accessing `sdk.cards` registers the card view; accessing `sdk.prices` registers price data. You only pay the memory cost for the data you query.
3.  **Dynamic Adaptation**: The SDK introspects Parquet metadata to automatically handle schema changes, plural-column array conversion, and format legality unpivoting.
4.  **Materialization**: Queries return validated Pydantic models for individual record ergonomics, or Polars DataFrames for bulk processing.

## Use Cases

### Price Analytics

```python
with MtgjsonSDK() as sdk:
    # Find the cheapest printing of a card by name
    cheapest = sdk.prices.cheapest_printing("Ragavan, Nimble Pilferer")

    # Aggregate statistics (min, max, avg) for a specific card
    trend = sdk.prices.price_trend(
        cheapest["uuid"], provider="tcgplayer", finish="normal"
    )
    print(f"Range: ${trend['min_price']} - ${trend['max_price']}")
    print(f"Average: ${trend['avg_price']} over {trend['data_points']} data points")

    # Historical price lookup with date filtering
    history = sdk.prices.history(
        cheapest["uuid"],
        provider="tcgplayer",
        date_from="2024-01-01",
        date_to="2024-12-31",
    )

    # Top 10 most expensive printings across the entire dataset
    priciest = sdk.prices.most_expensive_printings(limit=10)
```

### Advanced Card Search

The `search()` method supports ~20 composable filters that can be combined freely:

```python
with MtgjsonSDK() as sdk:
    # Complex filters: Modern-legal red creatures with CMC <= 2
    aggro = sdk.cards.search(
        colors=["R"],
        types="Creature",
        mana_value_lte=2.0,
        legal_in="modern",
        limit=50,
    )

    # Typo-tolerant fuzzy search (Jaro-Winkler similarity)
    results = sdk.cards.search(fuzzy_name="Ligtning Bolt")  # still finds it

    # Rules text search using regular expressions
    burn = sdk.cards.search(text_regex=r"deals? \d+ damage to any target")

    # Search by keyword ability across formats
    flyers = sdk.cards.search(keyword="Flying", colors=["W", "U"], legal_in="standard")

    # Find cards by foreign-language name
    blitz = sdk.cards.search(localized_name="Blitzschlag")  # German for Lightning Bolt
```

<details>
<summary>All <code>search()</code> parameters</summary>

| Parameter | Type | Description |
|---|---|---|
| `name` | `str` | Name pattern (`%` = wildcard) |
| `fuzzy_name` | `str` | Typo-tolerant Jaro-Winkler match |
| `localized_name` | `str` | Foreign-language name search |
| `colors` | `list[str]` | Cards containing these colors |
| `color_identity` | `list[str]` | Color identity filter |
| `legal_in` | `str` | Format legality |
| `rarity` | `str` | Rarity filter |
| `mana_value` | `float` | Exact mana value |
| `mana_value_lte` | `float` | Mana value upper bound |
| `mana_value_gte` | `float` | Mana value lower bound |
| `text` | `str` | Rules text substring |
| `text_regex` | `str` | Rules text regex |
| `types` | `str` | Type line search |
| `artist` | `str` | Artist name |
| `keyword` | `str` | Keyword ability |
| `is_promo` | `bool` | Promo status |
| `availability` | `str` | `"paper"` or `"mtgo"` |
| `language` | `str` | Language filter |
| `layout` | `str` | Card layout |
| `set_code` | `str` | Set code |
| `set_type` | `str` | Set type (joins sets table) |
| `power` | `str` | Power filter |
| `toughness` | `str` | Toughness filter |
| `limit` / `offset` | `int` | Pagination |
| `as_dataframe` | `bool` | Return Polars DataFrame |

</details>

### Collection & Cross-Reference

```python
with MtgjsonSDK() as sdk:
    # Cross-reference by any external ID system
    cards = sdk.identifiers.find_by_scryfall_id("f7a21fe4-...")
    cards = sdk.identifiers.find_by_tcgplayer_id("12345")
    cards = sdk.identifiers.find_by_mtgo_id("67890")

    # Get all external identifiers for a card
    all_ids = sdk.identifiers.get_identifiers("card-uuid-here")
    # -> Scryfall, TCGPlayer, MTGO, Arena, Cardmarket, Card Kingdom, Cardsphere, ...

    # TCGPlayer SKU variants (foil, etched, etc.)
    skus = sdk.skus.get("card-uuid-here")

    # Export to a standalone DuckDB file for offline analysis
    sdk.export_db("my_collection.duckdb")
    # Now query with: duckdb my_collection.duckdb "SELECT * FROM cards LIMIT 5"
```

### Booster Simulation

```python
with MtgjsonSDK() as sdk:
    # See available booster types for a set
    types = sdk.booster.available_types("MH3")  # ["draft", "collector", ...]

    # Open a single draft pack using official set weights
    pack = sdk.booster.open_pack("MH3", "draft")
    for card in pack:
        print(f"  {card.name} ({card.rarity})")

    # Simulate opening a full box (36 packs)
    box = sdk.booster.open_box("MH3", "draft", packs=36)
    print(f"Opened {len(box)} packs, {sum(len(p) for p in box)} total cards")
```

## API Reference

### Core Data

```python
# Cards
sdk.cards.get_by_uuid("uuid")               # single card lookup
sdk.cards.get_by_uuids(["uuid1", "uuid2"])  # batch lookup
sdk.cards.get_by_name("Lightning Bolt")     # all printings of a name
sdk.cards.search(...)                       # composable filters (see above)
sdk.cards.get_printings("Lightning Bolt")   # all printings across sets
sdk.cards.get_atomic("Lightning Bolt")      # oracle data (no printing info)
sdk.cards.find_by_scryfall_id("...")        # cross-reference shortcut
sdk.cards.random(5)                         # random cards
sdk.cards.count()                           # total (or filtered with kwargs)

# Tokens
sdk.tokens.get_by_uuid("uuid")
sdk.tokens.get_by_name("Soldier Token")
sdk.tokens.search(name="%Token", set_code="MH3", colors=["W"])
sdk.tokens.for_set("MH3")

# Sets
sdk.sets.get("MH3")
sdk.sets.list(set_type="expansion")
sdk.sets.search(name="Horizons", release_year=2024)
```

### Playability

```python
# Legalities
sdk.legalities.formats_for_card("uuid")    # -> {"modern": "Legal", ...}
sdk.legalities.legal_in("modern")          # all modern-legal cards
sdk.legalities.is_legal("uuid", "modern")  # -> bool
sdk.legalities.banned_in("modern")         # also: restricted_in, suspended_in

# Decks & Sealed Products
sdk.decks.list(set_code="MH3")
sdk.decks.search(name="Eldrazi")
sdk.sealed.list(set_code="MH3")
sdk.sealed.get("uuid")
```

### Market & Identifiers

```python
# Prices
sdk.prices.get("uuid")                     # full nested price data
sdk.prices.today("uuid", provider="tcgplayer", finish="foil")
sdk.prices.history("uuid", provider="tcgplayer", date_from="2024-01-01")
sdk.prices.price_trend("uuid", provider="tcgplayer", finish="normal")
sdk.prices.cheapest_printing("Lightning Bolt")
sdk.prices.most_expensive_printings(limit=10)

# Identifiers (supports all major external ID systems)
sdk.identifiers.find_by_scryfall_id("...")
sdk.identifiers.find_by_tcgplayer_id("...")
sdk.identifiers.find_by_mtgo_id("...")
sdk.identifiers.find_by_mtg_arena_id("...")
sdk.identifiers.find_by_multiverse_id("...")
sdk.identifiers.find_by_mcm_id("...")
sdk.identifiers.find_by_card_kingdom_id("...")
sdk.identifiers.find_by("scryfallId", "...")  # generic lookup
sdk.identifiers.get_identifiers("uuid")       # all IDs for a card

# SKUs
sdk.skus.get("uuid")
sdk.skus.find_by_sku_id(123456)
sdk.skus.find_by_product_id(789)
```

### Booster & Enums

```python
sdk.booster.available_types("MH3")
sdk.booster.open_pack("MH3", "draft")
sdk.booster.open_box("MH3", packs=36)
sdk.booster.sheet_contents("MH3", "draft", "common")

sdk.enums.keywords()
sdk.enums.card_types()
sdk.enums.enum_values()
```

### System

```python
sdk.meta                                   # version and build date
sdk.views                                  # registered view names
sdk.refresh()                              # check CDN for new data -> bool
sdk.export_db("output.duckdb")             # export to persistent DuckDB file
sdk.sql(query, params)                     # raw parameterized SQL
sdk.close()                                # release resources
```

## Performance and Memory

When querying large datasets (thousands of cards), avoid returning Pydantic models. Instantiating tens of thousands of Python objects is CPU and memory intensive.

```python
# Returns a Polars DataFrame (zero-copy memory handoff from DuckDB)
df = sdk.cards.search(name="%", as_dataframe=True)

# Analysis runs in C++/Rust via Polars -- not Python
avg_cmc = df.select(pl.col("manaValue").mean())
```

## Advanced Usage

### Async Frameworks (FastAPI / Discord.py)

`AsyncMtgjsonSDK` wraps the sync client in a thread pool executor, making it safe to use from async frameworks without blocking the event loop. DuckDB releases the GIL during query execution, so thread pool concurrency works well.

```python
from mtgjson_sdk import AsyncMtgjsonSDK

async with AsyncMtgjsonSDK(max_workers=4) as sdk:
    cards = await sdk.run(sdk.inner.cards.search, name="Lightning%")
    count = await sdk.sql("SELECT COUNT(*) FROM cards")
```

### Auto-Refresh for Long-Running Services

```python
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

All user input goes through DuckDB parameter binding (`$1`, `$2`, ...):

```python
with MtgjsonSDK() as sdk:
    # Ensure views are registered before querying
    _ = sdk.cards.count()

    # Parameterized queries
    rows = sdk.sql(
        "SELECT name, setCode, rarity FROM cards WHERE manaValue <= $1 AND rarity = $2",
        [2, "mythic"],
    )
```

## Development

```bash
git clone https://github.com/mtgjson/mtgjson-sdk-python.git
cd mtgjson-sdk-python
uv sync --group dev
uv run pytest
```

### Code Style

```bash
uv run ruff check mtgjson_sdk/ tests/
uv run ruff format mtgjson_sdk/ tests/
```

## License

MIT
