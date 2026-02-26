#!/usr/bin/env python3
"""Sealed EV Calculator — Estimate booster pack expected value using MTGJSON data.

Simulates opening N booster packs for a given set, prices every card pulled and reports expected value statistics.

Usage:
    python examples/sealed_ev.py MH3
    python examples/sealed_ev.py MH3 --packs 500 --booster-type collector
    python examples/sealed_ev.py DSK --provider tcgplayer --seed 42

Requires: mtgjson-sdk (pip install mtgjson-sdk)
"""

from __future__ import annotations

import argparse
import random
import statistics
import sys
import time
from typing import Any

from mtgjson_sdk import MtgjsonSDK

# Log-scale bucket boundaries for the pack value histogram
HISTOGRAM_BUCKETS = [0, 1, 2, 5, 10, 25, 50, 100]
BAR_WIDTH = 40
BAR_CHAR = "#"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Estimate booster pack expected value using MTGJSON data.",
    )
    parser.add_argument("set_code", help="Set code (e.g. MH3, DSK, BLB)")
    parser.add_argument(
        "--packs",
        type=int,
        default=1000,
        help="Number of packs to simulate (default: 1000)",
    )
    parser.add_argument(
        "--booster-type",
        default="play",
        help="Booster type (default: play)",
    )
    parser.add_argument(
        "--provider",
        default="tcgplayer",
        help="Price provider (default: tcgplayer)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducibility",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

PRICE_SQL = """\
SELECT c.uuid, p.price
FROM cards c
JOIN all_prices_today p ON c.uuid = p.uuid
WHERE c.setCode = $1
  AND p.provider = $2
  AND p.finish = $3
  AND p.price_type = 'retail'
  AND p.date = (SELECT MAX(p2.date) FROM all_prices_today p2)
"""


def fetch_price_maps(
    sdk: MtgjsonSDK,
    set_code: str,
    provider: str,
) -> tuple[dict[str, float], dict[str, float]]:
    """Batch-fetch latest prices for all cards in a set.

    Returns (normal_prices, foil_prices) dicts mapping uuid -> price.
    Cards from foil booster sheets are priced using the foil map;
    everything else uses the normal map.
    """
    # Warm up views so raw SQL works
    _ = sdk.cards.count()
    _ = sdk.prices.today("__warmup__")

    normal = sdk.sql(PRICE_SQL, [set_code, provider, "normal"])
    normal_map = {r["uuid"]: r["price"] for r in normal}

    foil = sdk.sql(PRICE_SQL, [set_code, provider, "foil"])
    foil_map = {r["uuid"]: r["price"] for r in foil}

    return normal_map, foil_map


# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------


def simulate_packs(
    sdk: MtgjsonSDK,
    set_code: str,
    booster_type: str,
    n_packs: int,
) -> list[list[dict[str, Any]]]:
    packs: list[list[dict[str, Any]]] = []
    for i in range(n_packs):
        if i % 100 == 0:
            print(f"\r  Simulating... {i}/{n_packs}", end="", flush=True)
        pack = sdk.booster.open_pack(set_code, booster_type, as_dict=True)
        packs.append(pack)
    print(f"\r  Simulating... {n_packs}/{n_packs} done.     ")
    return packs


def price_card(
    card: dict[str, Any],
    normal_prices: dict[str, float],
    foil_prices: dict[str, float],
) -> float:
    """Look up the price for a single card, respecting its foil status."""
    uuid = card["uuid"]
    is_foil = card.get("isFoil", False)

    if is_foil:
        # Prefer foil price, fall back to normal
        return foil_prices.get(uuid, normal_prices.get(uuid, 0.0))
    else:
        # Prefer normal price, fall back to foil
        return normal_prices.get(uuid, foil_prices.get(uuid, 0.0))


def price_pack(
    pack: list[dict[str, Any]],
    normal_prices: dict[str, float],
    foil_prices: dict[str, float],
) -> float:
    return round(sum(price_card(c, normal_prices, foil_prices) for c in pack), 2)


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------


def compute_rarity_ev(
    all_packs: list[list[dict[str, Any]]],
    normal_prices: dict[str, float],
    foil_prices: dict[str, float],
    n_packs: int,
) -> list[dict[str, Any]]:
    """Average per-pack value contribution by rarity."""
    totals: dict[str, float] = {}
    counts: dict[str, int] = {}

    for pack in all_packs:
        for card in pack:
            rarity = card.get("rarity", "unknown")
            price = price_card(card, normal_prices, foil_prices)
            totals[rarity] = totals.get(rarity, 0.0) + price
            counts[rarity] = counts.get(rarity, 0) + 1

    # Sort by value descending
    result = []
    for rarity in sorted(totals, key=lambda r: totals[r], reverse=True):
        result.append(
            {
                "rarity": rarity,
                "avg_per_pack": totals[rarity] / n_packs,
                "total": totals[rarity],
                "count": counts[rarity],
            }
        )
    return result


def compute_top_cards(
    all_packs: list[list[dict[str, Any]]],
    normal_prices: dict[str, float],
    foil_prices: dict[str, float],
    n_packs: int,
    top_n: int = 10,
) -> list[dict[str, Any]]:
    """Top money cards by price, with pull rates."""
    uuid_pulls: dict[str, int] = {}
    uuid_info: dict[str, dict[str, Any]] = {}

    for pack in all_packs:
        for card in pack:
            uuid = card["uuid"]
            is_foil = card.get("isFoil", False)
            # Track foil and non-foil pulls separately for accurate pricing
            key = (uuid, is_foil)
            uuid_pulls[key] = uuid_pulls.get(key, 0) + 1
            if key not in uuid_info:
                price = price_card(card, normal_prices, foil_prices)
                uuid_info[key] = {
                    "name": card["name"],
                    "rarity": card.get("rarity", "?"),
                    "price": price,
                    "is_foil": is_foil,
                }

    ranked = sorted(uuid_info.items(), key=lambda x: x[1]["price"], reverse=True)

    result = []
    for key, info in ranked[:top_n]:
        if info["price"] <= 0:
            continue
        pulls = uuid_pulls[key]
        result.append(
            {
                "name": info["name"],
                "rarity": info["rarity"],
                "price": info["price"],
                "is_foil": info["is_foil"],
                "pulls": pulls,
                "pull_rate_pct": pulls / n_packs * 100,
            }
        )
    return result


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def print_header(
    set_name: str,
    set_code: str,
    booster_type: str,
    provider: str,
) -> None:
    title = f"Sealed EV Calculator - {set_name} ({set_code})"
    subtitle = f"Booster: {booster_type} | Provider: {provider}"
    width = max(len(title), len(subtitle)) + 6
    border = "=" * width
    print()
    print(f"  {border}")
    print(f"  {title:^{width}}")
    print(f"  {subtitle:^{width}}")
    print(f"  {border}")


def print_summary(
    pack_values: list[float],
    n_packs: int,
    total_cards: int,
    priced_count: int,
    total_unique: int,
) -> None:
    mean = statistics.mean(pack_values)
    median = statistics.median(pack_values)
    stdev = statistics.stdev(pack_values) if len(pack_values) > 1 else 0.0
    lo = min(pack_values)
    hi = max(pack_values)

    print()
    print(f"  Simulated {n_packs:,} packs ({total_cards:,} cards)")
    print(f"  Price coverage: {priced_count} / {total_unique} unique cards priced", end="")
    if total_unique > 0:
        print(f" ({priced_count / total_unique * 100:.1f}%)")
    else:
        print()

    print()
    print("  EXPECTED VALUE")
    print("  " + "-" * 38)
    print(f"  Mean:     ${mean:>7.2f}       Median:  ${median:.2f}")
    print(f"  Std Dev:  ${stdev:>7.2f}")
    print(f"  Min:      ${lo:>7.2f}       Max:     ${hi:.2f}")


def print_rarity_breakdown(rarity_data: list[dict[str, Any]], mean_ev: float) -> None:
    print()
    print("  EV BY RARITY")
    print("  " + "-" * 38)
    print(f"  {'Rarity':<12s} {'Avg/Pack':>10s}  {'% of EV':>8s}")

    for row in rarity_data:
        avg = row["avg_per_pack"]
        pct = (avg / mean_ev * 100) if mean_ev > 0 else 0
        print(f"  {row['rarity']:<12s} ${avg:>8.2f}  {pct:>7.1f}%")


def print_top_cards(top_cards: list[dict[str, Any]], n_packs: int) -> None:
    print()
    print("  TOP 10 MONEY CARDS")
    print("  " + "-" * 38)
    if not top_cards:
        print("  (no priced cards found)")
        return

    # Find the longest name for alignment
    max_name = max(len(c["name"]) for c in top_cards)
    max_name = min(max_name, 28)  # cap width

    print(f"  {'Card':<{max_name}s}  {'Price':>7s}  {'Pull %':>7s}")
    for card in top_cards:
        name = card["name"][:max_name]
        foil_tag = " *" if card["is_foil"] else ""
        print(
            f"  {name:<{max_name}s}{foil_tag}  "
            f"${card['price']:>6.2f}  "
            f"{card['pull_rate_pct']:>6.1f}%"
        )
    # Legend for foil indicator
    if any(c["is_foil"] for c in top_cards):
        print("  (* = foil slot)")


def print_histogram(pack_values: list[float]) -> None:
    n = len(pack_values)
    if n == 0:
        return

    # Count packs in each bucket
    counts = [0] * len(HISTOGRAM_BUCKETS)
    for v in pack_values:
        placed = False
        for i in range(len(HISTOGRAM_BUCKETS) - 1, 0, -1):
            if v >= HISTOGRAM_BUCKETS[i]:
                counts[i] += 1
                placed = True
                break
        if not placed:
            counts[0] += 1

    max_count = max(counts) if counts else 1

    print()
    print("  PACK VALUE DISTRIBUTION")
    print("  " + "-" * 38)

    for i, count in enumerate(counts):
        # Build label
        if i < len(HISTOGRAM_BUCKETS) - 1:
            label = f"${HISTOGRAM_BUCKETS[i]}-${HISTOGRAM_BUCKETS[i + 1]}"
        else:
            label = f"${HISTOGRAM_BUCKETS[i]}+"

        bar_len = int(count / max_count * BAR_WIDTH) if max_count > 0 else 0
        bar = BAR_CHAR * bar_len
        pct = count / n * 100
        print(f"  {label:>8s}  |{bar:<{BAR_WIDTH}s} {count:>5d} ({pct:5.1f}%)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()
    set_code = args.set_code.upper()

    if args.seed is not None:
        random.seed(args.seed)

    with MtgjsonSDK() as sdk:
        # -- Validate set --
        set_info = sdk.sets.get(set_code, as_dict=True)
        if not set_info:
            print(f"Error: Set '{set_code}' not found.")
            sys.exit(1)

        set_name = set_info.get("name", set_code)

        # -- Validate booster type --
        types = sdk.booster.available_types(set_code)
        if not types:
            print(f"Error: No booster data available for {set_name} ({set_code}).")
            print("  This set may not have booster products, or the data")
            print("  may not be available in the current MTGJSON build.")
            sys.exit(1)

        if args.booster_type not in types:
            print(
                f"Error: Booster type '{args.booster_type}' not available "
                f"for {set_code}."
            )
            print(f"  Available types: {', '.join(types)}")
            sys.exit(1)

        # -- Fetch prices --
        print(f"\n  Loading price data for {set_name}...")
        normal_prices, foil_prices = fetch_price_maps(
            sdk, set_code, args.provider
        )

        all_priced = set(normal_prices) | set(foil_prices)
        if not all_priced:
            print(
                f"  Warning: No price data found for {set_code} "
                f"(provider={args.provider}). All values will show $0.00."
            )

        # -- Simulate --
        t0 = time.time()
        all_packs = simulate_packs(sdk, set_code, args.booster_type, args.packs)
        sim_time = time.time() - t0

        # -- Price packs --
        pack_values = [
            price_pack(p, normal_prices, foil_prices) for p in all_packs
        ]

        # -- Stats --
        total_cards = sum(len(p) for p in all_packs)
        seen_uuids = {c["uuid"] for p in all_packs for c in p}
        priced_count = len(seen_uuids & all_priced)

        mean_ev = statistics.mean(pack_values)
        rarity_data = compute_rarity_ev(
            all_packs, normal_prices, foil_prices, args.packs
        )
        top_cards = compute_top_cards(
            all_packs, normal_prices, foil_prices, args.packs
        )

        # -- Report --
        print_header(set_name, set_code, args.booster_type, args.provider)
        print_summary(pack_values, args.packs, total_cards, priced_count, len(seen_uuids))
        print_rarity_breakdown(rarity_data, mean_ev)
        print_top_cards(top_cards, args.packs)
        print_histogram(pack_values)

        print()
        print(f"  Simulation completed in {sim_time:.1f}s")
        print()


if __name__ == "__main__":
    main()
