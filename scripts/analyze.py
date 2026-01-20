#!/usr/bin/env python3
"""
Analyze historical bet data to find optimal parameters.

Uses cached data from SQLite - no API calls needed.
"""

import argparse
import os
import sqlite3
from collections import defaultdict
from dataclasses import dataclass

# DB is in data/ folder at project root
DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "odds_monitor.db"
)


@dataclass
class BetRecord:
    """A single bet record with all relevant data."""

    game_id: str
    sport: str
    strategy: str
    bet_team: str
    bet_spread: float
    opening_spread: float
    spread_change_pct: float
    final_margin: int | None
    covered: bool
    profit: float


def load_all_bets() -> list[BetRecord]:
    """Load all bets from BOTH simulated_bets (backfill) and bet_outcomes (live)."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    bets = []

    # 1. Load from simulated_bets (historical backfill data)
    cursor.execute("""
        SELECT 
            game_id,
            sport,
            strategy,
            bet_team,
            bet_spread,
            opening_spread,
            pct_change,
            final_margin,
            covered,
            profit
        FROM simulated_bets
        WHERE covered IS NOT NULL
        ORDER BY game_id
    """)

    for row in cursor.fetchall():
        bets.append(
            BetRecord(
                game_id=row["game_id"],
                sport=row["sport"],
                strategy=row["strategy"],
                bet_team=row["bet_team"],
                bet_spread=row["bet_spread"],
                opening_spread=row["opening_spread"],
                spread_change_pct=row["pct_change"] * 100,
                final_margin=row["final_margin"],
                covered=bool(row["covered"]),
                profit=row["profit"] or 0,
            )
        )

    simulated_count = len(bets)

    # 2. Load from bet_outcomes (live monitor data)
    cursor.execute("""
        SELECT 
            b.game_id,
            g.sport,
            b.bet_type as strategy,
            b.bet_team,
            b.bet_spread,
            b.opening_spread,
            b.pct_change,
            b.final_margin,
            b.covered,
            b.profit
        FROM bet_outcomes b
        JOIN games g ON b.game_id = g.id
        WHERE b.covered IS NOT NULL
        ORDER BY b.game_id
    """)

    for row in cursor.fetchall():
        bets.append(
            BetRecord(
                game_id=row["game_id"],
                sport=row["sport"] or "unknown",
                strategy=row["strategy"],
                bet_team=row["bet_team"],
                bet_spread=row["bet_spread"],
                opening_spread=row["opening_spread"],
                spread_change_pct=(row["pct_change"] or 0) * 100,
                final_margin=row["final_margin"],
                covered=bool(row["covered"]),
                profit=row["profit"] or 0,
            )
        )

    live_count = len(bets) - simulated_count

    conn.close()

    print(f"   Sources: {simulated_count} from backfill, {live_count} from live monitor")

    return bets


def analyze_by_threshold(bets: list[BetRecord]) -> None:
    """Find optimal threshold by testing many values."""
    print("\n" + "=" * 70)
    print("THRESHOLD OPTIMIZATION")
    print("=" * 70)
    print("\nTesting thresholds from 25% to 300% in 25% increments...")
    print()

    # Group bets by their actual spread change percentage
    results_by_threshold: dict[int, dict] = {}

    for threshold in range(25, 325, 25):
        # Filter bets that meet this threshold
        matching = [b for b in bets if b.spread_change_pct >= threshold]

        if not matching:
            continue

        wins = sum(1 for b in matching if b.covered)
        losses = sum(1 for b in matching if not b.covered)
        total = wins + losses

        if total == 0:
            continue

        win_rate = wins / total * 100
        profit = sum(b.profit for b in matching)
        roi = profit / (total * 110) * 100  # Assuming $110 to win $100

        results_by_threshold[threshold] = {
            "bets": total,
            "wins": wins,
            "win_rate": win_rate,
            "profit": profit,
            "roi": roi,
        }

    # Print results
    print(f"{'Threshold':<12} {'Bets':<6} {'Wins':<6} {'Win%':<8} {'Profit':<10} {'ROI':<8}")
    print("-" * 60)

    best_roi = -999
    best_threshold = 0

    for threshold, stats in sorted(results_by_threshold.items()):
        print(
            f"{threshold}%{'':<10} {stats['bets']:<6} {stats['wins']:<6} "
            f"{stats['win_rate']:.1f}%{'':<4} ${stats['profit']:<9.0f} "
            f"{stats['roi']:.1f}%"
        )

        if stats["bets"] >= 5 and stats["roi"] > best_roi:
            best_roi = stats["roi"]
            best_threshold = threshold

    print()
    if best_threshold > 0:
        print(f"🎯 Best threshold (min 5 bets): {best_threshold}% with {best_roi:.1f}% ROI")


def analyze_by_spread_size(bets: list[BetRecord]) -> None:
    """Analyze win rate by opening spread size."""
    print("\n" + "=" * 70)
    print("ANALYSIS BY OPENING SPREAD SIZE")
    print("=" * 70)
    print("\nDoes the size of the original spread affect outcomes?")
    print()

    # Group by spread ranges
    ranges = [
        ("Small (0-3)", 0, 3),
        ("Medium (3-7)", 3, 7),
        ("Large (7-12)", 7, 12),
        ("Very Large (12+)", 12, 100),
    ]

    print(f"{'Spread Range':<20} {'Bets':<6} {'Wins':<6} {'Win%':<8} {'Profit':<10} {'ROI':<8}")
    print("-" * 65)

    for name, low, high in ranges:
        matching = [b for b in bets if low <= abs(b.opening_spread) < high]

        if not matching:
            continue

        wins = sum(1 for b in matching if b.covered)
        losses = sum(1 for b in matching if not b.covered)
        total = wins + losses

        if total == 0:
            continue

        win_rate = wins / total * 100
        profit = sum(b.profit for b in matching)
        roi = profit / (total * 110) * 100

        print(f"{name:<20} {total:<6} {wins:<6} {win_rate:.1f}%{'':<4} ${profit:<9.0f} {roi:.1f}%")


def analyze_by_bet_direction(bets: list[BetRecord]) -> None:
    """Analyze win rate by whether we're betting favorite or underdog."""
    print("\n" + "=" * 70)
    print("ANALYSIS BY BET DIRECTION (FAVORITE vs UNDERDOG)")
    print("=" * 70)
    print("\nAre we more successful betting favorites or underdogs?")
    print()

    # Determine if bet was on favorite or underdog
    favorites = []
    underdogs = []

    for bet in bets:
        # Negative spread = favorite, Positive spread = underdog
        if bet.bet_spread < 0:
            favorites.append(bet)
        else:
            underdogs.append(bet)

    print(f"{'Direction':<15} {'Bets':<6} {'Wins':<6} {'Win%':<8} {'Profit':<10} {'ROI':<8}")
    print("-" * 60)

    for name, group in [("Favorites", favorites), ("Underdogs", underdogs)]:
        if not group:
            continue

        wins = sum(1 for b in group if b.covered)
        losses = sum(1 for b in group if not b.covered)
        total = wins + losses

        if total == 0:
            continue

        win_rate = wins / total * 100
        profit = sum(b.profit for b in group)
        roi = profit / (total * 110) * 100

        print(f"{name:<15} {total:<6} {wins:<6} {win_rate:.1f}%{'':<4} ${profit:<9.0f} {roi:.1f}%")


def analyze_by_line_movement_direction(bets: list[BetRecord]) -> None:
    """Analyze by which direction the line moved."""
    print("\n" + "=" * 70)
    print("ANALYSIS BY LINE MOVEMENT DIRECTION")
    print("=" * 70)
    print("\nDoes it matter if the line moved toward or away from our bet?")
    print()

    # Line moved toward favorite (spread got smaller in absolute terms)
    # Line moved toward underdog (spread got larger in absolute terms)
    toward_favorite = []
    toward_underdog = []

    for bet in bets:
        opening = abs(bet.opening_spread)
        current = abs(bet.bet_spread)

        if current < opening:
            toward_favorite.append(bet)
        else:
            toward_underdog.append(bet)

    print(f"{'Movement':<20} {'Bets':<6} {'Wins':<6} {'Win%':<8} {'Profit':<10} {'ROI':<8}")
    print("-" * 65)

    for name, group in [
        ("Toward Favorite", toward_favorite),
        ("Toward Underdog", toward_underdog),
    ]:
        if not group:
            continue

        wins = sum(1 for b in group if b.covered)
        losses = sum(1 for b in group if not b.covered)
        total = wins + losses

        if total == 0:
            continue

        win_rate = wins / total * 100
        profit = sum(b.profit for b in group)
        roi = profit / (total * 110) * 100

        print(f"{name:<20} {total:<6} {wins:<6} {win_rate:.1f}%{'':<4} ${profit:<9.0f} {roi:.1f}%")


def analyze_by_sport(bets: list[BetRecord]) -> None:
    """Analyze by sport."""
    print("\n" + "=" * 70)
    print("ANALYSIS BY SPORT")
    print("=" * 70)
    print()

    by_sport: dict[str, list[BetRecord]] = defaultdict(list)
    for bet in bets:
        sport_name = "NCAAB" if "ncaab" in bet.sport else "NBA"
        by_sport[sport_name].append(bet)

    print(f"{'Sport':<10} {'Bets':<6} {'Wins':<6} {'Win%':<8} {'Profit':<10} {'ROI':<8}")
    print("-" * 55)

    for sport, group in sorted(by_sport.items()):
        wins = sum(1 for b in group if b.covered)
        losses = sum(1 for b in group if not b.covered)
        total = wins + losses

        if total == 0:
            continue

        win_rate = wins / total * 100
        profit = sum(b.profit for b in group)
        roi = profit / (total * 110) * 100

        print(f"{sport:<10} {total:<6} {wins:<6} {win_rate:.1f}%{'':<4} ${profit:<9.0f} {roi:.1f}%")


def analyze_combined_filters(bets: list[BetRecord]) -> None:
    """Find the best combination of multiple filters."""
    print("\n" + "=" * 70)
    print("MULTI-FACTOR OPTIMIZATION")
    print("=" * 70)
    print("\nTesting combinations of threshold + spread size + direction...")
    print()

    best_combos: list[tuple] = []

    # Test combinations
    thresholds = [50, 75, 100, 125, 150, 175, 200, 250]
    spread_ranges = [
        ("any", 0, 100),
        ("small", 0, 5),
        ("medium", 5, 10),
        ("large", 10, 100),
    ]
    directions = [("any", None), ("favorite", "fav"), ("underdog", "dog")]
    sports = [("any", None), ("ncaab", "ncaab"), ("nba", "nba")]

    for threshold in thresholds:
        for spread_name, spread_low, spread_high in spread_ranges:
            for dir_name, dir_filter in directions:
                for sport_name, sport_filter in sports:
                    # Apply all filters
                    matching = [b for b in bets if b.spread_change_pct >= threshold]

                    # Spread size filter
                    matching = [
                        b for b in matching if spread_low <= abs(b.opening_spread) < spread_high
                    ]

                    # Direction filter
                    if dir_filter == "fav":
                        matching = [b for b in matching if b.bet_spread < 0]
                    elif dir_filter == "dog":
                        matching = [b for b in matching if b.bet_spread >= 0]

                    # Sport filter
                    if sport_filter:
                        matching = [b for b in matching if sport_filter in b.sport]

                    if len(matching) < 3:
                        continue

                    wins = sum(1 for b in matching if b.covered)
                    losses = sum(1 for b in matching if not b.covered)
                    total = wins + losses

                    if total < 3:
                        continue

                    win_rate = wins / total * 100
                    profit = sum(b.profit for b in matching)
                    roi = profit / (total * 110) * 100

                    if roi > 0:  # Only track profitable combos
                        best_combos.append(
                            (
                                roi,
                                total,
                                win_rate,
                                profit,
                                threshold,
                                spread_name,
                                dir_name,
                                sport_name,
                            )
                        )

    # Sort by ROI
    best_combos.sort(reverse=True)

    if not best_combos:
        print("❌ No profitable combinations found in the data.")
        print("   This could mean:")
        print("   - Sample size is too small")
        print("   - The strategy needs refinement")
        print("   - Market efficiency is working against this approach")
        return

    print("🏆 TOP 10 PROFITABLE COMBINATIONS:")
    print()
    print(f"{'ROI':<8} {'Bets':<6} {'Win%':<8} {'Profit':<10} {'Filters'}")
    print("-" * 75)

    for i, combo in enumerate(best_combos[:10]):
        roi, total, win_rate, profit, threshold, spread, direction, sport = combo
        filters = f"≥{threshold}% change, {spread} spread, {direction}, {sport}"
        print(f"{roi:.1f}%{'':<4} {total:<6} {win_rate:.1f}%{'':<4} ${profit:<9.0f} {filters}")


def analyze_margin_of_victory(bets: list[BetRecord]) -> None:
    """Analyze how close the games were to covering."""
    print("\n" + "=" * 70)
    print("MARGIN OF VICTORY ANALYSIS")
    print("=" * 70)
    print("\nHow close are our bets to covering? Are we losing by a lot or a little?")
    print()

    # Use final_margin directly (positive = covered, negative = didn't cover)
    margins = [(b, b.final_margin) for b in bets if b.final_margin is not None]

    if not margins:
        print("No margin data available.")
        return

    # Wins vs losses margin distribution
    win_margins = [m for b, m in margins if b.covered]
    loss_margins = [m for b, m in margins if not b.covered]

    if win_margins:
        print(f"Average margin when WINNING: +{sum(win_margins)/len(win_margins):.1f} points")
    if loss_margins:
        print(f"Average margin when LOSING:  {sum(loss_margins)/len(loss_margins):.1f} points")
    print()

    if not loss_margins:
        print("No losses to analyze.")
        return

    # Close losses (within 3 points)
    close_losses = [m for m in loss_margins if m >= -3]
    pct_close = len(close_losses) / len(loss_margins) * 100 if loss_margins else 0
    print(f"Close losses (within 3 pts): {len(close_losses)} ({pct_close:.0f}% of losses)")

    # Blowout losses (more than 10 points)
    blowout_losses = [m for m in loss_margins if m < -10]
    pct_blowout = len(blowout_losses) / len(loss_margins) * 100 if loss_margins else 0
    print(f"Blowout losses (>10 pts):    {len(blowout_losses)} ({pct_blowout:.0f}% of losses)")


def suggest_optimal_strategy(bets: list[BetRecord]) -> None:
    """Suggest the optimal strategy based on all analysis."""
    print("\n" + "=" * 70)
    print("RECOMMENDED STRATEGY")
    print("=" * 70)
    print()

    # Find best threshold
    best_threshold = 50
    best_roi = -999

    for threshold in range(50, 275, 25):
        matching = [b for b in bets if b.spread_change_pct >= threshold]
        if len(matching) < 5:
            continue

        wins = sum(1 for b in matching if b.covered)
        total = wins + sum(1 for b in matching if not b.covered)
        if total < 5:
            continue

        profit = sum(b.profit for b in matching)
        roi = profit / (total * 110) * 100

        if roi > best_roi:
            best_roi = roi
            best_threshold = threshold

    print(f"Based on {len(bets)} historical bets:")
    print()
    print(f"  📊 Best performing threshold: {best_threshold}%")
    print(f"  📈 ROI at that threshold: {best_roi:.1f}%")
    print()

    if best_roi < 0:
        print("  ⚠️  WARNING: No profitable threshold found!")
        print("  Possible causes:")
        print("    - Sample size too small (need more data)")
        print("    - Strategy needs additional filters")
        print("    - Live odds market is too efficient")
        print()
        print("  Recommendations:")
        print("    1. Collect more data via live monitoring")
        print("    2. Try combining with other signals (time remaining, etc.)")
        print("    3. Consider opposite strategy (fade the movement)")
    else:
        print(f"  ✅ PROFITABLE STRATEGY FOUND!")
        print(f"  Use threshold: {best_threshold}%+")
        print()
        print("  To update your live monitor, set:")
        print(f"    ALERT_THRESHOLD = {best_threshold / 100}")


def main() -> None:
    """Run all analyses."""
    parser = argparse.ArgumentParser(description="Analyze historical bet data")
    parser.add_argument(
        "--threshold-only",
        action="store_true",
        help="Only run threshold optimization",
    )
    args = parser.parse_args()

    print("=" * 70)
    print("HISTORICAL BET ANALYSIS")
    print("=" * 70)
    print("\nLoading data from SQLite (no API calls needed)...")

    bets = load_all_bets()

    if not bets:
        print("\n❌ No bet data found in database.")
        print("   Run historical_backfill.py first to populate data.")
        return

    print(f"\n📊 Loaded {len(bets)} completed bets")

    # Count by result
    wins = sum(1 for b in bets if b.covered)
    losses = sum(1 for b in bets if not b.covered)
    win_rate = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
    print(f"   Wins: {wins}, Losses: {losses}, Win Rate: {win_rate:.1f}%")

    if args.threshold_only:
        analyze_by_threshold(bets)
        return

    # Run all analyses
    analyze_by_sport(bets)
    analyze_by_threshold(bets)
    analyze_by_spread_size(bets)
    analyze_by_bet_direction(bets)
    analyze_by_line_movement_direction(bets)
    analyze_margin_of_victory(bets)
    analyze_combined_filters(bets)
    suggest_optimal_strategy(bets)


if __name__ == "__main__":
    main()
