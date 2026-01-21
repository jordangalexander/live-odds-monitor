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
from pathlib import Path

# Try to import matplotlib for visualizations
try:
    import matplotlib.pyplot as plt

    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

# DB location: ~/.local/share/live-odds-monitor/
DB_PATH = Path.home() / ".local" / "share" / "live-odds-monitor" / "odds_monitor.db"


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


def analyze_by_time_remaining(bets: list[BetRecord]) -> None:
    """Analyze win rate by time remaining in game."""
    print("\n" + "=" * 70)
    print("ANALYSIS BY TIME REMAINING")
    print("=" * 70)
    print("\nDo line moves happen more at certain times? Are fades more profitable then?")
    print()

    # Group by time buckets
    time_ranges = [
        ("Late Game (<10 min)", 0, 10),
        ("Mid Game (10-20 min)", 10, 20),
        ("Early Game (20-30 min)", 20, 30),
        ("Very Early (30+ min)", 30, 60),
    ]

    print(f"{'Time Range':<20} {'Bets':<6} {'Wins':<6} {'Win%':<8} {'Profit':<10} {'ROI':<8}")
    print("-" * 65)

    for name, low, high in time_ranges:
        matching = [
            b for b in bets if b.final_margin is not None and low <= (40 - b.final_margin) < high
        ]

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


def analyze_by_score_differential(bets: list[BetRecord]) -> None:
    """Analyze by score differential at time of bet."""
    print("\n" + "=" * 70)
    print("ANALYSIS BY SCORE DIFFERENTIAL")
    print("=" * 70)
    print("\nDoes the current score gap affect fade profitability?")
    print()

    # This would require score data in the database
    # For now, show a placeholder
    print("⚠️  Score differential analysis requires score tracking in database.")
    print("   This is a great next step - add home_score/away_score to snapshots!")
    print("   Expected insight: Teams ahead might see more public chasing (better fades).")
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
        print(f"Average margin when WINNING: +{sum(win_margins) / len(win_margins):.1f} points")
    if loss_margins:
        print(f"Average margin when LOSING:  {sum(loss_margins) / len(loss_margins):.1f} points")
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


def create_visualizations(bets: list[BetRecord], output_dir: Path) -> None:
    """Create visualization charts for the backtest results."""
    if not HAS_MATPLOTLIB:
        print("\n⚠️  matplotlib not installed. Skipping visualizations.")
        print("   Install with: pip install matplotlib")
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    plt.style.use("seaborn-v0_8-darkgrid")

    # 1. ROI by Threshold Chart
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # Threshold performance
    thresholds = []
    rois = []
    win_rates = []
    bet_counts = []

    for threshold in range(25, 275, 25):
        matching = [b for b in bets if b.spread_change_pct >= threshold]
        if len(matching) < 3:
            continue

        wins = sum(1 for b in matching if b.covered)
        total = len(matching)
        profit = sum(b.profit for b in matching)
        roi = profit / (total * 110) * 100

        thresholds.append(threshold)
        rois.append(roi)
        win_rates.append(wins / total * 100)
        bet_counts.append(total)

    ax1 = axes[0, 0]
    ax1_twin = ax1.twinx()

    bars = ax1.bar(thresholds, rois, color="steelblue", alpha=0.7, label="ROI %")
    ax1_twin.plot(thresholds, bet_counts, "ro-", label="# Bets", markersize=6)

    ax1.axhline(y=0, color="black", linestyle="-", linewidth=0.5)
    ax1.set_xlabel("Minimum Threshold (%)")
    ax1.set_ylabel("ROI (%)", color="steelblue")
    ax1_twin.set_ylabel("Number of Bets", color="red")
    ax1.set_title("ROI by Minimum Threshold")
    ax1.legend(loc="upper left")
    ax1_twin.legend(loc="upper right")

    # 2. Win Rate by Threshold
    ax2 = axes[0, 1]
    colors = ["green" if wr >= 52.4 else "red" for wr in win_rates]
    ax2.bar(thresholds, win_rates, color=colors, alpha=0.7)
    ax2.axhline(y=52.4, color="orange", linestyle="--", linewidth=2, label="Breakeven (52.4%)")
    ax2.set_xlabel("Minimum Threshold (%)")
    ax2.set_ylabel("Win Rate (%)")
    ax2.set_title("Win Rate by Threshold (Green = Profitable)")
    ax2.legend()
    ax2.set_ylim(0, 100)

    # 3. Cumulative P&L over time
    ax3 = axes[1, 0]

    # Sort bets by game_id (proxy for time)
    sorted_bets = sorted(bets, key=lambda b: b.game_id)
    cumulative_pnl = []
    running_total = 0
    for bet in sorted_bets:
        running_total += bet.profit
        cumulative_pnl.append(running_total)

    ax3.plot(range(len(cumulative_pnl)), cumulative_pnl, "b-", linewidth=1.5)
    ax3.fill_between(
        range(len(cumulative_pnl)),
        cumulative_pnl,
        where=[p >= 0 for p in cumulative_pnl],
        color="green",
        alpha=0.3,
    )
    ax3.fill_between(
        range(len(cumulative_pnl)),
        cumulative_pnl,
        where=[p < 0 for p in cumulative_pnl],
        color="red",
        alpha=0.3,
    )
    ax3.axhline(y=0, color="black", linestyle="-", linewidth=0.5)
    ax3.set_xlabel("Bet Number")
    ax3.set_ylabel("Cumulative P&L ($)")
    ax3.set_title("Cumulative Profit/Loss Over Time")

    # 4. Sport comparison
    ax4 = axes[1, 1]

    by_sport: dict[str, list[BetRecord]] = defaultdict(list)
    for bet in bets:
        sport_name = "NCAAB" if "ncaab" in bet.sport else "NBA"
        by_sport[sport_name].append(bet)

    sports = []
    sport_rois = []
    sport_wins = []
    sport_totals = []

    for sport, group in sorted(by_sport.items()):
        wins = sum(1 for b in group if b.covered)
        total = len(group)
        profit = sum(b.profit for b in group)
        roi = profit / (total * 110) * 100

        sports.append(sport)
        sport_rois.append(roi)
        sport_wins.append(wins / total * 100)
        sport_totals.append(total)

    x = range(len(sports))
    width = 0.35

    bars1 = ax4.bar([i - width / 2 for i in x], sport_rois, width, label="ROI %", color="steelblue")
    bars2 = ax4.bar(
        [i + width / 2 for i in x], sport_wins, width, label="Win Rate %", color="green", alpha=0.7
    )

    ax4.axhline(y=0, color="black", linestyle="-", linewidth=0.5)
    ax4.axhline(y=52.4, color="orange", linestyle="--", linewidth=1, label="Breakeven")
    ax4.set_xlabel("Sport")
    ax4.set_ylabel("Percentage")
    ax4.set_title("Performance by Sport")
    ax4.set_xticks(x)
    ax4.set_xticklabels([f"{s}\n({t} bets)" for s, t in zip(sports, sport_totals)])
    ax4.legend()

    plt.tight_layout()
    chart_path = output_dir / "backtest_analysis.png"
    plt.savefig(chart_path, dpi=150, bbox_inches="tight")
    plt.close()

    print(f"\n📊 Saved main analysis chart to: {chart_path}")

    # 5. Separate chart: Margin distribution
    fig2, (ax5, ax6) = plt.subplots(1, 2, figsize=(12, 5))

    win_margins = [b.final_margin for b in bets if b.covered and b.final_margin is not None]
    loss_margins = [b.final_margin for b in bets if not b.covered and b.final_margin is not None]

    if win_margins:
        ax5.hist(win_margins, bins=20, color="green", alpha=0.7, edgecolor="black")
        ax5.axvline(
            x=sum(win_margins) / len(win_margins),
            color="darkgreen",
            linestyle="--",
            linewidth=2,
            label=f"Avg: +{sum(win_margins) / len(win_margins):.1f}",
        )
        ax5.set_xlabel("Margin (points)")
        ax5.set_ylabel("Frequency")
        ax5.set_title(f"Winning Bet Margins (n={len(win_margins)})")
        ax5.legend()

    if loss_margins:
        ax6.hist(loss_margins, bins=20, color="red", alpha=0.7, edgecolor="black")
        ax6.axvline(
            x=sum(loss_margins) / len(loss_margins),
            color="darkred",
            linestyle="--",
            linewidth=2,
            label=f"Avg: {sum(loss_margins) / len(loss_margins):.1f}",
        )
        ax6.set_xlabel("Margin (points)")
        ax6.set_ylabel("Frequency")
        ax6.set_title(f"Losing Bet Margins (n={len(loss_margins)})")
        ax6.legend()

    plt.tight_layout()
    margin_path = output_dir / "margin_distribution.png"
    plt.savefig(margin_path, dpi=150, bbox_inches="tight")
    plt.close()

    print(f"📊 Saved margin distribution to: {margin_path}")

    # 6. Spread size analysis chart
    fig3, ax7 = plt.subplots(figsize=(10, 6))

    spread_ranges = [
        ("0-3", 0, 3),
        ("3-5", 3, 5),
        ("5-7", 5, 7),
        ("7-10", 7, 10),
        ("10-15", 10, 15),
        ("15+", 15, 100),
    ]

    range_names = []
    range_rois = []
    range_counts = []

    for name, low, high in spread_ranges:
        matching = [b for b in bets if low <= abs(b.opening_spread) < high]
        if len(matching) < 2:
            continue

        profit = sum(b.profit for b in matching)
        roi = profit / (len(matching) * 110) * 100

        range_names.append(name)
        range_rois.append(roi)
        range_counts.append(len(matching))

    colors = ["green" if r > 0 else "red" for r in range_rois]
    bars = ax7.bar(range_names, range_rois, color=colors, alpha=0.7, edgecolor="black")

    # Add count labels on bars
    for bar, count in zip(bars, range_counts):
        ax7.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 1,
            f"n={count}",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    ax7.axhline(y=0, color="black", linestyle="-", linewidth=1)
    ax7.set_xlabel("Opening Spread Range (points)")
    ax7.set_ylabel("ROI (%)")
    ax7.set_title("ROI by Opening Spread Size")

    plt.tight_layout()
    spread_path = output_dir / "spread_size_analysis.png"
    plt.savefig(spread_path, dpi=150, bbox_inches="tight")
    plt.close()

    print(f"📊 Saved spread size analysis to: {spread_path}")


def main() -> None:
    """Run all analyses."""
    parser = argparse.ArgumentParser(description="Analyze historical bet data")
    parser.add_argument(
        "--threshold-only",
        action="store_true",
        help="Only run threshold optimization",
    )
    parser.add_argument(
        "--visualize",
        action="store_true",
        help="Generate visualization charts",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Directory to save charts (default: ./reports/)",
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
    # analyze_margin_of_victory(bets)  # Function not implemented
    analyze_by_time_remaining(bets)
    analyze_by_score_differential(bets)
    analyze_combined_filters(bets)
    suggest_optimal_strategy(bets)

    # Generate visualizations if requested
    if args.visualize:
        output_dir = Path(args.output_dir) if args.output_dir else Path.cwd() / "reports"
        create_visualizations(bets, output_dir)


if __name__ == "__main__":
    main()
