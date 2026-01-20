#!/usr/bin/env python3
"""
Fetch historical games and odds to backtest betting strategies.

This script:
1. Gets completed games from the past N days
2. Fetches opening and mid-game odds for each
3. Stores everything in SQLite (cached, won't re-fetch)
4. Simulates bets based on spread changes
5. Reports strategy performance

Usage:
    python3 scripts/backfill.py --days 14
    python3 scripts/backfill.py --days 7 --sport basketball_nba
    python3 scripts/backfill.py --analyze-only  # Skip fetching, just analyze
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.live_odds_monitor import Game, OddsAPIClient, Storage
from src.live_odds_monitor.config import MonitorConfig

# Snapshot types
SNAPSHOT_OPENING = "opening"      # 12h before game
SNAPSHOT_PREGAME = "pregame"      # At game start
SNAPSHOT_MIDGAME = "midgame"      # ~1h after start (halftime-ish)

# Strategy thresholds to test
THRESHOLDS = [0.5, 0.75, 1.0, 1.25, 1.5, 2.0]


def get_spread_from_bookmakers(bookmakers: list, home_team: str) -> tuple:
    """Extract spread from bookmaker data.

    Returns: (home_spread, away_spread, home_price, away_price)
    """
    for bm in bookmakers:
        if bm["key"] == "fanduel":
            for market in bm.get("markets", []):
                if market["key"] == "spreads":
                    home_spread = None
                    away_spread = None
                    home_price = None
                    away_price = None

                    for outcome in market.get("outcomes", []):
                        if outcome["name"] == home_team:
                            home_spread = outcome.get("point")
                            home_price = outcome.get("price")
                        else:
                            away_spread = outcome.get("point")
                            away_price = outcome.get("price")

                    return home_spread, away_spread, home_price, away_price

    return None, None, None, None


def fetch_historical_odds_for_game(
    client: OddsAPIClient,
    storage: Storage,
    game_id: str,
    sport: str,
    commence_time: datetime,
    home_team: str,
) -> dict:
    """Fetch and cache historical odds for a game.

    Returns dict with opening, pregame, midgame spreads.
    """
    results = {}

    # Check cache first
    cached_opening = storage.get_cached_odds(game_id, SNAPSHOT_OPENING)
    cached_pregame = storage.get_cached_odds(game_id, SNAPSHOT_PREGAME)
    cached_midgame = storage.get_cached_odds(game_id, SNAPSHOT_MIDGAME)

    # Opening: 12 hours before game
    if cached_opening:
        results["opening"] = cached_opening
    else:
        try:
            opening_time = commence_time - timedelta(hours=12)
            data = client.get_historical_event_odds(
                event_id=game_id,
                sport=sport,
                date=opening_time,
                markets="spreads",
            )

            if data and "data" in data:
                game_data = data["data"]
                spreads = get_spread_from_bookmakers(
                    game_data.get("bookmakers", []),
                    home_team,
                )

                storage.cache_historical_odds(
                    game_id=game_id,
                    sport=sport,
                    snapshot_type=SNAPSHOT_OPENING,
                    timestamp=opening_time.isoformat(),
                    bookmaker="fanduel",
                    spread_home=spreads[0],
                    spread_away=spreads[1],
                    spread_home_price=spreads[2],
                    spread_away_price=spreads[3],
                )

                results["opening"] = {
                    "spread_home": spreads[0],
                    "spread_away": spreads[1],
                }

            time.sleep(0.5)  # Rate limiting

        except Exception as e:
            print(f"    Error fetching opening odds: {e}")

    # Pregame: at game start
    if cached_pregame:
        results["pregame"] = cached_pregame
    else:
        try:
            data = client.get_historical_event_odds(
                event_id=game_id,
                sport=sport,
                date=commence_time,
                markets="spreads",
            )

            if data and "data" in data:
                game_data = data["data"]
                spreads = get_spread_from_bookmakers(
                    game_data.get("bookmakers", []),
                    home_team,
                )

                storage.cache_historical_odds(
                    game_id=game_id,
                    sport=sport,
                    snapshot_type=SNAPSHOT_PREGAME,
                    timestamp=commence_time.isoformat(),
                    bookmaker="fanduel",
                    spread_home=spreads[0],
                    spread_away=spreads[1],
                    spread_home_price=spreads[2],
                    spread_away_price=spreads[3],
                )

                results["pregame"] = {
                    "spread_home": spreads[0],
                    "spread_away": spreads[1],
                }

            time.sleep(0.5)

        except Exception as e:
            print(f"    Error fetching pregame odds: {e}")

    # Midgame: 1 hour after start (roughly halftime)
    if cached_midgame:
        results["midgame"] = cached_midgame
    else:
        try:
            midgame_time = commence_time + timedelta(hours=1)
            data = client.get_historical_event_odds(
                event_id=game_id,
                sport=sport,
                date=midgame_time,
                markets="spreads",
            )

            if data and "data" in data:
                game_data = data["data"]
                spreads = get_spread_from_bookmakers(
                    game_data.get("bookmakers", []),
                    home_team,
                )

                storage.cache_historical_odds(
                    game_id=game_id,
                    sport=sport,
                    snapshot_type=SNAPSHOT_MIDGAME,
                    timestamp=midgame_time.isoformat(),
                    bookmaker="fanduel",
                    spread_home=spreads[0],
                    spread_away=spreads[1],
                    spread_home_price=spreads[2],
                    spread_away_price=spreads[3],
                )

                results["midgame"] = {
                    "spread_home": spreads[0],
                    "spread_away": spreads[1],
                }

            time.sleep(0.5)

        except Exception as e:
            print(f"    Error fetching midgame odds: {e}")

    return results


def calculate_spread_change(opening: float, current: float) -> float | None:
    """Calculate percentage change in spread."""
    if opening is None or current is None:
        return None
    if opening == 0:
        return None

    return abs(current / opening) - 1


def simulate_bets_for_game(
    storage: Storage,
    game_id: str,
    sport: str,
    home_team: str,
    away_team: str,
    odds_data: dict,
    final_margin: int,  # home - away
) -> list:
    """Simulate bets for various strategies and record outcomes."""
    bets_created = []

    opening = odds_data.get("opening", {})
    midgame = odds_data.get("midgame", {})

    open_home = opening.get("spread_home")
    mid_home = midgame.get("spread_home")

    if open_home is None or mid_home is None:
        return bets_created

    pct_change = calculate_spread_change(open_home, mid_home)
    if pct_change is None:
        return bets_created

    # For each threshold, create a simulated bet if change exceeds it
    for threshold in THRESHOLDS:
        if pct_change >= threshold:
            strategy = f"spread_change_{int(threshold * 100)}pct"

            # Bet the midgame line (current line when alert would fire)
            # If spread widened, bet the team getting more points
            if abs(mid_home) > abs(open_home):
                # Spread widened - bet the underdog (getting more points)
                if mid_home > 0:
                    bet_team = home_team
                    bet_spread = mid_home
                else:
                    bet_team = away_team
                    bet_spread = -mid_home  # Convert to positive
            else:
                # Spread narrowed - bet the favorite (giving fewer points)
                if mid_home < 0:
                    bet_team = home_team
                    bet_spread = mid_home
                else:
                    bet_team = away_team
                    bet_spread = -mid_home

            # Determine if bet covered
            if bet_team == home_team:
                # Home team bet: margin + spread > 0 to cover
                adjusted = final_margin + mid_home
                covered = adjusted > 0
            else:
                # Away team bet: -margin + spread > 0 to cover
                adjusted = -final_margin + abs(mid_home)
                covered = adjusted > 0

            profit = 100.0 if covered else -110.0

            bet_id = storage.save_simulated_bet(
                game_id=game_id,
                sport=sport,
                strategy=strategy,
                bet_team=bet_team,
                bet_spread=bet_spread,
                opening_spread=open_home,
                pct_change=pct_change,
                snapshot_type=SNAPSHOT_MIDGAME,
            )

            storage.update_simulated_bet(
                bet_id=bet_id,
                final_margin=final_margin,
                covered=covered,
                profit=profit,
            )

            bets_created.append({
                "strategy": strategy,
                "team": bet_team,
                "spread": bet_spread,
                "covered": covered,
            })

    return bets_created


def fetch_completed_games(
    client: OddsAPIClient,
    sport: str,
    days: int,
) -> list:
    """Fetch completed games from the past N days."""
    # The scores endpoint with daysFrom gives us completed games
    # We can only go back 3 days with daysFrom, so we need historical
    all_games = []

    # For more than 3 days, we need to use the events endpoint
    # and check which games are completed via scores

    # Get recent completed games (last 3 days via scores)
    for days_back in range(1, min(4, days + 1)):
        try:
            scores = client.get_scores(sport, days_from=days_back)
            for game in scores:
                if game.get("completed"):
                    all_games.append(game)
            time.sleep(0.3)
        except Exception as e:
            print(f"  Error fetching scores for day -{days_back}: {e}")

    print(f"  Found {len(all_games)} completed games from last 3 days")

    # Remove duplicates
    seen_ids = set()
    unique_games = []
    for game in all_games:
        if game["id"] not in seen_ids:
            seen_ids.add(game["id"])
            unique_games.append(game)

    return unique_games


def print_strategy_analysis(storage: Storage, sport: str = None):
    """Print analysis for all strategies."""
    print("\n" + "=" * 70)
    print("STRATEGY PERFORMANCE ANALYSIS")
    print("=" * 70)

    print(f"\n{'Strategy':<25} {'Bets':<6} {'Wins':<6} {'Win%':<8} "
          f"{'Profit':<10} {'ROI':<8}")
    print("-" * 70)

    for threshold in THRESHOLDS:
        strategy = f"spread_change_{int(threshold * 100)}pct"
        stats = storage.get_simulated_bet_stats(strategy=strategy, sport=sport)

        if stats["total_bets"] > 0:
            print(
                f"{strategy:<25} "
                f"{stats['total_bets']:<6} "
                f"{stats['wins']:<6} "
                f"{stats['win_rate']*100:<7.1f}% "
                f"${stats['total_profit']:<9.0f} "
                f"{stats['roi']*100:<7.1f}%"
            )
        else:
            print(f"{strategy:<25} {'0':<6} {'-':<6} {'-':<8} {'-':<10} {'-':<8}")


def main():
    parser = argparse.ArgumentParser(
        description="Backfill historical data for strategy analysis"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Number of days to fetch (default: 14)",
    )
    parser.add_argument(
        "--sport",
        type=str,
        default=None,
        help="Sport to analyze (default: both NCAAB and NBA)",
    )
    parser.add_argument(
        "--analyze-only",
        action="store_true",
        help="Skip fetching, just analyze existing data",
    )
    parser.add_argument(
        "--max-games",
        type=int,
        default=None,
        help="Maximum games to process (for testing)",
    )
    parser.add_argument(
        "--all-teams",
        action="store_true",
        help="Process all teams (ignore watchlist filter for NCAAB)",
    )

    args = parser.parse_args()

    client = OddsAPIClient(os.getenv("ODDS_API_KEY"))
    storage = Storage()
    config = MonitorConfig()

    if args.sport:
        sports = [args.sport]
    else:
        sports = ["basketball_ncaab", "basketball_nba"]

    print("=" * 70)
    print("HISTORICAL BACKFILL & STRATEGY ANALYSIS")
    print("=" * 70)
    print(f"Days to analyze: {args.days}")
    print(f"Sports: {', '.join(sports)}")
    if not args.all_teams:
        print(f"NCAAB watchlist: {len(config.watchlist)} teams")
    else:
        print("NCAAB: Processing ALL teams (no filter)")
    print(f"Credits remaining: {client.requests_remaining or 'Unknown'}")
    print("=" * 70)

    if not args.analyze_only:
        for sport in sports:
            print(f"\n📊 Processing {sport}...")

            # Fetch completed games
            games = fetch_completed_games(client, sport, args.days)
            print(f"  Found {len(games)} completed games")

            # Filter NCAAB by watchlist (unless --all-teams)
            if sport == "basketball_ncaab" and not args.all_teams:
                original_count = len(games)
                games = [
                    g for g in games
                    if config.is_team_watched(g["home_team"])
                    or config.is_team_watched(g["away_team"])
                ]
                print(f"  Filtered to {len(games)} watchlist games "
                      f"(from {original_count})")

            if args.max_games:
                games = games[:args.max_games]
                print(f"  (Limited to {args.max_games} for testing)")

            processed = 0
            cached = 0

            for game in games:
                game_id = game["id"]
                home = game["home_team"]
                away = game["away_team"]

                # Parse commence time
                commence_str = game.get("commence_time", "")
                if commence_str.endswith("Z"):
                    commence_str = commence_str[:-1] + "+00:00"
                try:
                    commence = datetime.fromisoformat(commence_str)
                except Exception:
                    continue

                # Check if already cached
                if storage.has_cached_game(game_id):
                    cached += 1
                    continue

                print(f"\n  [{processed + 1}/{len(games)}] {away} @ {home}")

                # Save game to database
                game_obj = Game(
                    id=game_id,
                    home_team=home,
                    away_team=away,
                    commence_time=commence,
                    sport=sport,
                )
                storage.save_game(game_obj)

                # Get final score
                home_score = None
                away_score = None
                for s in game.get("scores", []):
                    try:
                        if s["name"] == home:
                            home_score = int(s.get("score", 0))
                        else:
                            away_score = int(s.get("score", 0))
                    except (ValueError, TypeError):
                        pass

                if home_score is not None and away_score is not None:
                    storage.save_game_result(game_id, home_score, away_score)
                    final_margin = home_score - away_score
                else:
                    final_margin = None

                # Fetch historical odds
                odds_data = fetch_historical_odds_for_game(
                    client=client,
                    storage=storage,
                    game_id=game_id,
                    sport=sport,
                    commence_time=commence,
                    home_team=home,
                )

                # Simulate bets
                if final_margin is not None and odds_data:
                    bets = simulate_bets_for_game(
                        storage=storage,
                        game_id=game_id,
                        sport=sport,
                        home_team=home,
                        away_team=away,
                        odds_data=odds_data,
                        final_margin=final_margin,
                    )

                    if bets:
                        for bet in bets[:1]:  # Just show first strategy hit
                            result = "✅" if bet["covered"] else "❌"
                            print(f"    {result} {bet['strategy']}")

                processed += 1
                print(f"    Credits remaining: {client.requests_remaining}")

            print(f"\n  Processed: {processed}, Cached: {cached}")

    # Analyze results - combined only (remove per-sport to avoid duplicate output)
    print_strategy_analysis(storage, sport=None)

    print(f"\n✅ Done! Credits remaining: {client.requests_remaining}")


if __name__ == "__main__":
    main()
