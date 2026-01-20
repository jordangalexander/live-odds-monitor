#!/usr/bin/env python3
"""
Monitor live games, comparing current spread to opening line.
Polls every 15 minutes. Records all data for backtesting.
"""

import os
import sys
import time
from datetime import datetime, timedelta, timezone

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.live_odds_monitor import (
    Game,
    HistoricalTracker,
    OddsAPIClient,
    Storage,
)

# Configuration
POLL_INTERVAL = 15 * 60  # 15 minutes in seconds
MAX_GAMES = 10  # Monitor up to this many games
ALERT_THRESHOLD = 1.0  # 100% spread change
MIN_TIME_REMAINING = 10  # minutes - stop watching if less

# Store opening lines (in-memory cache)
opening_lines = {}  # game_id -> spread
stopped_games = set()  # games we stopped watching due to time
alerted_games = set()  # games we've already alerted on


def estimate_minutes_remaining(commence_time_str):
    """
    Estimate minutes remaining in a college basketball game.

    College basketball: two 20-minute halves = 40 min game time
    Real time with stoppages/halftime: ~2 hours total

    Returns estimated minutes left, or None if can't determine.
    """
    try:
        # Parse commence time
        if commence_time_str.endswith("Z"):
            commence_time_str = commence_time_str[:-1] + "+00:00"
        commence = datetime.fromisoformat(commence_time_str)
        now = datetime.now(timezone.utc)

        elapsed = (now - commence).total_seconds() / 60  # minutes

        # Typical game is ~120 minutes real time
        # First half: ~0-50 min elapsed
        # Halftime: ~50-65 min elapsed
        # Second half: ~65-120 min elapsed

        if elapsed < 50:
            # First half - plenty of time
            return 40  # full game remaining (rough)
        elif elapsed < 65:
            # Halftime
            return 20  # second half remaining
        else:
            # Second half
            # Map 65-120 elapsed to 20-0 game minutes
            second_half_elapsed = elapsed - 65
            remaining = max(0, 20 - (second_half_elapsed * 20 / 55))
            return remaining

    except Exception:
        return None


def get_spread(game_data, team_filter=None):
    """Extract spread from game data."""
    for bm in game_data.get("bookmakers", []):
        if bm["key"] == "fanduel":
            for market in bm.get("markets", []):
                if market["key"] == "spreads":
                    for outcome in market.get("outcomes", []):
                        if team_filter is None:
                            return outcome.get("point"), outcome["name"]
                        if team_filter in outcome["name"]:
                            return outcome.get("point"), outcome["name"]
    return None, None


def fetch_opening_line(client, game_id, sport="basketball_ncaab"):
    """Fetch opening line from historical API."""
    if game_id in opening_lines:
        return opening_lines[game_id]

    # Query 12 hours ago to get pre-game odds
    target_time = datetime.now(timezone.utc) - timedelta(hours=12)

    try:
        historical = client.get_historical_event_odds(
            event_id=game_id,
            sport=sport,
            date=target_time,
            markets="spreads",
        )

        if historical and "data" in historical:
            data = historical["data"]
            spread, team = get_spread(data)
            if spread is not None:
                home = data.get("home_team", "")
                opening_lines[game_id] = (spread, team, home)
                return opening_lines[game_id]
    except Exception as e:
        print(f"  Error fetching historical: {e}")

    return None


def main():
    client = OddsAPIClient(os.getenv("ODDS_API_KEY"))
    storage = Storage()
    tracker = HistoricalTracker(storage=storage, client=client)

    print("=" * 70)
    print("Live Game Monitor - Spread Tracker (with Historical Tracking)")
    print("=" * 70)
    print(f"Polling every {POLL_INTERVAL // 60} minutes")
    print(f"Monitoring up to {MAX_GAMES} games")
    print(f"Alert threshold: {ALERT_THRESHOLD * 100:.0f}% spread change")
    print("Recording all data to SQLite for backtesting")
    print("=" * 70)
    print()

    try:
        while True:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n[{now}] Polling...")

            # Get live odds
            odds_data = client.get_live_odds(
                "basketball_ncaab",
                markets="spreads",
                bookmakers="fanduel",
            )

            # Get scores
            scores_data = client.get_scores("basketball_ncaab")

            # Find live games (have scores, not completed)
            live_games = []
            for game in odds_data:
                game_id = game["id"]

                # Skip games we've already stopped watching
                if game_id in stopped_games:
                    continue

                # Check if game is live
                for score in scores_data:
                    if score["id"] == game_id:
                        if score.get("scores") and not score.get("completed"):
                            game["_scores"] = score.get("scores", [])
                            game["_commence_time"] = score.get("commence_time")

                            # Check time remaining
                            commence = score.get("commence_time", "")
                            mins_left = estimate_minutes_remaining(commence)
                            game["_mins_remaining"] = mins_left

                            if mins_left is not None:
                                if mins_left < MIN_TIME_REMAINING:
                                    stopped_games.add(game_id)
                                    away = game["away_team"]
                                    home = game["home_team"]
                                    print(
                                        f"  ⏱️  Stopped: {away} @ {home} "
                                        f"(~{mins_left:.0f} min left)"
                                    )
                                    continue

                            live_games.append(game)
                        break

            print(f"Found {len(live_games)} live games with odds")

            # Monitor up to MAX_GAMES
            monitored = 0
            for game in live_games[:MAX_GAMES]:
                game_id = game["id"]
                home = game["home_team"]
                away = game["away_team"]

                # Save game to database
                commence_str = game.get("commence_time", "")
                if commence_str.endswith("Z"):
                    commence_str = commence_str[:-1] + "+00:00"
                try:
                    commence_dt = datetime.fromisoformat(commence_str)
                except Exception:
                    commence_dt = datetime.now(timezone.utc)

                game_obj = Game(
                    id=game_id,
                    home_team=home,
                    away_team=away,
                    commence_time=commence_dt,
                )
                storage.save_game(game_obj)

                # Get current spread (home team)
                current_spread, spread_team = get_spread(game, home.split()[0])
                if current_spread is None:
                    current_spread, spread_team = get_spread(game)

                # Get score
                home_score = 0
                away_score = 0
                score_str = ""
                for s in game.get("_scores", []):
                    score_str += f"{s['name']}: {s.get('score', '?')}  "
                    try:
                        if s["name"] == home:
                            home_score = int(s.get("score", 0))
                        else:
                            away_score = int(s.get("score", 0))
                    except (ValueError, TypeError):
                        pass

                mins_left = game.get("_mins_remaining")
                time_str = f"(~{mins_left:.0f} min left)" if mins_left else ""

                # Get opening line
                opening = fetch_opening_line(client, game_id)

                print(f"\n  {away} @ {home} {time_str}")
                print(f"  Score: {score_str}")

                if current_spread is None:
                    print("  No spread available from FanDuel")
                    monitored += 1
                    continue

                # Record opening line if we have it
                if opening:
                    open_spread, open_team, _ = opening
                    tracker.record_opening_line(
                        game_id=game_id,
                        spread_team=open_team,
                        spread_value=open_spread,
                    )

                # Record live line snapshot
                tracker.record_live_line(
                    game_id=game_id,
                    spread_team=spread_team,
                    spread_value=current_spread,
                    home_score=home_score,
                    away_score=away_score,
                    mins_remaining=mins_left,
                )

                if opening:
                    print(f"  Opening: {open_team} {open_spread:+.1f}")
                    print(f"  Current: {spread_team} {current_spread:+.1f}")

                    # Calculate change
                    if open_spread and open_spread != 0:
                        pct_change = abs(current_spread / open_spread) - 1
                        if abs(current_spread) > abs(open_spread):
                            direction = "widened"
                        else:
                            direction = "narrowed"
                        pct_display = pct_change * 100
                        print(f"  Change: {pct_display:+.1f}% ({direction})")

                        if pct_change >= ALERT_THRESHOLD:
                            print(
                                f"  🚨 ALERT: Spread moved "
                                f"{pct_display:.0f}% from open!"
                            )

                            # Record alert and bet (only once per game)
                            if game_id not in alerted_games:
                                tracker.record_alert(
                                    game_id=game_id,
                                    spread_team=spread_team,
                                    current_spread=current_spread,
                                    opening_spread=open_spread,
                                    pct_change=pct_change,
                                    mins_remaining=mins_left,
                                )
                                alerted_games.add(game_id)
                                print("  📝 Recorded bet for backtesting")
                else:
                    print(
                        f"  Current: {spread_team} {current_spread:+.1f} "
                        "(no opening line)"
                    )

                monitored += 1

            print(f"\n  Credits remaining: {client.requests_remaining}")

            # Periodically check for completed games and resolve bets
            results_updated = tracker.fetch_and_record_game_results()
            if results_updated:
                print(f"  📊 Updated {results_updated} game results")

            bets_resolved = tracker.resolve_pending_bets()
            if bets_resolved:
                print(f"  ✅ Resolved {bets_resolved} pending bets")

                # Show current stats
                stats = tracker.get_strategy_stats(
                    min_pct_change=ALERT_THRESHOLD,
                    min_mins_remaining=MIN_TIME_REMAINING,
                )
                if stats["total_bets"] > 0:
                    print(
                        f"  📈 Strategy: {stats['wins']}/{stats['total_bets']} "
                        f"({stats['win_rate']*100:.1f}%) | "
                        f"Profit: ${stats['total_profit']:.0f}"
                    )

            print(f"  Next poll in {POLL_INTERVAL // 60} minutes...")

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print("\n\nStopped by user")
        print(f"Final credits remaining: {client.requests_remaining}")

        # Show final stats
        stats = tracker.get_strategy_stats()
        if stats["total_bets"] > 0:
            print("\n📊 Session Statistics:")
            print(f"  Total bets tracked: {stats['total_bets']}")
            print(f"  Wins: {stats['wins']} | Losses: {stats['losses']}")
            print(f"  Win rate: {stats['win_rate']*100:.1f}%")
            print(f"  Total profit: ${stats['total_profit']:.0f}")
            print(f"  ROI: {stats['roi']*100:.1f}%")


if __name__ == "__main__":
    main()
