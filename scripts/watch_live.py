#!/usr/bin/env python3
"""
Monitor live games, comparing current spread to opening line.
Polls every 15 minutes. Records all data for backtesting.
"""

import os
import sys
import time
import requests
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
MAX_GAMES = None  # No limit - monitor all available games
# Time windows are sport-specific (NBA: 48min, NCAAB: 40min games)
# Optimal: ~25-60% of game remaining (enough time but not too early)
MIN_TIME_REMAINING = 10  # minutes - stop watching if less (NCAAB) / 12 (NBA)
MAX_TIME_REMAINING = 36  # minutes - max for NBA (75% of game)
MAX_EMPTY_POLLS = 3  # Stop after this many consecutive polls with no live games
MAX_RUNTIME_HOURS = 6  # Maximum hours to run before auto-stopping

# Fade strategy filters (from backtest analysis)
# Tight spreads (0-3 pts): 82% win, 57% ROI
# Medium spreads (3-7 pts): 71% win, 35% ROI
# Large spreads (7-12 pts): 100% win, 91% ROI (small sample)
# Expanded to 0-10 to collect more data on medium/large spreads
MAX_OPENING_SPREAD = 10.0  # Expanded from 5.0 to capture more opportunities
MIN_OPENING_SPREAD = 0.0  # Minimum spread (0 = pick'em)

# Weighted scoring system - must reach MIN_ALERT_SCORE to trigger bet
MIN_ALERT_SCORE = 70  # 0-100 scale, 70+ indicates high-quality opportunity

# Sports to monitor
SPORTS = ["basketball_nba", "basketball_ncaab"]

# Win probabilities by sport (from backtest Jan 2026)
# NBA: 88.7% win rate, 69.4% ROI (62 bets)
# NCAAB: 65.5% win rate, 25.1% ROI (58 bets)
WIN_PROBABILITY_BY_SPORT = {
    "basketball_nba": 0.887,
    "basketball_ncaab": 0.655,
}

# Bankroll & Bet Sizing (Kelly Criterion based)
# Using 1/4 Kelly for safety
BANKROLL = 100.0  # Total bankroll in dollars
KELLY_FRACTION = 0.25  # Use 1/4 Kelly for safety


def calculate_bet_size(bankroll: float, win_prob: float, odds: int = -110) -> float:
    """
    Calculate optimal bet size using fractional Kelly Criterion.

    Kelly % = (bp - q) / b
    where:
      b = decimal odds - 1 (what you win per $1 bet)
      p = win probability
      q = loss probability (1 - p)

    Then multiply by KELLY_FRACTION for safety.
    """
    if odds < 0:
        # Negative odds: bet |odds| to win 100
        b = 100 / abs(odds)
    else:
        # Positive odds: bet 100 to win odds
        b = odds / 100

    p = win_prob
    q = 1 - p

    kelly_pct = (b * p - q) / b

    # Apply fractional Kelly and cap at reasonable max
    bet_pct = kelly_pct * KELLY_FRACTION
    bet_pct = max(0, min(bet_pct, 0.20))  # Cap at 20% max

    return round(bankroll * bet_pct, 2)


def calculate_opportunity_score(
    pct_change: float,
    mins_remaining: float,
    opening_spread: float,
    score_diff: float,
    sport: str,
) -> dict:
    """
    Calculate weighted opportunity score (0-100) based on multiple factors.

    Factors:
    1. Line Movement (35 pts): Bigger moves = stronger signal
    2. Time Remaining (30 pts): 10-30 min window is optimal
    3. Opening Spread (15 pts): Tighter spreads perform better
    4. Score Differential (10 pts): Close games have more value left
    5. Sport (10 pts): NBA performs significantly better

    Returns dict with score and breakdown
    """
    score = 0
    breakdown = {}

    # 1. LINE MOVEMENT (35 points) - Most important factor
    # 75%+ moves show clear public overreaction
    if pct_change >= 2.5:  # 250%+
        line_score = 35
    elif pct_change >= 2.0:  # 200%+
        line_score = 30
    elif pct_change >= 1.5:  # 150%+
        line_score = 25
    elif pct_change >= 1.0:  # 100%+
        line_score = 20
    elif pct_change >= 0.75:  # 75%+
        line_score = 15
    else:
        line_score = 0
    score += line_score
    breakdown["line_move"] = line_score

    # 2. TIME REMAINING (30 points) - Critical for comeback potential
    # Sport-adjusted windows (NBA: 48min, NCAAB: 40min games)
    # Sweet spot: ~25-60% of game remaining (enough time but not too early)
    if mins_remaining is None:
        time_score = 0
    else:
        is_nba = "nba" in sport.lower()
        if is_nba:
            # NBA: 48 min game - optimal 12-30 min (25-62.5% remaining)
            if 15 <= mins_remaining <= 30:  # Optimal range
                time_score = 30
            elif 12 <= mins_remaining < 15 or 30 < mins_remaining <= 36:  # Good range
                time_score = 20
            elif mins_remaining < 12:  # Too late
                time_score = 5
            else:  # 36+ min - too early, weak performance
                time_score = 10
        else:
            # NCAAB: 40 min game - optimal 10-25 min (25-62.5% remaining)
            if 12 <= mins_remaining <= 25:  # Optimal range
                time_score = 30
            elif 10 <= mins_remaining < 12 or 25 < mins_remaining <= 30:  # Good range
                time_score = 20
            elif mins_remaining < 10:  # Too late
                time_score = 5
            else:  # 30+ min - too early, weak performance
                time_score = 10
    score += time_score
    breakdown["time_remaining"] = time_score

    # 3. OPENING SPREAD (15 points) - Tighter spreads perform better
    # Scoring: prioritize tight but don't exclude medium spreads entirely
    abs_spread = abs(opening_spread)
    if abs_spread <= 2:  # Very tight (pick'em to 2pts)
        spread_score = 15
    elif abs_spread <= 3:  # Tight (2-3pts)
        spread_score = 12
    elif abs_spread <= 5:  # Medium-tight (3-5pts)
        spread_score = 8
    elif abs_spread <= 7:  # Medium (5-7pts)
        spread_score = 5
    elif abs_spread <= 10:  # Large (7-10pts) - collecting data
        spread_score = 3
    else:  # Very large (>10pts)
        spread_score = 0
    score += spread_score
    breakdown["opening_spread"] = spread_score

    # 4. SCORE DIFFERENTIAL (10 points) - Game still competitive?
    # Closer games have more comeback potential
    abs_diff = abs(score_diff)
    if abs_diff <= 5:  # Very close
        diff_score = 10
    elif abs_diff <= 10:  # Close
        diff_score = 7
    elif abs_diff <= 15:  # Moderate
        diff_score = 4
    else:  # Blowout
        diff_score = 0
    score += diff_score
    breakdown["score_diff"] = diff_score

    # 5. SPORT (10 points) - NBA significantly outperforms NCAAB
    # NBA: 88.7% win rate vs NCAAB: 65.5% win rate
    if "nba" in sport.lower():
        sport_score = 10
    else:  # NCAAB
        sport_score = 5
    score += sport_score
    breakdown["sport"] = sport_score

    return {
        "total_score": score,
        "breakdown": breakdown,
        "grade": "A" if score >= 80 else "B" if score >= 70 else "C" if score >= 60 else "D",
    }


def get_win_probability(sport: str) -> float:
    """Get the win probability for a given sport."""
    return WIN_PROBABILITY_BY_SPORT.get(sport, 0.70)  # Default 70% if unknown


# Store opening lines (in-memory cache)
opening_lines = {}  # game_id -> spread
stopped_games = set()  # games we stopped watching due to time
alerted_games = set()  # games we've already alerted on

# Credit tracking
credit_stats = {
    "api_calls": 0,  # Historical API calls made (~10 credits each)
    "cache_hits": 0,  # Times we used cached data instead
    "skipped_spreads": 0,  # Times we skipped due to spread > 5
}


def estimate_minutes_remaining(commence_time_str, home_score=0, away_score=0):
    """
    Estimate minutes remaining in a basketball game using both time and score.

    Basketball: two 20-minute halves = 40 min game time
    Real time with stoppages/halftime: ~2 hours total

    Uses score as a proxy for game progress when time estimation is uncertain.
    """
    try:
        # Parse commence time
        if commence_time_str.endswith("Z"):
            commence_time_str = commence_time_str[:-1] + "+00:00"
        commence = datetime.fromisoformat(commence_time_str)
        now = datetime.now(timezone.utc)

        elapsed = (now - commence).total_seconds() / 60  # minutes

        # Use score to refine estimation
        total_score = home_score + away_score

        # Early game: low score, lots of time left
        if total_score < 20:
            estimated_remaining = max(35, 40 - elapsed * 0.8)  # Conservative estimate
        # Mid game: moderate score
        elif total_score < 60:
            estimated_remaining = max(20, 35 - elapsed * 0.7)
        # Late game: high score, less time
        elif total_score < 100:
            estimated_remaining = max(10, 25 - elapsed * 0.6)
        # Very late: game probably ending
        else:
            estimated_remaining = max(5, 15 - elapsed * 0.5)

        # Cap at reasonable bounds
        return min(max(estimated_remaining, 5), 40)

    except Exception as e:
        # Fallback to basic time-based estimation
        try:
            if commence_time_str.endswith("Z"):
                commence_time_str = commence_time_str[:-1] + "+00:00"
            commence = datetime.fromisoformat(commence_time_str)
            now = datetime.now(timezone.utc)
            elapsed = (now - commence).total_seconds() / 60
            return max(40 - elapsed, 5)
        except:
            return None


# Cache for ESPN game data (refreshed each poll)
espn_game_cache = {}


def fetch_espn_game_clock(sport="basketball_nba"):
    """
    Fetch live game clock data from ESPN's unofficial API.
    Returns dict mapping team names to game clock info.
    """
    try:
        if sport == "basketball_nba":
            url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
        else:  # NCAAB
            url = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"

        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()

        game_clocks = {}
        for event in data.get("events", []):
            status = event.get("status", {})
            period = status.get("period", 0)
            clock = status.get("displayClock", "")

            # Extract team names
            competitions = event.get("competitions", [])
            if not competitions:
                continue

            comp = competitions[0]
            teams = comp.get("competitors", [])

            if len(teams) >= 2:
                home_team = teams[0].get("team", {}).get("displayName", "")
                away_team = teams[1].get("team", {}).get("displayName", "")

                # Convert clock to minutes remaining
                mins_remaining = None
                if clock and ":" in clock:
                    try:
                        parts = clock.split(":")
                        mins = int(parts[0])
                        secs = int(parts[1])

                        # Basketball: 2 halves of 20 min (college) or 4 quarters of 12 min (NBA)
                        if sport == "basketball_nba":
                            # NBA: 4 quarters of 12 min = 48 min total
                            mins_per_period = 12
                            total_periods = 4
                        else:
                            # College: 2 halves of 20 min = 40 min total
                            mins_per_period = 20
                            total_periods = 2

                        # Calculate time remaining
                        periods_remaining = total_periods - period
                        mins_remaining = (periods_remaining * mins_per_period) + mins + (secs / 60)
                        mins_remaining = max(0, mins_remaining)

                    except (ValueError, IndexError):
                        pass

                # Store for both teams
                if mins_remaining is not None:
                    game_clocks[home_team] = mins_remaining
                    game_clocks[away_team] = mins_remaining

        return game_clocks

    except Exception as e:
        print(f"  ⚠️  ESPN API error: {e}")
        return {}


def get_accurate_time_remaining(home_team, away_team, sport, fallback_estimate=None):
    """
    Get accurate time remaining from ESPN API, with fallback to estimate.
    """
    # Check cache first
    if sport in espn_game_cache:
        clocks = espn_game_cache[sport]

        # Try to match by team name (ESPN might use slightly different names)
        for team in [home_team, away_team]:
            # Try exact match first
            if team in clocks:
                return clocks[team]

            # Try partial match (e.g., "76ers" in "Philadelphia 76ers")
            team_short = team.split()[-1]  # Last word
            for espn_team, mins in clocks.items():
                if team_short in espn_team or espn_team in team:
                    return mins

    # Fall back to estimate
    return fallback_estimate


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


def fetch_opening_line(
    client, game_id, sport="basketball_ncaab", storage=None, current_spread=None
):
    """Fetch opening line from cache or historical API.

    Credit optimization:
    - Check in-memory cache first
    - Check database cache second (persists across restarts)
    - Skip API call if current spread > 5 pts (outside our filter anyway)
    - Only call historical API as last resort (~10 credits each)
    """
    global credit_stats

    # 1. Check in-memory cache
    if game_id in opening_lines:
        credit_stats["cache_hits"] += 1
        return opening_lines[game_id]

    # 2. Check database cache (persists across restarts)
    if storage:
        cached = storage.get_opening_line_cache(game_id)
        if cached:
            result = (cached["spread_value"], cached["spread_team"], cached["home_team"])
            opening_lines[game_id] = result  # Also store in memory
            credit_stats["cache_hits"] += 1
            return result

    # 3. Skip API call if current spread is outside our filter (> 5 pts)
    # No point spending 10 credits for a game we won't bet on anyway
    if current_spread is not None and abs(current_spread) > MAX_OPENING_SPREAD:
        credit_stats["skipped_spreads"] += 1
        return None

    # 4. Call historical API (expensive - ~10 credits)
    credit_stats["api_calls"] += 1
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

                # 5. Save to database cache for future runs
                if storage:
                    storage.save_opening_line_cache(
                        game_id=game_id,
                        sport=sport,
                        spread_value=spread,
                        spread_team=team,
                        home_team=home,
                    )

                return opening_lines[game_id]
    except Exception as e:
        print(f"  Error fetching historical: {e}")

    return None


def main():
    client = OddsAPIClient(os.getenv("ODDS_API_KEY"))
    storage = Storage()
    tracker = HistoricalTracker(storage=storage, client=client)

    print("=" * 70)
    print("🎯 FADE STRATEGY MONITOR")
    print("=" * 70)
    print(f"Strategy: Bet AGAINST large line moves (fade the public)")
    print(f"Quality Score: {MIN_ALERT_SCORE}/100 minimum (weighted multi-factor)")
    print(f"  - Line Movement: 35 pts | Time Remaining: 30 pts (sport-adjusted)")
    print(f"  - Opening Spread: 15 pts (prioritizes tight, allows medium)")
    print(f"  - Score Diff: 10 pts | Sport: 10 pts")
    print(
        f"Filters: {MIN_OPENING_SPREAD}-{MAX_OPENING_SPREAD} pt opening spreads (expanded for data collection)"
    )
    print(f"  Time windows: NBA 12-36 min, NCAAB 10-30 min (optimal ~25-60% of game)")
    print("-" * 70)
    print("Win Probabilities (from backtest):")
    for sport, prob in WIN_PROBABILITY_BY_SPORT.items():
        sport_name = "NBA" if "nba" in sport else "NCAAB"
        bet_size = calculate_bet_size(BANKROLL, prob)
        print(f"  {sport_name}: {prob * 100:.1f}% → ${bet_size:.2f}/bet (1/4 Kelly)")
    print("-" * 70)
    print(f"Bankroll: ${BANKROLL:.0f}")
    print(f"Polling every {POLL_INTERVAL // 60} minutes")
    print(f"Auto-stop: after {MAX_EMPTY_POLLS} empty polls or {MAX_RUNTIME_HOURS}h")
    print("=" * 70)
    print()

    # Track for auto-stop
    empty_poll_count = 0
    start_time = datetime.now()

    try:
        while True:
            # Check max runtime
            elapsed_hours = (datetime.now() - start_time).total_seconds() / 3600
            if elapsed_hours >= MAX_RUNTIME_HOURS:
                print(f"\n⏰ Max runtime ({MAX_RUNTIME_HOURS}h) reached. Stopping...")
                break

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n[{now}] Polling...")

            all_live_games = []

            # Fetch ESPN game clocks for all sports (free, accurate time data)
            print("  Fetching game clocks from ESPN...")
            for sport in SPORTS:
                espn_game_cache[sport] = fetch_espn_game_clock(sport)

            # Poll each sport
            for sport in SPORTS:
                sport_name = "NBA" if "nba" in sport else "NCAAB"

                # Get live odds
                odds_data = client.get_live_odds(
                    sport,
                    markets="spreads",
                    bookmakers="fanduel",
                )

                # Get scores
                scores_data = client.get_scores(sport)

                # Find live games (have scores, not completed)
                for game in odds_data:
                    game_id = game["id"]
                    game["_sport"] = sport  # Tag with sport

                    # Skip games we've already stopped watching
                    if game_id in stopped_games:
                        continue

                    # Check if game is live
                    for score in scores_data:
                        if score["id"] == game_id:
                            if score.get("scores") and not score.get("completed"):
                                game["_scores"] = score.get("scores", [])
                                game["_commence_time"] = score.get("commence_time")

                                # Check time remaining (basic estimate for filtering)
                                commence = score.get("commence_time", "")
                                mins_left = estimate_minutes_remaining(
                                    commence
                                )  # Basic estimate first
                                game["_mins_remaining"] = mins_left

                                if mins_left is not None:
                                    if mins_left < MIN_TIME_REMAINING:
                                        stopped_games.add(game_id)
                                        away = game["away_team"]
                                        home = game["home_team"]
                                        print(
                                            f"  ⏱️  Stopped [{sport_name}]: {away} @ {home} (~{mins_left:.0f} min left)"
                                        )
                                        continue

                                all_live_games.append(game)

            live_games = all_live_games

            print(f"Found {len(live_games)} live games with odds")

            # Track consecutive empty polls
            if len(live_games) == 0:
                empty_poll_count += 1
                print(f"  No live games ({empty_poll_count}/{MAX_EMPTY_POLLS} empty polls)")
                if empty_poll_count >= MAX_EMPTY_POLLS:
                    print("Stopping: too many consecutive empty polls")
                    break
            else:
                empty_poll_count = 0  # Reset when we find games

            # Track alerts in this poll for summary
            poll_alerts = []

            # Monitor up to MAX_GAMES
            monitored = 0
            for game in live_games[:MAX_GAMES]:
                game_id = game["id"]
                home = game["home_team"]
                away = game["away_team"]
                sport = game.get("_sport", "basketball_ncaab")
                sport_name = "NBA" if "nba" in sport else "NCAAB"

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

                # Get accurate time remaining from ESPN, with fallback to estimate
                commence = game.get("_commence_time", "")
                estimated_mins = estimate_minutes_remaining(commence, home_score, away_score)
                mins_left = get_accurate_time_remaining(
                    home, away, sport, fallback_estimate=estimated_mins
                )
                game["_mins_remaining"] = mins_left

                # Show time source in output
                if mins_left == estimated_mins or mins_left is None:
                    time_str = f"(~{mins_left:.0f} min left*)" if mins_left else ""
                else:
                    time_str = f"({mins_left:.1f} min left)" if mins_left else ""

                # Get opening line (with credit optimization)
                opening = fetch_opening_line(
                    client, game_id, sport, storage=storage, current_spread=current_spread
                )

                print(f"\n  [{sport_name}] {away} @ {home} {time_str}")
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

                    # Check if opening spread is in optimal range first
                    opening_spread_abs = abs(open_spread)
                    if (
                        opening_spread_abs < MIN_OPENING_SPREAD
                        or opening_spread_abs > MAX_OPENING_SPREAD
                    ):
                        print(
                            f"  ⏭️  SKIP: Opening spread {open_spread:+.1f} outside optimal range ({MIN_OPENING_SPREAD}-{MAX_OPENING_SPREAD})"
                        )
                        monitored += 1
                        continue

                    # Calculate change
                    if open_spread and open_spread != 0:
                        pct_change = abs(current_spread / open_spread) - 1
                        if abs(current_spread) > abs(open_spread):
                            direction = "widened"
                        else:
                            direction = "narrowed"
                        pct_display = pct_change * 100
                        print(f"  Change: {pct_display:+.1f}% ({direction})")

                        # Calculate opportunity score using weighted factors
                        score_diff = home_score - away_score
                        opp_score = calculate_opportunity_score(
                            pct_change=pct_change,
                            mins_remaining=mins_left,
                            opening_spread=open_spread,
                            score_diff=score_diff,
                            sport=sport,
                        )

                        # Display score breakdown
                        print(
                            f"  📊 Quality Score: {opp_score['total_score']}/100 (Grade {opp_score['grade']})"
                        )
                        print(
                            f"     Line:{opp_score['breakdown']['line_move']} | Time:{opp_score['breakdown']['time_remaining']} | Spread:{opp_score['breakdown']['opening_spread']} | Score:{opp_score['breakdown']['score_diff']} | Sport:{opp_score['breakdown']['sport']}"
                        )

                        if opp_score["total_score"] >= MIN_ALERT_SCORE:
                            print(f"  🚨 ALERT: High-quality opportunity detected!")
                            # FADE STRATEGY: Bet AGAINST the line movement
                            # The line moved TOWARD one team - fade by betting the OTHER team
                            #
                            # Key insight: Compare which team the line moved toward
                            # If home spread got MORE NEGATIVE (or less positive) = line moved toward home
                            # If home spread got MORE POSITIVE (or less negative) = line moved toward away

                            # Normalize spreads to home team perspective
                            if open_team == home:
                                open_home = open_spread
                            else:
                                open_home = -open_spread

                            if spread_team == home:
                                current_home = current_spread
                            else:
                                current_home = -current_spread

                            # Determine which direction the line moved
                            # More negative home spread = line moved TOWARD home (home became bigger favorite)
                            # More positive home spread = line moved TOWARD away (away became bigger favorite)
                            home_spread_change = current_home - open_home

                            if home_spread_change < 0:
                                # Line moved toward HOME - fade by betting AWAY (the underdog)
                                fade_team = away
                                if current_home < 0:
                                    fade_spread = (
                                        -current_home
                                    )  # Away gets opposite of home's negative
                                else:
                                    fade_spread = current_home  # Away gets home's positive spread
                            else:
                                # Line moved toward AWAY - fade by betting HOME (the underdog)
                                fade_team = home
                                fade_spread = current_home  # Home's current spread

                            # Calculate recommended bet size using sport-specific probability
                            win_prob = get_win_probability(sport)
                            bet_size = calculate_bet_size(BANKROLL, win_prob)
                            potential_win = round(bet_size * (100 / 110), 2)

                            print(f"  🎯 FADE BET: {fade_team} {fade_spread:+.1f}")
                            print(
                                f"  💰 WAGER: ${bet_size:.2f} to win ${potential_win:.2f} ({sport_name} {win_prob * 100:.0f}% edge)"
                            )

                            # Add to poll alerts summary
                            poll_alerts.append(
                                {
                                    "sport": sport_name,
                                    "away": away,
                                    "home": home,
                                    "bet_team": fade_team,
                                    "bet_spread": fade_spread,
                                    "bet_size": bet_size,
                                    "potential_win": potential_win,
                                    "mins_left": mins_left,
                                    "pct_change": pct_change,
                                    "quality_score": opp_score["total_score"],
                                    "grade": opp_score["grade"],
                                }
                            )

                            # Record alert and bet (only once per game)
                            if game_id not in alerted_games:
                                tracker.record_alert(
                                    game_id=game_id,
                                    spread_team=fade_team,
                                    current_spread=fade_spread,
                                    opening_spread=open_home,  # Home perspective
                                    pct_change=pct_change,
                                    mins_remaining=mins_left,
                                )
                                alerted_games.add(game_id)
                                print("  📝 Recorded FADE bet for backtesting")
                else:
                    print(f"  Current: {spread_team} {current_spread:+.1f} (no opening line)")

                monitored += 1

            print(f"\n  Credits remaining: {client.requests_remaining}")
            print(f"  ℹ️  Time source: ESPN API (accurate) | * = estimate")

            # Show credit optimization stats
            saved = credit_stats["cache_hits"] + credit_stats["skipped_spreads"]
            if credit_stats["api_calls"] > 0 or saved > 0:
                print(
                    f"  💰 Credit savings: {saved} lookups saved ({credit_stats['cache_hits']} cached, {credit_stats['skipped_spreads']} skipped)"
                )
                print(
                    f"     Historical API calls: {credit_stats['api_calls']} (~{credit_stats['api_calls'] * 10} credits)"
                )

            # Display betting summary at bottom for easy viewing
            if poll_alerts:
                print("\n" + "=" * 70)
                print("🎯 BETTING OPPORTUNITIES THIS POLL")
                print("=" * 70)
                print(f"💡 Minimum Quality Score: {MIN_ALERT_SCORE}/100 (Only Grade B+ or better)")
                print(f"💡 Bet sizes are fixed per sport based on Kelly Criterion:")
                print(
                    f"   - NBA: ${calculate_bet_size(BANKROLL, WIN_PROBABILITY_BY_SPORT['basketball_nba']):.2f}/bet (88.7% historical win rate)"
                )
                print(
                    f"   - NCAAB: ${calculate_bet_size(BANKROLL, WIN_PROBABILITY_BY_SPORT['basketball_ncaab']):.2f}/bet (65.5% historical win rate)"
                )
                print("-" * 70)
                # Sort by quality score (best opportunities first)
                sorted_alerts = sorted(poll_alerts, key=lambda x: x["quality_score"], reverse=True)
                for alert in sorted_alerts:
                    time_display = f"{alert['mins_left']:.1f} min" if alert["mins_left"] else "?"
                    print(f"[{alert['sport']}] {alert['away']} @ {alert['home']}")
                    print(f"  ➤ BET: {alert['bet_team']} {alert['bet_spread']:+.1f}")
                    print(
                        f"  ➤ WAGER: ${alert['bet_size']:.2f} to win ${alert['potential_win']:.2f}"
                    )
                    print(
                        f"  ➤ QUALITY: {alert['quality_score']}/100 (Grade {alert['grade']}) | TIME: {time_display}"
                    )
                    print()
                print("=" * 70)

            # Periodically check for completed games and resolve bets
            results_updated = tracker.fetch_and_record_game_results()
            if results_updated:
                print(f"  📊 Updated {results_updated} game results")

            bets_resolved = tracker.resolve_pending_bets()
            if bets_resolved:
                print(f"  ✅ Resolved {bets_resolved} pending bets")

                # Show current stats
                stats = tracker.get_strategy_stats(
                    min_pct_change=0.75,  # 75% minimum for stats display
                    min_mins_remaining=MIN_TIME_REMAINING,
                )
                if stats["total_bets"] > 0:
                    print(
                        f"  📈 Strategy: {stats['wins']}/{stats['total_bets']} "
                        f"({stats['win_rate'] * 100:.1f}%) | "
                        f"Profit: ${stats['total_profit']:.0f}"
                    )

            print(f"  Next poll in {POLL_INTERVAL // 60} minutes...")

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print("\n\nStopped by user")
        print(f"Final credits remaining: {client.requests_remaining}")

        # Show credit optimization summary
        saved = credit_stats["cache_hits"] + credit_stats["skipped_spreads"]
        credits_saved = saved * 10  # ~10 credits per historical call
        print(f"\n💰 Credit Optimization Summary:")
        print(
            f"  Historical API calls made: {credit_stats['api_calls']} (~{credit_stats['api_calls'] * 10} credits)"
        )
        print(f"  Lookups saved: {saved} (~{credits_saved} credits saved)")
        print(f"    - From cache: {credit_stats['cache_hits']}")
        print(f"    - Skipped (spread > 5): {credit_stats['skipped_spreads']}")

        # Show final stats
        stats = tracker.get_strategy_stats()
        if stats["total_bets"] > 0:
            print("\n📊 Session Statistics:")
            print(f"  Total bets tracked: {stats['total_bets']}")
            print(f"  Wins: {stats['wins']} | Losses: {stats['losses']}")
            print(f"  Win rate: {stats['win_rate'] * 100:.1f}%")
            print(f"  Total profit: ${stats['total_profit']:.0f}")
            print(f"  ROI: {stats['roi'] * 100:.1f}%")


if __name__ == "__main__":
    main()
