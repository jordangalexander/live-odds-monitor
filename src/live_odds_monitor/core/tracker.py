"""Historical tracking for backtesting betting strategies."""

import os

from ..api.odds_api import OddsAPIClient
from ..db.storage import Storage


class HistoricalTracker:
    """Tracks line movements and game outcomes for backtesting."""

    def __init__(
        self,
        storage: Storage = None,
        client: OddsAPIClient = None,
    ):
        """Initialize tracker.

        Args:
            storage: Storage instance (creates one if not provided)
            client: API client (creates one if not provided)
        """
        self.storage = storage or Storage()
        self.client = client or OddsAPIClient(os.getenv("ODDS_API_KEY"))

    def record_opening_line(
        self,
        game_id: str,
        spread_team: str,
        spread_value: float,
        spread_price: int = -110,
        bookmaker: str = "fanduel",
    ) -> None:
        """Record opening line for a game.

        Only records if we don't already have an opening line.
        """
        existing = self.storage.get_opening_snapshot(game_id)
        if existing:
            return

        self.storage.save_line_snapshot(
            game_id=game_id,
            bookmaker=bookmaker,
            spread_team=spread_team,
            spread_value=spread_value,
            spread_price=spread_price,
            is_opening=True,
        )

    def record_live_line(
        self,
        game_id: str,
        spread_team: str,
        spread_value: float,
        spread_price: int = -110,
        home_score: int = 0,
        away_score: int = 0,
        mins_remaining: float = None,
        bookmaker: str = "fanduel",
    ) -> None:
        """Record a live line snapshot."""
        self.storage.save_line_snapshot(
            game_id=game_id,
            bookmaker=bookmaker,
            spread_team=spread_team,
            spread_value=spread_value,
            spread_price=spread_price,
            home_score=home_score,
            away_score=away_score,
            mins_remaining=mins_remaining,
            is_opening=False,
        )

    def record_alert(
        self,
        game_id: str,
        spread_team: str,
        current_spread: float,
        opening_spread: float,
        pct_change: float,
        mins_remaining: float = None,
    ) -> int:
        """Record an alert and create a hypothetical bet.

        Returns the bet_id for later outcome tracking.
        """
        # Save the alert
        message = (
            f"Spread moved {pct_change * 100:.0f}% from {opening_spread:+.1f} "
            f"to {current_spread:+.1f} ({spread_team})"
        )
        alert_id = self.storage.save_alert_with_id(
            game_id=game_id,
            alert_type="spread_change",
            message=message,
        )

        # Record hypothetical bet (betting the current spread)
        bet_id = self.storage.save_bet_outcome(
            game_id=game_id,
            bet_type="spread",
            bet_team=spread_team,
            bet_spread=current_spread,
            opening_spread=opening_spread,
            pct_change=pct_change,
            mins_remaining=mins_remaining,
            alert_id=alert_id,
        )

        return bet_id

    def fetch_and_record_game_results(
        self,
        sport: str = "basketball_ncaab",
        days_back: int = 3,
    ) -> int:
        """Fetch completed games and record results.

        Returns number of games updated.
        """
        # Get games needing results
        pending_games = self.storage.get_games_needing_results()

        if not pending_games:
            return 0

        # Fetch scores from API (completed games)
        scores = self.client.get_scores(sport, days_from=days_back)

        updated = 0
        for game in pending_games:
            game_id = game["id"]

            # Find matching score
            for score in scores:
                if score["id"] == game_id and score.get("completed"):
                    home_score = None
                    away_score = None

                    for s in score.get("scores", []):
                        if s["name"] == game["home_team"]:
                            home_score = int(s.get("score", 0))
                        else:
                            away_score = int(s.get("score", 0))

                    if home_score is not None and away_score is not None:
                        self.storage.save_game_result(
                            game_id=game_id,
                            final_score_home=home_score,
                            final_score_away=away_score,
                        )
                        updated += 1
                    break

        return updated

    def resolve_pending_bets(self) -> int:
        """Resolve pending bets using game results.

        Returns number of bets resolved.
        """
        pending = self.storage.get_pending_bets()
        resolved = 0

        for bet in pending:
            result = self.storage.get_game_result(bet["game_id"])
            if not result:
                continue

            # Calculate margin (home score - away score)
            margin = result["final_score_home"] - result["final_score_away"]

            # Determine if bet covered
            # If bet_team is home team, they need to win by more than spread
            # If bet_team is away team, they need to lose by less than spread
            bet_team = bet["bet_team"]
            bet_spread = bet["bet_spread"]
            home_team = bet.get("home_team", "")

            if bet_team == home_team or home_team.startswith(bet_team.split()[0]):
                # Bet on home team
                # Home spread of -5 means home needs to win by >5
                adjusted_margin = margin + bet_spread
                covered = adjusted_margin > 0
            else:
                # Bet on away team
                # Away spread of +5 means away can lose by <5
                adjusted_margin = -margin + bet_spread
                covered = adjusted_margin > 0

            # Standard -110 odds: win $100 on $110 bet
            # Profit: +$100 if win, -$110 if lose
            profit = 100.0 if covered else -110.0

            self.storage.update_bet_outcome(
                bet_id=bet["id"],
                final_margin=margin,
                covered=covered,
                profit=profit,
            )
            resolved += 1

        return resolved

    def get_strategy_stats(
        self,
        min_pct_change: float = None,
        min_mins_remaining: float = None,
    ) -> dict:
        """Get statistics for the betting strategy.

        Args:
            min_pct_change: Filter by minimum % change (e.g., 1.0 for 100%)
            min_mins_remaining: Filter by minimum time remaining

        Returns:
            Dict with strategy statistics
        """
        all_bets = self.storage.get_all_bets()

        # Filter to resolved bets only
        resolved = [b for b in all_bets if b.get("covered") is not None]

        # Apply filters
        if min_pct_change is not None:
            resolved = [
                b for b in resolved
                if b.get("pct_change", 0) >= min_pct_change
            ]

        if min_mins_remaining is not None:
            resolved = [
                b for b in resolved
                if (b.get("mins_remaining") or 0) >= min_mins_remaining
            ]

        if not resolved:
            return {
                "total_bets": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0,
                "total_profit": 0,
                "roi": 0,
            }

        wins = sum(1 for b in resolved if b["covered"])
        losses = len(resolved) - wins
        total_profit = sum(b.get("profit", 0) for b in resolved)
        total_wagered = len(resolved) * 110  # $110 per bet at -110 odds

        return {
            "total_bets": len(resolved),
            "wins": wins,
            "losses": losses,
            "win_rate": wins / len(resolved) if resolved else 0,
            "total_profit": total_profit,
            "roi": total_profit / total_wagered if total_wagered else 0,
        }
