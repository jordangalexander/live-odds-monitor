"""Main monitoring service for live odds tracking."""

import time
import signal
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

from ..api.odds_api import OddsAPIClient
from ..db.models import Game, Odds, GameScore, Alert
from ..db.storage import Storage
from ..data_store import OpeningLinesStore, OpeningLine, OddsSnapshotStore
from .alerts import AlertManager, create_default_alert_manager
from ..config import MonitorConfig, default_config


logger = logging.getLogger(__name__)


class OddsMonitor:
    """Monitors live odds and detects betting opportunities."""
    
    def __init__(
        self,
        config: MonitorConfig = None,
        api_client: OddsAPIClient = None,
        storage: Storage = None,
        alert_manager: AlertManager = None,
        opening_lines_store: OpeningLinesStore = None,
        odds_snapshot_store: OddsSnapshotStore = None,
    ):
        """Initialize the monitor.
        
        Args:
            config: Monitor configuration
            api_client: Odds API client
            storage: Database storage (SQLite for session data)
            alert_manager: Alert manager
            opening_lines_store: Parquet store for opening lines (persistent)
            odds_snapshot_store: Parquet store for odds time-series (optional)
        """
        self.config = config or default_config
        self.client = api_client or OddsAPIClient()
        self.storage = storage or Storage(self.config.db_path)
        self.alert_manager = alert_manager or create_default_alert_manager()
        
        # Parquet-based stores for persistent data
        self.opening_lines = opening_lines_store or OpeningLinesStore()
        self.odds_snapshots = odds_snapshot_store  # Optional, can be None
        
        # Preload current season's opening lines into memory
        self.opening_lines.preload(self.config.sport)
        
        self.games: Dict[str, Game] = {}  # game_id -> Game
        self.running = False
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
    
    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals."""
        logger.info("Shutting down monitor...")
        self.running = False
        
        # Flush any pending data to disk
        self.opening_lines.close()
        if self.odds_snapshots:
            self.odds_snapshots.close()
    
    def _is_watched_game(self, game_data: dict) -> bool:
        """Check if a game involves watched teams.
        
        Args:
            game_data: Game data from API
            
        Returns:
            True if either team is watched
        """
        return (
            self.config.is_team_watched(game_data["home_team"]) or
            self.config.is_team_watched(game_data["away_team"])
        )
    
    def _fetch_opening_odds(self, game: Game) -> Optional[Odds]:
        """Fetch opening odds for a game.
        
        Lookup priority:
        1. Parquet store (persistent, fast O(1) lookup)
        2. SQLite store (legacy fallback)
        3. Historical API (expensive - 30 credits!)
        
        Args:
            game: Game to fetch opening odds for
            
        Returns:
            Opening odds or None if not available
        """
        # 1. Check Parquet store first (fast, persistent)
        cached = self.opening_lines.get(game.id, sport=self.config.sport)
        if cached:
            logger.debug(f"Using cached opening odds for {game.id}")
            return Odds(
                spread_home=cached.get('spread_home'),
                spread_away=cached.get('spread_away'),
                spread_home_price=cached.get('spread_home_price'),
                spread_away_price=cached.get('spread_away_price'),
                total=cached.get('total'),
                over_price=cached.get('over_price'),
                under_price=cached.get('under_price'),
                moneyline_home=cached.get('moneyline_home'),
                moneyline_away=cached.get('moneyline_away'),
            )
        
        # 2. Check SQLite store (legacy fallback)
        stored_odds = self.storage.get_opening_odds(game.id)
        if stored_odds:
            logger.debug(f"Using SQLite opening odds for {game.id}")
            # Migrate to Parquet store for next time
            self._save_opening_line(game, stored_odds, source="migrated")
            return stored_odds
        
        # 3. Fetch from historical API (expensive - 30 credits!)
        # Look for odds from 24 hours before game time
        try:
            target_time = game.commence_time - timedelta(hours=24)
            
            # Don't fetch historical if game already started
            if datetime.utcnow() > game.commence_time:
                logger.info(f"Game {game.id} already started, using current as 'opening'")
                return None
            
            logger.info(f"Fetching historical odds for {game.away_team} @ {game.home_team}")
            
            historical = self.client.get_historical_event_odds(
                event_id=game.id,
                sport=self.config.sport,
                date=target_time,
                markets=self.config.markets
            )
            
            if historical and "data" in historical:
                odds = Odds.from_api_response(
                    historical["data"],
                    self.config.bookmaker
                )
                
                # Save to both stores
                self.storage.save_opening_odds(game.id, odds)
                self._save_opening_line(game, odds, source="historical_api")
                
                return odds
                
        except Exception as e:
            logger.error(f"Failed to fetch historical odds for {game.id}: {e}")
        
        return None
    
    def _save_opening_line(self, game: Game, odds: Odds, source: str) -> None:
        """Save opening line to Parquet store."""
        season = self.opening_lines._get_season(game.commence_time)
        
        line = OpeningLine(
            game_id=game.id,
            sport=self.config.sport,
            season=season,
            home_team=game.home_team,
            away_team=game.away_team,
            commence_time=game.commence_time,
            spread_home=odds.spread_home,
            spread_away=odds.spread_away,
            spread_home_price=odds.spread_home_price,
            spread_away_price=odds.spread_away_price,
            total=odds.total,
            over_price=odds.over_price,
            under_price=odds.under_price,
            moneyline_home=odds.moneyline_home,
            moneyline_away=odds.moneyline_away,
            bookmaker=self.config.bookmaker,
            captured_at=datetime.utcnow(),
            source=source,
        )
        
        self.opening_lines.save(line)
    
    def _update_game_score(self, game: Game, scores_data: List[dict]) -> None:
        """Update game score from scores API data.
        
        Args:
            game: Game to update
            scores_data: Scores data from API
        """
        for score_data in scores_data:
            if score_data["id"] == game.id:
                scores = score_data.get("scores", [])
                
                home_score = 0
                away_score = 0
                for s in scores or []:
                    if s["name"] == game.home_team:
                        home_score = int(s.get("score", 0) or 0)
                    else:
                        away_score = int(s.get("score", 0) or 0)
                
                game.score = GameScore(
                    home_score=home_score,
                    away_score=away_score,
                    is_completed=score_data.get("completed", False),
                    is_live=not score_data.get("completed", False) and scores is not None
                )
                break
    
    def _check_for_alerts(self, game: Game) -> List[Alert]:
        """Check if game warrants any alerts.
        
        Args:
            game: Game to check
            
        Returns:
            List of alerts to send
        """
        alerts = []
        
        # Skip if we don't have opening odds
        if not game.opening_odds or not game.current_odds:
            return alerts
        
        # Skip if game is completed
        if game.score and game.score.is_completed:
            return alerts
        
        # Check time remaining
        mins_remaining = None
        if game.score:
            mins_remaining = game.score.get_minutes_remaining()
            
            if mins_remaining is not None and mins_remaining < self.config.min_time_remaining_minutes:
                logger.debug(f"Game {game.id} has only {mins_remaining} min left, skipping")
                return alerts
        
        # Check spread change (need valid spreads to compare)
        if (
            game.opening_odds.spread_home is not None
            and game.current_odds.spread_home is not None
            and self.config.should_alert(
                game.opening_odds.spread_home,
                game.current_odds.spread_home
            )
        ):
            # Check if we already sent this alert
            if not self.storage.has_alert_been_sent(game.id, "spread_change"):
                alert = Alert.spread_alert(game)
                alerts.append(alert)
        
        return alerts
    
    def _poll_once(self) -> None:
        """Execute one polling cycle."""
        logger.info("Polling for updates...")
        
        try:
            # Fetch live odds
            odds_data = self.client.get_live_odds(
                sport=self.config.sport,
                markets=self.config.markets,
                bookmakers=self.config.bookmaker
            )
            
            logger.info(f"Found {len(odds_data)} games, {self.client.requests_remaining} API credits remaining")
            
            # Fetch scores for live games
            scores_data = self.client.get_scores(sport=self.config.sport)
            
            # Process each game
            watched_count = 0
            for game_data in odds_data:
                # Filter by watchlist
                if not self._is_watched_game(game_data):
                    continue
                
                watched_count += 1
                game_id = game_data["id"]
                
                # Get or create game
                if game_id not in self.games:
                    game = Game(
                        id=game_id,
                        home_team=game_data["home_team"],
                        away_team=game_data["away_team"],
                        commence_time=datetime.fromisoformat(
                            game_data["commence_time"].replace("Z", "+00:00")
                        ).replace(tzinfo=None),
                        sport=self.config.sport
                    )
                    self.games[game_id] = game
                    self.storage.save_game(game)
                    
                    # Fetch opening odds (expensive, only do once)
                    game.opening_odds = self._fetch_opening_odds(game)
                else:
                    game = self.games[game_id]
                
                # Update current odds
                current_odds = Odds.from_api_response(game_data, self.config.bookmaker)
                game.current_odds = current_odds
                game.last_updated = datetime.utcnow()
                
                # Record odds history
                self.storage.record_odds(game_id, current_odds)
                
                # Update score
                self._update_game_score(game, scores_data)
                
                # Check for alerts
                for alert in self._check_for_alerts(game):
                    if self.alert_manager.send_alert(alert):
                        self.storage.save_alert(alert)
                        logger.info(f"Alert sent for {game.away_team} @ {game.home_team}")
            
            logger.info(f"Monitoring {watched_count} watched games")
            
        except Exception as e:
            logger.error(f"Error during polling: {e}")
    
    def run(self) -> None:
        """Start the monitoring loop."""
        self.running = True
        logger.info(f"Starting odds monitor...")
        logger.info(f"Watching teams: {', '.join(self.config.watchlist[:5])}...")
        logger.info(f"Poll interval: {self.config.poll_interval_seconds}s")
        logger.info(f"Spread threshold: {self.config.spread_change_threshold*100:.0f}% change")
        logger.info(f"Min time remaining: {self.config.min_time_remaining_minutes} min")
        
        while self.running:
            self._poll_once()
            
            # Wait for next poll
            logger.info(f"Sleeping for {self.config.poll_interval_seconds}s...")
            
            # Sleep in small intervals to allow for shutdown
            for _ in range(self.config.poll_interval_seconds):
                if not self.running:
                    break
                time.sleep(1)
        
        logger.info("Monitor stopped.")
        self.client.close()
    
    def run_once(self) -> None:
        """Run a single poll cycle (useful for testing)."""
        self._poll_once()
        self.client.close()


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Live Odds Monitor")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument("--interval", type=int, help="Poll interval in seconds")
    args = parser.parse_args()
    
    config = MonitorConfig()
    if args.interval:
        config.poll_interval_seconds = args.interval
    
    monitor = OddsMonitor(config=config)
    
    if args.once:
        monitor.run_once()
    else:
        monitor.run()


if __name__ == "__main__":
    main()
