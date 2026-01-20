"""Data models for the live odds monitor."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List


@dataclass
class Odds:
    """Represents odds for a game."""
    
    spread_home: Optional[float] = None  # e.g., -3.5
    spread_away: Optional[float] = None  # e.g., +3.5
    spread_home_price: Optional[int] = None  # e.g., -110
    spread_away_price: Optional[int] = None  # e.g., -110
    
    moneyline_home: Optional[int] = None  # e.g., -150
    moneyline_away: Optional[int] = None  # e.g., +130
    
    total: Optional[float] = None  # e.g., 145.5
    over_price: Optional[int] = None  # e.g., -110
    under_price: Optional[int] = None  # e.g., -110
    
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    @classmethod
    def from_api_response(cls, game: dict, bookmaker_key: str = "fanduel") -> "Odds":
        """Create Odds from API response.
        
        Args:
            game: Game dict from API response
            bookmaker_key: Bookmaker to extract odds for
            
        Returns:
            Odds instance
        """
        odds = cls()
        
        for bookmaker in game.get("bookmakers", []):
            if bookmaker["key"] != bookmaker_key:
                continue
                
            for market in bookmaker.get("markets", []):
                if market["key"] == "spreads":
                    for outcome in market.get("outcomes", []):
                        if outcome["name"] == game["home_team"]:
                            odds.spread_home = outcome.get("point")
                            odds.spread_home_price = outcome.get("price")
                        else:
                            odds.spread_away = outcome.get("point")
                            odds.spread_away_price = outcome.get("price")
                            
                elif market["key"] == "h2h":
                    for outcome in market.get("outcomes", []):
                        if outcome["name"] == game["home_team"]:
                            odds.moneyline_home = outcome.get("price")
                        else:
                            odds.moneyline_away = outcome.get("price")
                            
                elif market["key"] == "totals":
                    for outcome in market.get("outcomes", []):
                        if outcome["name"] == "Over":
                            odds.total = outcome.get("point")
                            odds.over_price = outcome.get("price")
                        else:
                            odds.under_price = outcome.get("price")
        
        return odds


@dataclass
class GameScore:
    """Current score and time remaining for a game."""
    
    home_score: int = 0
    away_score: int = 0
    period: Optional[str] = None  # e.g., "1st Half", "2nd Half"
    clock: Optional[str] = None  # e.g., "12:34"
    is_completed: bool = False
    is_live: bool = False
    
    def get_minutes_remaining(self) -> Optional[int]:
        """Estimate minutes remaining in the game.
        
        Returns:
            Estimated minutes remaining, or None if unknown
        """
        if self.is_completed:
            return 0
        if not self.is_live:
            return None
            
        # College basketball: 2 x 20 minute halves
        # Parse clock if available
        if self.clock:
            try:
                parts = self.clock.split(":")
                minutes = int(parts[0])
                
                if self.period and "1" in self.period:
                    # First half: add 20 minutes for second half
                    return minutes + 20
                else:
                    # Second half
                    return minutes
            except (ValueError, IndexError):
                pass
        
        return None


@dataclass
class Game:
    """Represents a game being monitored."""
    
    id: str  # API game ID
    home_team: str
    away_team: str
    commence_time: datetime
    sport: str = "basketball_ncaab"
    
    # Opening odds (fetched from historical API)
    opening_odds: Optional[Odds] = None
    
    # Current live odds
    current_odds: Optional[Odds] = None
    
    # Current score/time
    score: Optional[GameScore] = None
    
    # Tracking
    last_updated: Optional[datetime] = None
    alerts_sent: List[str] = field(default_factory=list)  # List of alert types sent
    
    @property
    def is_live(self) -> bool:
        """Check if game is currently in progress."""
        if self.score:
            return self.score.is_live
        now = datetime.utcnow()
        return self.commence_time <= now
    
    @property
    def spread_change(self) -> Optional[float]:
        """Calculate spread change from open.
        
        Returns:
            Percentage change in spread, or None if data unavailable
        """
        if not self.opening_odds or not self.current_odds:
            return None
        if not self.opening_odds.spread_home or not self.current_odds.spread_home:
            return None
        if self.opening_odds.spread_home == 0:
            return None
            
        return abs(self.current_odds.spread_home / self.opening_odds.spread_home) - 1
    
    def get_spread_summary(self) -> str:
        """Get human-readable spread change summary."""
        if not self.opening_odds or not self.current_odds:
            return "No spread data"
            
        open_spread = self.opening_odds.spread_home
        curr_spread = self.current_odds.spread_home
        
        if open_spread is None or curr_spread is None:
            return "No spread data"
            
        change_pct = self.spread_change
        if change_pct is None:
            return "No spread data"
            
        return (
            f"{self.home_team}: opened {open_spread:+.1f}, "
            f"now {curr_spread:+.1f} ({change_pct*100:+.0f}%)"
        )


@dataclass
class Alert:
    """Represents an alert to be sent."""
    
    game: Game
    alert_type: str  # "spread_change", "total_change", etc.
    message: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    sent: bool = False
    
    @classmethod
    def spread_alert(cls, game: Game) -> "Alert":
        """Create a spread change alert."""
        mins_left = None
        if game.score:
            mins_left = game.score.get_minutes_remaining()
        
        time_str = f"{mins_left} min left" if mins_left else "time unknown"
        
        message = (
            f"🚨 SPREAD ALERT 🚨\n"
            f"{game.away_team} @ {game.home_team}\n"
            f"{game.get_spread_summary()}\n"
            f"Time remaining: {time_str}"
        )
        
        return cls(
            game=game,
            alert_type="spread_change",
            message=message
        )
