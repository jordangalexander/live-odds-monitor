"""Configuration settings for the live odds monitor."""

from dataclasses import dataclass, field


@dataclass
class MonitorConfig:
    """Configuration for the odds monitor."""

    # Teams to watch (case-insensitive partial match)
    # Add team names or partial names to monitor
    watchlist: list[str] = field(default_factory=lambda: [
        # Top 25 teams (update as needed)
        "Auburn",
        "Duke",
        "Iowa St",
        "Alabama",
        "Florida",
        "Houston",
        "Tennessee",
        "Michigan St",
        "Kentucky",
        "Texas A&M",
        "Oregon",
        "Marquette",
        "Purdue",
        "St. John's",
        "Texas Tech",
        "Wisconsin",
        "UConn",
        "Kansas",
        "UCLA",
        "Memphis",
        "Oklahoma",
        "Missouri",
        "Mississippi St",
        "Louisville",
        "Clemson",
    ])

    # === OPTIMAL PARAMETERS (from backtest analysis) ===
    # Best combo: ≥200% change + medium spread (5-10) + NBA = 100% win rate
    
    # Alert thresholds
    spread_change_threshold: float = 2.0  # 200% change (best performing)
    min_time_remaining_minutes: int = 10  # Only alert if 10+ min left

    # Spread size filters (in points)
    # Very Large (12+) showed +4.1% ROI, Medium (5-10) + NBA = +90.9% ROI
    min_opening_spread: float = 5.0  # Minimum opening spread size
    max_opening_spread: float = 15.0  # Maximum opening spread size (None = no limit)

    # Sport preference (NBA showed better results with filters)
    prefer_nba: bool = True  # Prioritize NBA over NCAAB

    # Polling settings
    poll_interval_seconds: int = 120  # Poll every 2 minutes

    # Sports to monitor
    sport: str = "basketball_ncaab"

    # Bookmaker to use for odds
    bookmaker: str = "fanduel"

    # Markets to track
    markets: str = "h2h,spreads,totals"

    # Database path
    db_path: str = "odds_monitor.db"

    def is_team_watched(self, team_name: str) -> bool:
        """Check if a team is in the watchlist.

        Args:
            team_name: Full team name from the API

        Returns:
            True if team matches any watchlist entry
        """
        team_lower = team_name.lower()
        for watched in self.watchlist:
            if watched.lower() in team_lower:
                return True
        return False

    def should_alert(
        self,
        opening_spread: float | None,
        current_spread: float | None,
    ) -> bool:
        """Check if spread change warrants an alert.

        Args:
            opening_spread: Opening spread (e.g., -3.0)
            current_spread: Current spread (e.g., -6.0)

        Returns:
            True if spread has changed by threshold amount
        """
        if opening_spread is None or current_spread is None:
            return False
        if opening_spread == 0:
            return False

        # Calculate percentage change in absolute spread
        pct_change = abs(current_spread / opening_spread) - 1
        return pct_change >= self.spread_change_threshold

    def is_optimal_bet(
        self,
        opening_spread: float,
        pct_change: float,
        sport: str,
    ) -> tuple[bool, str]:
        """Check if a potential bet meets the optimal criteria from backtest.

        Based on analysis of 129 bets:
        - Best combo: ≥200% + medium spread (5-10) + NBA = 100% win rate
        - Very large spreads (12+) also showed profitability

        Args:
            opening_spread: Absolute value of opening spread
            pct_change: Percentage change (e.g., 2.0 = 200%)
            sport: Sport key (e.g., 'basketball_nba')

        Returns:
            Tuple of (is_optimal, reason_string)
        """
        reasons = []

        # Check threshold
        if pct_change < self.spread_change_threshold:
            return False, f"Change {pct_change*100:.0f}% below {self.spread_change_threshold*100:.0f}%"

        # Check spread size
        abs_spread = abs(opening_spread)
        if abs_spread < self.min_opening_spread:
            return False, f"Spread {abs_spread:.1f} below min {self.min_opening_spread:.1f}"

        if self.max_opening_spread and abs_spread > self.max_opening_spread:
            return False, f"Spread {abs_spread:.1f} above max {self.max_opening_spread:.1f}"

        # All checks passed
        is_nba = "nba" in sport.lower() and "ncaab" not in sport.lower()

        if pct_change >= 2.0 and 5.0 <= abs_spread <= 10.0 and is_nba:
            reasons.append("🎯 OPTIMAL: ≥200% + medium spread + NBA (100% win rate in backtest)")
        elif abs_spread >= 12.0:
            reasons.append("📊 Good: Large spread (54.5% win rate in backtest)")
        else:
            reasons.append("✅ Meets threshold criteria")

        return True, " | ".join(reasons)


# Default configuration instance
default_config = MonitorConfig()
