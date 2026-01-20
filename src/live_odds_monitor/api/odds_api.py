"""The Odds API client for fetching live and historical odds."""

import os
from datetime import datetime
from typing import Optional, List, Dict, Any
import httpx


class OddsAPIClient:
    """Client for The Odds API."""
    
    BASE_URL = "https://api.the-odds-api.com/v4"
    NCAAB_SPORT = "basketball_ncaab"
    NCAAF_SPORT = "americanfootball_ncaaf"
    NFL_SPORT = "americanfootball_nfl"
    NBA_SPORT = "basketball_nba"
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the Odds API client.
        
        Args:
            api_key: The Odds API key. If not provided, will look for ODDS_API_KEY env var.
        """
        self.api_key = api_key or os.getenv("ODDS_API_KEY")
        if not self.api_key:
            raise ValueError(
                "API key is required. Set ODDS_API_KEY environment variable or pass api_key parameter."
            )
        self.client = httpx.Client(timeout=30.0)
        self._requests_remaining: Optional[int] = None
        self._requests_used: Optional[int] = None
    
    @property
    def requests_remaining(self) -> Optional[int]:
        """Get remaining API requests from last call."""
        return self._requests_remaining
    
    @property
    def requests_used(self) -> Optional[int]:
        """Get used API requests from last call."""
        return self._requests_used
    
    def _update_quota(self, response: httpx.Response) -> None:
        """Update quota tracking from response headers."""
        if "x-requests-remaining" in response.headers:
            self._requests_remaining = int(response.headers["x-requests-remaining"])
        if "x-requests-used" in response.headers:
            self._requests_used = int(response.headers["x-requests-used"])
    
    def get_live_odds(
        self,
        sport: str = None,
        regions: str = "us",
        markets: str = "h2h,spreads,totals",
        bookmakers: str = "fanduel"
    ) -> List[Dict[str, Any]]:
        """Get live odds for games.
        
        Args:
            sport: Sport key (default: basketball_ncaab)
            regions: Comma-separated regions (default: "us")
            markets: Comma-separated markets (default: "h2h,spreads,totals")
            bookmakers: Comma-separated bookmakers (default: "fanduel")
        
        Returns:
            List of games with odds data
        """
        sport_key = sport or self.NCAAB_SPORT
        url = f"{self.BASE_URL}/sports/{sport_key}/odds"
        params = {
            "apiKey": self.api_key,
            "regions": regions,
            "markets": markets,
            "bookmakers": bookmakers,
            "oddsFormat": "american",
        }
        
        response = self.client.get(url, params=params)
        response.raise_for_status()
        self._update_quota(response)
        
        return response.json()
    
    def get_scores(
        self,
        sport: str = None,
        days_from: int = None
    ) -> List[Dict[str, Any]]:
        """Get live scores and game status.
        
        Args:
            sport: Sport key (default: basketball_ncaab)
            days_from: Optional, get completed games from N days ago (1-3)
        
        Returns:
            List of games with scores
        """
        sport_key = sport or self.NCAAB_SPORT
        url = f"{self.BASE_URL}/sports/{sport_key}/scores"
        params = {
            "apiKey": self.api_key,
        }
        if days_from:
            params["daysFrom"] = days_from
        
        response = self.client.get(url, params=params)
        response.raise_for_status()
        self._update_quota(response)
        
        return response.json()
    
    def get_events(
        self,
        sport: str = None,
        commence_time_from: datetime = None,
        commence_time_to: datetime = None,
    ) -> List[Dict[str, Any]]:
        """Get upcoming events without odds (free endpoint).
        
        Args:
            sport: Sport key (default: basketball_ncaab)
            commence_time_from: Filter games starting after this time
            commence_time_to: Filter games starting before this time
        
        Returns:
            List of events
        """
        sport_key = sport or self.NCAAB_SPORT
        url = f"{self.BASE_URL}/sports/{sport_key}/events"
        params = {
            "apiKey": self.api_key,
        }
        if commence_time_from:
            params["commenceTimeFrom"] = commence_time_from.strftime("%Y-%m-%dT%H:%M:%SZ")
        if commence_time_to:
            params["commenceTimeTo"] = commence_time_to.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        response = self.client.get(url, params=params)
        response.raise_for_status()
        self._update_quota(response)
        
        return response.json()
    
    def get_historical_odds(
        self,
        sport: str = None,
        date: datetime = None,
        regions: str = "us",
        markets: str = "h2h,spreads,totals",
        bookmakers: str = "fanduel"
    ) -> Dict[str, Any]:
        """Get historical odds snapshot.
        
        Note: Costs 10x more than regular odds calls!
        
        Args:
            sport: Sport key (default: basketball_ncaab)
            date: Timestamp to fetch odds for (ISO 8601)
            regions: Comma-separated regions (default: "us")
            markets: Comma-separated markets (default: "h2h,spreads,totals")
            bookmakers: Comma-separated bookmakers (default: "fanduel")
        
        Returns:
            Historical odds snapshot with timestamp info
        """
        sport_key = sport or self.NCAAB_SPORT
        url = f"{self.BASE_URL}/historical/sports/{sport_key}/odds"
        
        params = {
            "apiKey": self.api_key,
            "regions": regions,
            "markets": markets,
            "bookmakers": bookmakers,
            "oddsFormat": "american",
        }
        if date:
            params["date"] = date.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        response = self.client.get(url, params=params)
        response.raise_for_status()
        self._update_quota(response)
        
        return response.json()
    
    def get_historical_event_odds(
        self,
        event_id: str,
        sport: str = None,
        date: datetime = None,
        regions: str = "us",
        markets: str = "h2h,spreads,totals",
    ) -> Dict[str, Any]:
        """Get historical odds for a specific event.
        
        Note: Costs 10x more than regular odds calls!
        
        Args:
            event_id: The event/game ID
            sport: Sport key (default: basketball_ncaab)
            date: Timestamp to fetch odds for (ISO 8601)
            regions: Comma-separated regions (default: "us")
            markets: Comma-separated markets (default: "h2h,spreads,totals")
        
        Returns:
            Historical odds for the event
        """
        sport_key = sport or self.NCAAB_SPORT
        url = f"{self.BASE_URL}/historical/sports/{sport_key}/events/{event_id}/odds"
        
        params = {
            "apiKey": self.api_key,
            "regions": regions,
            "markets": markets,
            "oddsFormat": "american",
        }
        if date:
            params["date"] = date.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        response = self.client.get(url, params=params)
        response.raise_for_status()
        self._update_quota(response)
        
        return response.json()
    
    def get_event_odds(
        self,
        event_id: str,
        sport: str = None,
        regions: str = "us",
        markets: str = "h2h,spreads,totals",
        bookmakers: str = "fanduel"
    ) -> Dict[str, Any]:
        """Get current odds for a specific event.
        
        Args:
            event_id: The event/game ID
            sport: Sport key (default: basketball_ncaab)
            regions: Comma-separated regions (default: "us")
            markets: Comma-separated markets (default: "h2h,spreads,totals")
            bookmakers: Comma-separated bookmakers (default: "fanduel")
        
        Returns:
            Current odds for the event
        """
        sport_key = sport or self.NCAAB_SPORT
        url = f"{self.BASE_URL}/sports/{sport_key}/events/{event_id}/odds"
        
        params = {
            "apiKey": self.api_key,
            "regions": regions,
            "markets": markets,
            "bookmakers": bookmakers,
            "oddsFormat": "american",
        }
        
        response = self.client.get(url, params=params)
        response.raise_for_status()
        self._update_quota(response)
        
        return response.json()
    
    def get_all_sports_odds(
        self,
        sports: list = None,
        regions: str = "us",
        markets: str = "h2h,spreads,totals",
        bookmakers: str = "fanduel"
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Get live odds for multiple sports.
        
        Args:
            sports: List of sport keys (default: [NCAAB, NCAAF, NFL, NBA])
            regions: Comma-separated regions (default: "us")
            markets: Comma-separated markets (default: "h2h,spreads,totals")
            bookmakers: Comma-separated bookmakers (default: "fanduel")
        
        Returns:
            Dictionary with sport keys as keys and odds data as values
        """
        if sports is None:
            sports = [self.NCAAB_SPORT, self.NCAAF_SPORT, self.NFL_SPORT, self.NBA_SPORT]
        
        all_odds = {}
        for sport in sports:
            try:
                odds = self.get_live_odds(
                    sport=sport,
                    regions=regions,
                    markets=markets,
                    bookmakers=bookmakers
                )
                all_odds[sport] = odds
            except Exception as e:
                print(f"Error fetching {sport}: {e}")
                all_odds[sport] = []
        
        return all_odds
    
    def close(self):
        """Close the HTTP client."""
        self.client.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
