"""
Data storage layer for opening lines and historical odds.

Design Philosophy:
- Opening lines are immutable once captured (write once, read forever)
- Use Parquet for efficient columnar storage and analytics
- Partition by sport and season for manageable file sizes
- Use DuckDB for fast analytical queries when needed
- Keep session/operational data in SQLite (alerts, current state)

Directory Structure:
    data/
    ├── opening_lines/           # Parquet files (persistent, valuable)
    │   ├── basketball_ncaab/
    │   │   ├── 2025-2026.parquet
    │   │   └── 2024-2025.parquet
    │   ├── basketball_nba/
    │   │   └── 2025-2026.parquet
    │   └── football_nfl/
    │       └── 2025-2026.parquet
    ├── odds_snapshots/          # Time-series odds data (optional, for research)
    │   └── basketball_ncaab/
    │       └── 2025-2026.parquet
    └── session.db               # SQLite for operational data
"""

from dataclasses import dataclass, asdict
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List, Dict, Any
import logging

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class OpeningLine:
    """A single opening line record."""
    game_id: str
    sport: str
    season: str
    home_team: str
    away_team: str
    commence_time: datetime
    
    # Opening odds (from historical API or first seen)
    spread_home: Optional[float]
    spread_away: Optional[float]
    spread_home_price: Optional[int]
    spread_away_price: Optional[int]
    total: Optional[float]
    over_price: Optional[int]
    under_price: Optional[int]
    moneyline_home: Optional[int]
    moneyline_away: Optional[int]
    
    # Metadata
    bookmaker: str
    captured_at: datetime
    source: str  # "historical_api" or "first_seen"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for DataFrame creation."""
        d = asdict(self)
        # Ensure datetime objects are properly handled
        d['commence_time'] = self.commence_time
        d['captured_at'] = self.captured_at
        return d


class OpeningLinesStore:
    """
    Persistent storage for opening lines using Parquet files.
    
    Opening lines are valuable and should never be refetched once captured.
    This store provides:
    - Fast lookups by game_id during live monitoring
    - Efficient storage using columnar Parquet format
    - Partitioning by sport/season for scalability
    - In-memory caching for current season data
    
    Usage:
        store = OpeningLinesStore("data/opening_lines")
        
        # Check if we have opening line
        line = store.get(game_id, sport="basketball_ncaab")
        
        # Save a new opening line
        store.save(opening_line)
        
        # Bulk analytics
        df = store.load_season("basketball_ncaab", "2025-2026")
    """
    
    def __init__(self, base_path: str = "data/opening_lines"):
        """Initialize the opening lines store.
        
        Args:
            base_path: Base directory for Parquet files
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # In-memory cache for fast lookups during monitoring
        # Key: (sport, season) -> DataFrame
        self._cache: Dict[tuple, pd.DataFrame] = {}
        
        # Index for O(1) lookups: game_id -> OpeningLine dict
        self._index: Dict[str, Dict[str, Any]] = {}
        
        # Track pending writes for batch saving
        self._pending: List[OpeningLine] = []
        self._batch_size = 10  # Flush every N records
    
    def _get_season(self, dt: datetime) -> str:
        """Determine season string from datetime.
        
        NCAA basketball season spans Aug-Mar, so:
        - Aug 2025 - Jul 2026 = "2025-2026"
        """
        year = dt.year
        month = dt.month
        
        if month >= 8:  # Aug-Dec
            return f"{year}-{year + 1}"
        else:  # Jan-Jul
            return f"{year - 1}-{year}"
    
    def _get_parquet_path(self, sport: str, season: str) -> Path:
        """Get path to Parquet file for sport/season."""
        sport_dir = self.base_path / sport
        sport_dir.mkdir(parents=True, exist_ok=True)
        return sport_dir / f"{season}.parquet"
    
    def _load_into_cache(self, sport: str, season: str) -> pd.DataFrame:
        """Load a season's data into cache."""
        cache_key = (sport, season)
        
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        parquet_path = self._get_parquet_path(sport, season)
        
        if parquet_path.exists():
            df = pd.read_parquet(parquet_path)
            logger.info(f"Loaded {len(df)} opening lines from {parquet_path}")
        else:
            # Create empty DataFrame with proper schema
            df = pd.DataFrame(columns=[
                'game_id', 'sport', 'season', 'home_team', 'away_team',
                'commence_time', 'spread_home', 'spread_away',
                'spread_home_price', 'spread_away_price', 'total',
                'over_price', 'under_price',
                'moneyline_home', 'moneyline_away',
                'bookmaker', 'captured_at', 'source'
            ])
            logger.debug(f"No existing data at {parquet_path}")
        
        self._cache[cache_key] = df
        
        # Build index for O(1) lookups
        for _, row in df.iterrows():
            self._index[row['game_id']] = row.to_dict()
        
        return df
    
    def get(self, game_id: str, sport: str = None, season: str = None) -> Optional[Dict[str, Any]]:
        """Get opening line for a game.
        
        Args:
            game_id: Unique game identifier
            sport: Sport key (e.g., "basketball_ncaab")
            season: Season string (e.g., "2025-2026")
            
        Returns:
            Opening line data as dict, or None if not found
        """
        # Check index first (O(1) lookup)
        if game_id in self._index:
            return self._index[game_id]
        
        # If sport and season provided, load that partition
        if sport and season:
            self._load_into_cache(sport, season)
            return self._index.get(game_id)
        
        return None
    
    def has(self, game_id: str) -> bool:
        """Check if we have opening line for a game."""
        return game_id in self._index
    
    def save(self, line: OpeningLine, flush: bool = False) -> None:
        """Save an opening line.
        
        Lines are batched and flushed periodically for efficiency.
        
        Args:
            line: Opening line to save
            flush: Force immediate write to disk
        """
        # Skip if we already have this game
        if line.game_id in self._index:
            logger.debug(f"Opening line for {line.game_id} already exists, skipping")
            return
        
        # Add to index immediately for in-session lookups
        self._index[line.game_id] = line.to_dict()
        
        # Add to pending batch
        self._pending.append(line)
        
        # Flush if batch is full or forced
        if flush or len(self._pending) >= self._batch_size:
            self._flush()
    
    def _flush(self) -> None:
        """Write pending lines to Parquet files."""
        if not self._pending:
            return
        
        # Group by sport/season
        groups: Dict[tuple, List[OpeningLine]] = {}
        for line in self._pending:
            key = (line.sport, line.season)
            if key not in groups:
                groups[key] = []
            groups[key].append(line)
        
        # Write each group
        for (sport, season), lines in groups.items():
            self._append_to_parquet(sport, season, lines)
        
        self._pending.clear()
        logger.info(f"Flushed {sum(len(g) for g in groups.values())} opening lines to disk")
    
    def _append_to_parquet(self, sport: str, season: str, lines: List[OpeningLine]) -> None:
        """Append lines to a Parquet file."""
        parquet_path = self._get_parquet_path(sport, season)
        
        # Convert to DataFrame
        new_df = pd.DataFrame([line.to_dict() for line in lines])
        
        # Load existing or get from cache
        cache_key = (sport, season)
        if cache_key in self._cache:
            existing_df = self._cache[cache_key]
        elif parquet_path.exists():
            existing_df = pd.read_parquet(parquet_path)
        else:
            existing_df = pd.DataFrame()
        
        # Combine and deduplicate
        if not existing_df.empty:
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['game_id'], keep='first')
        else:
            combined_df = new_df
        
        # Write back
        combined_df.to_parquet(parquet_path, index=False, engine='pyarrow')
        
        # Update cache
        self._cache[cache_key] = combined_df
        
        logger.debug(f"Wrote {len(combined_df)} total lines to {parquet_path}")
    
    def load_season(self, sport: str, season: str) -> pd.DataFrame:
        """Load all opening lines for a season.
        
        Args:
            sport: Sport key
            season: Season string
            
        Returns:
            DataFrame with all opening lines
        """
        return self._load_into_cache(sport, season).copy()
    
    def load_current_season(self, sport: str) -> pd.DataFrame:
        """Load opening lines for the current season."""
        season = self._get_season(datetime.now())
        return self.load_season(sport, season)
    
    def preload(self, sport: str, seasons: List[str] = None) -> None:
        """Preload seasons into cache for fast access.
        
        Args:
            sport: Sport key
            seasons: List of seasons to load, or None for current season only
        """
        if seasons is None:
            seasons = [self._get_season(datetime.now())]
        
        for season in seasons:
            self._load_into_cache(sport, season)
        
        logger.info(f"Preloaded {len(self._index)} opening lines for {sport}")
    
    def close(self) -> None:
        """Flush pending data and close the store."""
        self._flush()
    
    def stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        total_files = 0
        total_size = 0
        total_records = 0
        
        for sport_dir in self.base_path.iterdir():
            if sport_dir.is_dir():
                for parquet_file in sport_dir.glob("*.parquet"):
                    total_files += 1
                    total_size += parquet_file.stat().st_size
                    df = pd.read_parquet(parquet_file)
                    total_records += len(df)
        
        return {
            "total_files": total_files,
            "total_size_mb": round(total_size / 1024 / 1024, 2),
            "total_records": total_records,
            "cached_records": len(self._index),
            "pending_writes": len(self._pending),
        }


class OddsSnapshotStore:
    """
    Time-series storage for odds snapshots during live games.
    
    This is optional but useful for:
    - Analyzing line movement patterns
    - Building ML models for predicting sharp moves
    - Backtesting betting strategies
    
    Uses Parquet with efficient compression since this can get large.
    """
    
    def __init__(self, base_path: str = "data/odds_snapshots"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        self._pending: List[Dict[str, Any]] = []
        self._batch_size = 100  # Larger batches for time-series data
    
    def record(
        self,
        game_id: str,
        sport: str,
        timestamp: datetime,
        spread_home: Optional[float],
        spread_away: Optional[float],
        total: Optional[float],
        home_score: Optional[int] = None,
        away_score: Optional[int] = None,
        period: Optional[str] = None,
        time_remaining: Optional[str] = None,
    ) -> None:
        """Record a single odds snapshot."""
        self._pending.append({
            'game_id': game_id,
            'sport': sport,
            'timestamp': timestamp,
            'spread_home': spread_home,
            'spread_away': spread_away,
            'total': total,
            'home_score': home_score,
            'away_score': away_score,
            'period': period,
            'time_remaining': time_remaining,
        })
        
        if len(self._pending) >= self._batch_size:
            self._flush()
    
    def _flush(self) -> None:
        """Write pending snapshots to Parquet."""
        if not self._pending:
            return
        
        # Group by sport and date
        groups: Dict[tuple, List[Dict]] = {}
        for snap in self._pending:
            dt = snap['timestamp']
            key = (snap['sport'], dt.strftime('%Y-%m-%d'))
            if key not in groups:
                groups[key] = []
            groups[key].append(snap)
        
        for (sport, date_str), snaps in groups.items():
            sport_dir = self.base_path / sport
            sport_dir.mkdir(parents=True, exist_ok=True)
            
            parquet_path = sport_dir / f"{date_str}.parquet"
            new_df = pd.DataFrame(snaps)
            
            if parquet_path.exists():
                existing_df = pd.read_parquet(parquet_path)
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                combined_df = new_df
            
            combined_df.to_parquet(parquet_path, index=False, engine='pyarrow')
        
        self._pending.clear()
    
    def load_game(self, game_id: str, sport: str, game_date: date) -> pd.DataFrame:
        """Load all snapshots for a specific game."""
        parquet_path = self.base_path / sport / f"{game_date.isoformat()}.parquet"
        
        if not parquet_path.exists():
            return pd.DataFrame()
        
        df = pd.read_parquet(parquet_path)
        return df[df['game_id'] == game_id]
    
    def close(self) -> None:
        """Flush pending data."""
        self._flush()


# Convenience function for creating stores
def create_data_stores(base_path: str = "data") -> tuple[OpeningLinesStore, OddsSnapshotStore]:
    """Create both data stores with a common base path.
    
    Args:
        base_path: Base directory for all data
        
    Returns:
        Tuple of (opening_lines_store, odds_snapshot_store)
    """
    return (
        OpeningLinesStore(f"{base_path}/opening_lines"),
        OddsSnapshotStore(f"{base_path}/odds_snapshots"),
    )
