"""SQLite storage for tracking games, odds, and alerts."""

import os
import sqlite3
from datetime import datetime, timedelta

from .models import Alert, Game, Odds

# Default DB path: data/odds_monitor.db from project root
_DEFAULT_DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
    "data",
    "odds_monitor.db"
)


class Storage:
    """SQLite database for persisting monitor state."""
    
    def __init__(self, db_path: str = None):
        """Initialize the storage.

        Args:
            db_path: Path to SQLite database file (defaults to data/odds_monitor.db)
        """
        self.db_path = db_path or _DEFAULT_DB_PATH
        self._init_db()
    
    def _get_conn(self) -> sqlite3.Connection:
        """Get a database connection."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _init_db(self):
        """Initialize database tables."""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        # Games table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS games (
                id TEXT PRIMARY KEY,
                home_team TEXT NOT NULL,
                away_team TEXT NOT NULL,
                commence_time TEXT NOT NULL,
                sport TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        
        # Opening odds table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS opening_odds (
                game_id TEXT PRIMARY KEY,
                spread_home REAL,
                spread_away REAL,
                spread_home_price INTEGER,
                spread_away_price INTEGER,
                moneyline_home INTEGER,
                moneyline_away INTEGER,
                total REAL,
                over_price INTEGER,
                under_price INTEGER,
                fetched_at TEXT NOT NULL,
                FOREIGN KEY (game_id) REFERENCES games(id)
            )
        """)
        
        # Odds history table (for tracking line movement)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS odds_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id TEXT NOT NULL,
                spread_home REAL,
                spread_away REAL,
                spread_home_price INTEGER,
                spread_away_price INTEGER,
                moneyline_home INTEGER,
                moneyline_away INTEGER,
                total REAL,
                over_price INTEGER,
                under_price INTEGER,
                recorded_at TEXT NOT NULL,
                FOREIGN KEY (game_id) REFERENCES games(id)
            )
        """)
        
        # Alerts table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                message TEXT NOT NULL,
                sent_at TEXT NOT NULL,
                FOREIGN KEY (game_id) REFERENCES games(id)
            )
        """)

        # Game results table (for backtesting)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS game_results (
                game_id TEXT PRIMARY KEY,
                final_score_home INTEGER,
                final_score_away INTEGER,
                completed_at TEXT NOT NULL,
                FOREIGN KEY (game_id) REFERENCES games(id)
            )
        """)

        # Line snapshots during live games (for tracking movement)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS line_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                bookmaker TEXT NOT NULL,
                spread_team TEXT,
                spread_value REAL,
                spread_price INTEGER,
                home_score INTEGER,
                away_score INTEGER,
                mins_remaining REAL,
                is_opening INTEGER DEFAULT 0,
                FOREIGN KEY (game_id) REFERENCES games(id)
            )
        """)

        # Bet outcomes table (for strategy analysis)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bet_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id TEXT NOT NULL,
                alert_id INTEGER,
                bet_type TEXT NOT NULL,
                bet_team TEXT NOT NULL,
                bet_spread REAL NOT NULL,
                mins_remaining REAL,
                opening_spread REAL,
                pct_change REAL,
                final_margin INTEGER,
                covered INTEGER,
                profit REAL,
                FOREIGN KEY (game_id) REFERENCES games(id),
                FOREIGN KEY (alert_id) REFERENCES alerts(id)
            )
        """)

        # Historical odds cache (avoid re-fetching paid API data)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS historical_odds_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id TEXT NOT NULL,
                sport TEXT NOT NULL,
                snapshot_type TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                bookmaker TEXT NOT NULL,
                spread_home REAL,
                spread_away REAL,
                spread_home_price INTEGER,
                spread_away_price INTEGER,
                moneyline_home INTEGER,
                moneyline_away INTEGER,
                total REAL,
                over_price INTEGER,
                under_price INTEGER,
                fetched_at TEXT NOT NULL,
                UNIQUE(game_id, snapshot_type, bookmaker)
            )
        """)

        # Simulated bets from historical data
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS simulated_bets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id TEXT NOT NULL,
                sport TEXT NOT NULL,
                strategy TEXT NOT NULL,
                bet_team TEXT NOT NULL,
                bet_spread REAL NOT NULL,
                opening_spread REAL NOT NULL,
                pct_change REAL NOT NULL,
                snapshot_type TEXT NOT NULL,
                final_margin INTEGER,
                covered INTEGER,
                profit REAL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (game_id) REFERENCES games(id)
            )
        """)

        conn.commit()
        conn.close()
    
    def save_game(self, game: Game) -> None:
        """Save or update a game.
        
        Args:
            game: Game to save
        """
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT OR REPLACE INTO games
            (id, home_team, away_team, commence_time, sport, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            game.id,
            game.home_team,
            game.away_team,
            game.commence_time.isoformat(),
            game.sport,
            datetime.utcnow().isoformat()
        ))
        
        conn.commit()
        conn.close()
    
    def save_opening_odds(self, game_id: str, odds: Odds) -> None:
        """Save opening odds for a game.
        
        Args:
            game_id: Game ID
            odds: Opening odds
        """
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT OR REPLACE INTO opening_odds
            (game_id, spread_home, spread_away, spread_home_price,
             spread_away_price, moneyline_home, moneyline_away,
             total, over_price, under_price, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            game_id,
            odds.spread_home,
            odds.spread_away,
            odds.spread_home_price,
            odds.spread_away_price,
            odds.moneyline_home,
            odds.moneyline_away,
            odds.total,
            odds.over_price,
            odds.under_price,
            datetime.utcnow().isoformat()
        ))
        
        conn.commit()
        conn.close()

    def get_opening_odds(self, game_id: str) -> Odds | None:
        """Get opening odds for a game.

        Args:
            game_id: Game ID

        Returns:
            Opening odds or None if not found
        """
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT * FROM opening_odds WHERE game_id = ?",
            (game_id,),
        )
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            return None
            
        return Odds(
            spread_home=row["spread_home"],
            spread_away=row["spread_away"],
            spread_home_price=row["spread_home_price"],
            spread_away_price=row["spread_away_price"],
            moneyline_home=row["moneyline_home"],
            moneyline_away=row["moneyline_away"],
            total=row["total"],
            over_price=row["over_price"],
            under_price=row["under_price"],
            timestamp=datetime.fromisoformat(row["fetched_at"])
        )
    
    def record_odds(self, game_id: str, odds: Odds) -> None:
        """Record current odds to history.
        
        Args:
            game_id: Game ID
            odds: Current odds
        """
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO odds_history
            (game_id, spread_home, spread_away, spread_home_price,
             spread_away_price, moneyline_home, moneyline_away,
             total, over_price, under_price, recorded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            game_id,
            odds.spread_home,
            odds.spread_away,
            odds.spread_home_price,
            odds.spread_away_price,
            odds.moneyline_home,
            odds.moneyline_away,
            odds.total,
            odds.over_price,
            odds.under_price,
            datetime.utcnow().isoformat()
        ))
        
        conn.commit()
        conn.close()
    
    def save_alert(self, alert: Alert) -> None:
        """Save an alert to the database.
        
        Args:
            alert: Alert to save
        """
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO alerts (game_id, alert_type, message, sent_at)
            VALUES (?, ?, ?, ?)
        """, (
            alert.game.id,
            alert.alert_type,
            alert.message,
            alert.timestamp.isoformat()
        ))
        
        conn.commit()
        conn.close()
    
    def has_alert_been_sent(self, game_id: str, alert_type: str) -> bool:
        """Check if an alert has already been sent for a game.
        
        Args:
            game_id: Game ID
            alert_type: Type of alert
            
        Returns:
            True if alert already sent
        """
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) as count FROM alerts
            WHERE game_id = ? AND alert_type = ?
        """, (game_id, alert_type))
        
        row = cursor.fetchone()
        conn.close()
        
        return row["count"] > 0
    
    def get_odds_history(self, game_id: str) -> list[Odds]:
        """Get odds history for a game.

        Args:
            game_id: Game ID

        Returns:
            List of historical odds snapshots
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM odds_history
            WHERE game_id = ?
            ORDER BY recorded_at ASC
        """, (game_id,))
        
        rows = cursor.fetchall()
        conn.close()
        
        return [
            Odds(
                spread_home=row["spread_home"],
                spread_away=row["spread_away"],
                spread_home_price=row["spread_home_price"],
                spread_away_price=row["spread_away_price"],
                moneyline_home=row["moneyline_home"],
                moneyline_away=row["moneyline_away"],
                total=row["total"],
                over_price=row["over_price"],
                under_price=row["under_price"],
                timestamp=datetime.fromisoformat(row["recorded_at"])
            )
            for row in rows
        ]
    
    def cleanup_old_games(self, days: int = 7) -> int:
        """Remove games older than specified days.
        
        Args:
            days: Number of days to keep
            
        Returns:
            Number of games removed
        """
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cutoff = datetime.utcnow()
        from datetime import timedelta
        cutoff = cutoff - timedelta(days=days)
        
        # Get count of games to delete
        cursor.execute("""
            SELECT COUNT(*) as count FROM games
            WHERE commence_time < ?
        """, (cutoff.isoformat(),))
        count = cursor.fetchone()["count"]
        
        # Delete related records first
        cursor.execute("""
            DELETE FROM opening_odds WHERE game_id IN (
                SELECT id FROM games WHERE commence_time < ?
            )
        """, (cutoff.isoformat(),))
        
        cursor.execute("""
            DELETE FROM odds_history WHERE game_id IN (
                SELECT id FROM games WHERE commence_time < ?
            )
        """, (cutoff.isoformat(),))
        
        cursor.execute("""
            DELETE FROM alerts WHERE game_id IN (
                SELECT id FROM games WHERE commence_time < ?
            )
        """, (cutoff.isoformat(),))
        
        cursor.execute(
            "DELETE FROM games WHERE commence_time < ?",
            (cutoff.isoformat(),),
        )

        conn.commit()
        conn.close()

        return count

    # --- Backtesting Methods ---

    def save_line_snapshot(
        self,
        game_id: str,
        bookmaker: str,
        spread_team: str,
        spread_value: float,
        spread_price: int = -110,
        home_score: int = 0,
        away_score: int = 0,
        mins_remaining: float = None,
        is_opening: bool = False,
    ) -> None:
        """Record a line snapshot during a live game."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO line_snapshots
            (game_id, timestamp, bookmaker, spread_team, spread_value,
             spread_price, home_score, away_score, mins_remaining, is_opening)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            game_id,
            datetime.utcnow().isoformat(),
            bookmaker,
            spread_team,
            spread_value,
            spread_price,
            home_score,
            away_score,
            mins_remaining,
            1 if is_opening else 0,
        ))

        conn.commit()
        conn.close()

    def get_opening_snapshot(self, game_id: str) -> dict | None:
        """Get the opening line snapshot for a game."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM line_snapshots
            WHERE game_id = ? AND is_opening = 1
            ORDER BY timestamp ASC LIMIT 1
        """, (game_id,))

        row = cursor.fetchone()
        conn.close()

        if row:
            return dict(row)
        return None

    def get_line_snapshots(self, game_id: str) -> list[dict]:
        """Get all line snapshots for a game."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM line_snapshots
            WHERE game_id = ?
            ORDER BY timestamp ASC
        """, (game_id,))

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def save_game_result(
        self,
        game_id: str,
        final_score_home: int,
        final_score_away: int,
    ) -> None:
        """Record final game result."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT OR REPLACE INTO game_results
            (game_id, final_score_home, final_score_away, completed_at)
            VALUES (?, ?, ?, ?)
        """, (
            game_id,
            final_score_home,
            final_score_away,
            datetime.utcnow().isoformat(),
        ))

        conn.commit()
        conn.close()

    def get_game_result(self, game_id: str) -> dict | None:
        """Get final result for a game."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT * FROM game_results WHERE game_id = ?",
            (game_id,),
        )

        row = cursor.fetchone()
        conn.close()

        if row:
            return dict(row)
        return None

    def save_bet_outcome(
        self,
        game_id: str,
        bet_type: str,
        bet_team: str,
        bet_spread: float,
        opening_spread: float,
        pct_change: float,
        mins_remaining: float = None,
        alert_id: int = None,
    ) -> int:
        """Record a hypothetical bet for later analysis."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO bet_outcomes
            (game_id, alert_id, bet_type, bet_team, bet_spread,
             mins_remaining, opening_spread, pct_change,
             final_margin, covered, profit)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)
        """, (
            game_id,
            alert_id,
            bet_type,
            bet_team,
            bet_spread,
            mins_remaining,
            opening_spread,
            pct_change,
        ))

        bet_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return bet_id

    def update_bet_outcome(
        self,
        bet_id: int,
        final_margin: int,
        covered: bool,
        profit: float,
    ) -> None:
        """Update bet outcome after game completes."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE bet_outcomes
            SET final_margin = ?, covered = ?, profit = ?
            WHERE id = ?
        """, (
            final_margin,
            1 if covered else 0,
            profit,
            bet_id,
        ))

        conn.commit()
        conn.close()

    def get_pending_bets(self) -> list[dict]:
        """Get bets that haven't been resolved yet."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT b.*, g.home_team, g.away_team
            FROM bet_outcomes b
            JOIN games g ON b.game_id = g.id
            WHERE b.covered IS NULL
        """)

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def get_all_bets(self) -> list[dict]:
        """Get all bet outcomes for analysis."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT b.*, g.home_team, g.away_team, g.commence_time
            FROM bet_outcomes b
            JOIN games g ON b.game_id = g.id
            ORDER BY g.commence_time DESC
        """)

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def get_games_needing_results(self) -> list[dict]:
        """Get games that are likely completed but don't have results."""
        conn = self._get_conn()
        cursor = conn.cursor()

        # Games started more than 3 hours ago without results
        cutoff = (datetime.utcnow() - timedelta(hours=3)).isoformat()

        cursor.execute("""
            SELECT g.*
            FROM games g
            LEFT JOIN game_results r ON g.id = r.game_id
            WHERE r.game_id IS NULL
            AND g.commence_time < ?
            ORDER BY g.commence_time DESC
        """, (cutoff,))

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def save_alert_with_id(
        self,
        game_id: str,
        alert_type: str,
        message: str,
    ) -> int:
        """Save an alert and return its ID."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO alerts (game_id, alert_type, message, sent_at)
            VALUES (?, ?, ?, ?)
        """, (
            game_id,
            alert_type,
            message,
            datetime.utcnow().isoformat(),
        ))

        alert_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return alert_id

    # --- Historical Odds Cache Methods ---

    def cache_historical_odds(
        self,
        game_id: str,
        sport: str,
        snapshot_type: str,
        timestamp: str,
        bookmaker: str,
        spread_home: float = None,
        spread_away: float = None,
        spread_home_price: int = None,
        spread_away_price: int = None,
        moneyline_home: int = None,
        moneyline_away: int = None,
        total: float = None,
        over_price: int = None,
        under_price: int = None,
    ) -> None:
        """Cache historical odds to avoid re-fetching."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT OR REPLACE INTO historical_odds_cache
            (game_id, sport, snapshot_type, timestamp, bookmaker,
             spread_home, spread_away, spread_home_price, spread_away_price,
             moneyline_home, moneyline_away, total, over_price, under_price,
             fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            game_id,
            sport,
            snapshot_type,
            timestamp,
            bookmaker,
            spread_home,
            spread_away,
            spread_home_price,
            spread_away_price,
            moneyline_home,
            moneyline_away,
            total,
            over_price,
            under_price,
            datetime.utcnow().isoformat(),
        ))

        conn.commit()
        conn.close()

    def get_cached_odds(
        self,
        game_id: str,
        snapshot_type: str,
        bookmaker: str = "fanduel",
    ) -> dict | None:
        """Get cached historical odds."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM historical_odds_cache
            WHERE game_id = ? AND snapshot_type = ? AND bookmaker = ?
        """, (game_id, snapshot_type, bookmaker))

        row = cursor.fetchone()
        conn.close()

        if row:
            return dict(row)
        return None

    def get_all_cached_odds_for_game(self, game_id: str) -> list[dict]:
        """Get all cached odds snapshots for a game."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM historical_odds_cache
            WHERE game_id = ?
            ORDER BY timestamp ASC
        """, (game_id,))

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def save_simulated_bet(
        self,
        game_id: str,
        sport: str,
        strategy: str,
        bet_team: str,
        bet_spread: float,
        opening_spread: float,
        pct_change: float,
        snapshot_type: str,
    ) -> int:
        """Save a simulated bet from historical analysis."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO simulated_bets
            (game_id, sport, strategy, bet_team, bet_spread,
             opening_spread, pct_change, snapshot_type,
             final_margin, covered, profit, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?)
        """, (
            game_id,
            sport,
            strategy,
            bet_team,
            bet_spread,
            opening_spread,
            pct_change,
            snapshot_type,
            datetime.utcnow().isoformat(),
        ))

        bet_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return bet_id

    def update_simulated_bet(
        self,
        bet_id: int,
        final_margin: int,
        covered: bool,
        profit: float,
    ) -> None:
        """Update simulated bet with outcome."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE simulated_bets
            SET final_margin = ?, covered = ?, profit = ?
            WHERE id = ?
        """, (
            final_margin,
            1 if covered else 0,
            profit,
            bet_id,
        ))

        conn.commit()
        conn.close()

    def get_simulated_bets(
        self,
        strategy: str = None,
        sport: str = None,
    ) -> list[dict]:
        """Get simulated bets with optional filters."""
        conn = self._get_conn()
        cursor = conn.cursor()

        query = """
            SELECT s.*, g.home_team, g.away_team, g.commence_time
            FROM simulated_bets s
            JOIN games g ON s.game_id = g.id
            WHERE 1=1
        """
        params = []

        if strategy:
            query += " AND s.strategy = ?"
            params.append(strategy)

        if sport:
            query += " AND s.sport = ?"
            params.append(sport)

        query += " ORDER BY g.commence_time DESC"

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def get_simulated_bet_stats(
        self,
        strategy: str = None,
        sport: str = None,
        min_pct_change: float = None,
    ) -> dict:
        """Get statistics for simulated bets."""
        bets = self.get_simulated_bets(strategy=strategy, sport=sport)

        # Filter to resolved only
        resolved = [b for b in bets if b.get("covered") is not None]

        if min_pct_change is not None:
            resolved = [
                b for b in resolved
                if b.get("pct_change", 0) >= min_pct_change
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
        total_wagered = len(resolved) * 110

        return {
            "total_bets": len(resolved),
            "wins": wins,
            "losses": losses,
            "win_rate": wins / len(resolved) if resolved else 0,
            "total_profit": total_profit,
            "roi": total_profit / total_wagered if total_wagered else 0,
        }

    def has_cached_game(self, game_id: str) -> bool:
        """Check if we have any cached odds for a game."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT COUNT(*) as count FROM historical_odds_cache "
            "WHERE game_id = ?",
            (game_id,),
        )

        row = cursor.fetchone()
        conn.close()

        return row["count"] > 0
