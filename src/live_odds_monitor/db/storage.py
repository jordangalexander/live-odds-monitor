"""SQLite storage for tracking games, odds, and alerts.

Schema v2 consolidates redundant tables:
- odds_snapshots: Unified table for all odds (opening, live, historical)
- bets: Unified table for all bets (live and backtest)

Old tables are migrated automatically on first run.
"""

import os
import sqlite3
from datetime import datetime, timedelta

from .models import Alert, Game, Odds

# Schema version for migrations
SCHEMA_VERSION = 2

# Default DB path: Use environment variable, or ~/.local/share/live-odds-monitor/
# This keeps data OUTSIDE the repo to prevent accidental deletion
_DEFAULT_DB_DIR = os.environ.get(
    "ODDS_MONITOR_DATA_DIR",
    os.path.expanduser("~/.local/share/live-odds-monitor"),
)
_DEFAULT_DB_PATH = os.path.join(_DEFAULT_DB_DIR, "odds_monitor.db")


class Storage:
    """SQLite database for persisting monitor state."""

    def __init__(self, db_path: str = None):
        """Initialize the storage.

        Args:
            db_path: Path to SQLite database file (defaults to ~/.local/share/live-odds-monitor/)
        """
        self.db_path = db_path or _DEFAULT_DB_PATH
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        """Get a database connection."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _get_schema_version(self, conn: sqlite3.Connection) -> int:
        """Get current schema version from database."""
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY
            )
        """)
        cursor.execute("SELECT version FROM schema_version LIMIT 1")
        row = cursor.fetchone()
        return row["version"] if row else 1

    def _set_schema_version(self, conn: sqlite3.Connection, version: int) -> None:
        """Set schema version in database."""
        cursor = conn.cursor()
        cursor.execute("DELETE FROM schema_version")
        cursor.execute("INSERT INTO schema_version (version) VALUES (?)", (version,))

    def _init_db(self):
        """Initialize database tables."""
        conn = self._get_conn()
        cursor = conn.cursor()

        current_version = self._get_schema_version(conn)

        # Games table (unchanged)
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

        # Game results table (unchanged)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS game_results (
                game_id TEXT PRIMARY KEY,
                final_score_home INTEGER,
                final_score_away INTEGER,
                completed_at TEXT NOT NULL,
                FOREIGN KEY (game_id) REFERENCES games(id)
            )
        """)

        # Alerts table (unchanged)
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

        # Historical odds cache (unchanged - serves distinct caching purpose)
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

        # =====================================================================
        # CONSOLIDATED TABLES (Schema v2)
        # =====================================================================

        # odds_snapshots: Unified table for all odds
        # Replaces: opening_odds, odds_history, line_snapshots
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS odds_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                source TEXT NOT NULL,
                snapshot_type TEXT NOT NULL,
                bookmaker TEXT DEFAULT 'fanduel',
                spread_home REAL,
                spread_away REAL,
                spread_home_price INTEGER,
                spread_away_price INTEGER,
                moneyline_home INTEGER,
                moneyline_away INTEGER,
                total REAL,
                over_price INTEGER,
                under_price INTEGER,
                home_score INTEGER,
                away_score INTEGER,
                mins_remaining REAL,
                FOREIGN KEY (game_id) REFERENCES games(id)
            )
        """)

        # bets: Unified table for all bets
        # Replaces: bet_outcomes, simulated_bets
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id TEXT NOT NULL,
                source TEXT NOT NULL,
                strategy TEXT NOT NULL,
                bet_team TEXT NOT NULL,
                bet_spread REAL NOT NULL,
                opening_spread REAL,
                pct_change REAL,
                snapshot_type TEXT,
                mins_remaining REAL,
                alert_id INTEGER,
                final_margin INTEGER,
                covered INTEGER,
                profit REAL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (game_id) REFERENCES games(id),
                FOREIGN KEY (alert_id) REFERENCES alerts(id)
            )
        """)

        # Create indexes for common queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_odds_snapshots_game 
            ON odds_snapshots(game_id)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_odds_snapshots_type 
            ON odds_snapshots(snapshot_type)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_bets_game 
            ON bets(game_id)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_bets_source 
            ON bets(source)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_bets_strategy 
            ON bets(strategy)
        """)

        # =====================================================================
        # LEGACY TABLES (kept for backwards compatibility during migration)
        # =====================================================================

        # Opening odds table (legacy)
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

        # Odds history table (legacy)
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

        # Line snapshots table (legacy)
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

        # Bet outcomes table (legacy)
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

        # Simulated bets table (legacy)
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

        # Simple opening line cache for live monitoring (saves API credits)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS opening_line_cache (
                game_id TEXT PRIMARY KEY,
                sport TEXT NOT NULL,
                spread_value REAL NOT NULL,
                spread_team TEXT NOT NULL,
                home_team TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            )
        """)

        conn.commit()

        # Run migration if needed
        if current_version < SCHEMA_VERSION:
            self._migrate_to_v2(conn)
            self._set_schema_version(conn, SCHEMA_VERSION)
            conn.commit()

        conn.close()

    def _migrate_to_v2(self, conn: sqlite3.Connection) -> None:
        """Migrate data from legacy tables to consolidated tables."""
        cursor = conn.cursor()
        print("Migrating database to schema v2...")

        # Migrate opening_odds to odds_snapshots
        cursor.execute("""
            INSERT OR IGNORE INTO odds_snapshots 
            (game_id, timestamp, source, snapshot_type, bookmaker,
             spread_home, spread_away, spread_home_price, spread_away_price,
             moneyline_home, moneyline_away, total, over_price, under_price)
            SELECT 
                game_id, fetched_at, 'live', 'opening', 'fanduel',
                spread_home, spread_away, spread_home_price, spread_away_price,
                moneyline_home, moneyline_away, total, over_price, under_price
            FROM opening_odds
        """)
        opening_count = cursor.rowcount

        # Migrate odds_history to odds_snapshots
        cursor.execute("""
            INSERT OR IGNORE INTO odds_snapshots 
            (game_id, timestamp, source, snapshot_type, bookmaker,
             spread_home, spread_away, spread_home_price, spread_away_price,
             moneyline_home, moneyline_away, total, over_price, under_price)
            SELECT 
                game_id, recorded_at, 'live', 'live', 'fanduel',
                spread_home, spread_away, spread_home_price, spread_away_price,
                moneyline_home, moneyline_away, total, over_price, under_price
            FROM odds_history
        """)
        history_count = cursor.rowcount

        # Migrate line_snapshots to odds_snapshots
        cursor.execute("""
            INSERT OR IGNORE INTO odds_snapshots 
            (game_id, timestamp, source, snapshot_type, bookmaker,
             spread_home, home_score, away_score, mins_remaining)
            SELECT 
                game_id, timestamp, 'live', 
                CASE WHEN is_opening = 1 THEN 'opening' ELSE 'live' END,
                bookmaker, spread_value, home_score, away_score, mins_remaining
            FROM line_snapshots
        """)
        snapshot_count = cursor.rowcount

        # Migrate bet_outcomes to bets
        cursor.execute("""
            INSERT OR IGNORE INTO bets 
            (game_id, source, strategy, bet_team, bet_spread,
             opening_spread, pct_change, mins_remaining, alert_id,
             final_margin, covered, profit, created_at)
            SELECT 
                game_id, 'live', COALESCE(bet_type, 'spread'), bet_team, bet_spread,
                opening_spread, pct_change, mins_remaining, alert_id,
                final_margin, covered, profit, datetime('now')
            FROM bet_outcomes
        """)
        live_bet_count = cursor.rowcount

        # Migrate simulated_bets to bets
        cursor.execute("""
            INSERT OR IGNORE INTO bets 
            (game_id, source, strategy, bet_team, bet_spread,
             opening_spread, pct_change, snapshot_type,
             final_margin, covered, profit, created_at)
            SELECT 
                game_id, 'backtest', strategy, bet_team, bet_spread,
                opening_spread, pct_change, snapshot_type,
                final_margin, covered, profit, created_at
            FROM simulated_bets
        """)
        backtest_bet_count = cursor.rowcount

        print(f"  Migrated {opening_count} opening odds")
        print(f"  Migrated {history_count} odds history records")
        print(f"  Migrated {snapshot_count} line snapshots")
        print(f"  Migrated {live_bet_count} live bets")
        print(f"  Migrated {backtest_bet_count} backtest bets")
        print("Migration complete!")

    # =========================================================================
    # UNIFIED API (uses new tables)
    # =========================================================================

    def save_odds_snapshot(
        self,
        game_id: str,
        source: str,
        snapshot_type: str,
        spread_home: float = None,
        spread_away: float = None,
        spread_home_price: int = None,
        spread_away_price: int = None,
        moneyline_home: int = None,
        moneyline_away: int = None,
        total: float = None,
        over_price: int = None,
        under_price: int = None,
        home_score: int = None,
        away_score: int = None,
        mins_remaining: float = None,
        bookmaker: str = "fanduel",
    ) -> int:
        """Save an odds snapshot to the unified table.

        Args:
            game_id: Game ID
            source: 'live' or 'backtest'
            snapshot_type: 'opening', 'live', 'pregame', 'midgame'
            bookmaker: Bookmaker name
            ... odds fields

        Returns:
            Snapshot ID
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO odds_snapshots
            (game_id, timestamp, source, snapshot_type, bookmaker,
             spread_home, spread_away, spread_home_price, spread_away_price,
             moneyline_home, moneyline_away, total, over_price, under_price,
             home_score, away_score, mins_remaining)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                game_id,
                datetime.utcnow().isoformat(),
                source,
                snapshot_type,
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
                home_score,
                away_score,
                mins_remaining,
            ),
        )

        snapshot_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return snapshot_id

    def get_odds_snapshots(
        self,
        game_id: str,
        snapshot_type: str = None,
        source: str = None,
    ) -> list[dict]:
        """Get odds snapshots for a game.

        Args:
            game_id: Game ID
            snapshot_type: Filter by type ('opening', 'live', etc.)
            source: Filter by source ('live', 'backtest')

        Returns:
            List of snapshot dicts
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        query = "SELECT * FROM odds_snapshots WHERE game_id = ?"
        params = [game_id]

        if snapshot_type:
            query += " AND snapshot_type = ?"
            params.append(snapshot_type)

        if source:
            query += " AND source = ?"
            params.append(source)

        query += " ORDER BY timestamp ASC"

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def get_opening_odds_snapshot(self, game_id: str) -> dict | None:
        """Get opening odds snapshot for a game."""
        snapshots = self.get_odds_snapshots(game_id, snapshot_type="opening")
        return snapshots[0] if snapshots else None

    def save_bet(
        self,
        game_id: str,
        source: str,
        strategy: str,
        bet_team: str,
        bet_spread: float,
        opening_spread: float = None,
        pct_change: float = None,
        snapshot_type: str = None,
        mins_remaining: float = None,
        alert_id: int = None,
    ) -> int:
        """Save a bet to the unified table.

        Args:
            game_id: Game ID
            source: 'live' or 'backtest'
            strategy: Strategy name (e.g., 'spread_change_50pct')
            bet_team: Team being bet on
            bet_spread: Spread at time of bet
            opening_spread: Opening spread value
            pct_change: Percentage change from opening
            snapshot_type: For backtest bets, the snapshot type
            mins_remaining: Minutes remaining in game
            alert_id: Associated alert ID (for live bets)

        Returns:
            Bet ID
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO bets
            (game_id, source, strategy, bet_team, bet_spread,
             opening_spread, pct_change, snapshot_type, mins_remaining,
             alert_id, final_margin, covered, profit, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?)
        """,
            (
                game_id,
                source,
                strategy,
                bet_team,
                bet_spread,
                opening_spread,
                pct_change,
                snapshot_type,
                mins_remaining,
                alert_id,
                datetime.utcnow().isoformat(),
            ),
        )

        bet_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return bet_id

    def update_bet(
        self,
        bet_id: int,
        final_margin: int,
        covered: bool,
        profit: float,
    ) -> None:
        """Update bet outcome after game completes."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE bets
            SET final_margin = ?, covered = ?, profit = ?
            WHERE id = ?
        """,
            (
                final_margin,
                1 if covered else 0,
                profit,
                bet_id,
            ),
        )

        conn.commit()
        conn.close()

    def get_bets(
        self,
        source: str = None,
        strategy: str = None,
        resolved_only: bool = False,
    ) -> list[dict]:
        """Get bets with optional filters.

        Args:
            source: Filter by source ('live', 'backtest')
            strategy: Filter by strategy name
            resolved_only: Only return bets with outcomes

        Returns:
            List of bet dicts with game info
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        query = """
            SELECT b.*, g.home_team, g.away_team, g.sport, g.commence_time
            FROM bets b
            JOIN games g ON b.game_id = g.id
            WHERE 1=1
        """
        params = []

        if source:
            query += " AND b.source = ?"
            params.append(source)

        if strategy:
            query += " AND b.strategy = ?"
            params.append(strategy)

        if resolved_only:
            query += " AND b.covered IS NOT NULL"

        query += " ORDER BY g.commence_time DESC"

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def get_pending_bets_unified(self) -> list[dict]:
        """Get bets that haven't been resolved yet."""
        return self.get_bets(resolved_only=False)

    def get_bet_stats(
        self,
        source: str = None,
        strategy: str = None,
        min_pct_change: float = None,
    ) -> dict:
        """Get statistics for bets matching filters."""
        bets = self.get_bets(source=source, strategy=strategy, resolved_only=True)

        if min_pct_change is not None:
            bets = [b for b in bets if (b.get("pct_change") or 0) >= min_pct_change]

        if not bets:
            return {
                "total_bets": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0,
                "total_profit": 0,
                "roi": 0,
            }

        wins = sum(1 for b in bets if b["covered"])
        losses = len(bets) - wins
        total_profit = sum(b.get("profit", 0) for b in bets)
        total_wagered = len(bets) * 110

        return {
            "total_bets": len(bets),
            "wins": wins,
            "losses": losses,
            "win_rate": wins / len(bets) if bets else 0,
            "total_profit": total_profit,
            "roi": total_profit / total_wagered if total_wagered else 0,
        }

    # =========================================================================
    # LEGACY API (for backwards compatibility - writes to both old and new)
    # =========================================================================

    def save_game(self, game: Game) -> None:
        """Save or update a game.

        Args:
            game: Game to save
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT OR REPLACE INTO games
            (id, home_team, away_team, commence_time, sport, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            (
                game.id,
                game.home_team,
                game.away_team,
                game.commence_time.isoformat(),
                game.sport,
                datetime.utcnow().isoformat(),
            ),
        )

        conn.commit()
        conn.close()

    def save_opening_odds(self, game_id: str, odds: Odds) -> None:
        """Save opening odds for a game (writes to both legacy and unified tables).

        Args:
            game_id: Game ID
            odds: Opening odds
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        now = datetime.utcnow().isoformat()

        # Write to legacy table
        cursor.execute(
            """
            INSERT OR REPLACE INTO opening_odds
            (game_id, spread_home, spread_away, spread_home_price,
             spread_away_price, moneyline_home, moneyline_away,
             total, over_price, under_price, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
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
                now,
            ),
        )

        # Also write to unified table
        cursor.execute(
            """
            INSERT INTO odds_snapshots
            (game_id, timestamp, source, snapshot_type, bookmaker,
             spread_home, spread_away, spread_home_price, spread_away_price,
             moneyline_home, moneyline_away, total, over_price, under_price)
            VALUES (?, ?, 'live', 'opening', 'fanduel', ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                game_id,
                now,
                odds.spread_home,
                odds.spread_away,
                odds.spread_home_price,
                odds.spread_away_price,
                odds.moneyline_home,
                odds.moneyline_away,
                odds.total,
                odds.over_price,
                odds.under_price,
            ),
        )

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
            timestamp=datetime.fromisoformat(row["fetched_at"]),
        )

    def record_odds(self, game_id: str, odds: Odds) -> None:
        """Record current odds to history (writes to both legacy and unified tables).

        Args:
            game_id: Game ID
            odds: Current odds
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        now = datetime.utcnow().isoformat()

        # Write to legacy table
        cursor.execute(
            """
            INSERT INTO odds_history
            (game_id, spread_home, spread_away, spread_home_price,
             spread_away_price, moneyline_home, moneyline_away,
             total, over_price, under_price, recorded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
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
                now,
            ),
        )

        # Also write to unified table
        cursor.execute(
            """
            INSERT INTO odds_snapshots
            (game_id, timestamp, source, snapshot_type, bookmaker,
             spread_home, spread_away, spread_home_price, spread_away_price,
             moneyline_home, moneyline_away, total, over_price, under_price)
            VALUES (?, ?, 'live', 'live', 'fanduel', ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                game_id,
                now,
                odds.spread_home,
                odds.spread_away,
                odds.spread_home_price,
                odds.spread_away_price,
                odds.moneyline_home,
                odds.moneyline_away,
                odds.total,
                odds.over_price,
                odds.under_price,
            ),
        )

        conn.commit()
        conn.close()

    def save_alert(self, alert: Alert) -> None:
        """Save an alert to the database.

        Args:
            alert: Alert to save
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO alerts (game_id, alert_type, message, sent_at)
            VALUES (?, ?, ?, ?)
        """,
            (alert.game.id, alert.alert_type, alert.message, alert.timestamp.isoformat()),
        )

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

        cursor.execute(
            """
            SELECT COUNT(*) as count FROM alerts
            WHERE game_id = ? AND alert_type = ?
        """,
            (game_id, alert_type),
        )

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

        cursor.execute(
            """
            SELECT * FROM odds_history
            WHERE game_id = ?
            ORDER BY recorded_at ASC
        """,
            (game_id,),
        )

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
                timestamp=datetime.fromisoformat(row["recorded_at"]),
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
        cursor.execute(
            """
            SELECT COUNT(*) as count FROM games
            WHERE commence_time < ?
        """,
            (cutoff.isoformat(),),
        )
        count = cursor.fetchone()["count"]

        # Delete related records first
        cursor.execute(
            """
            DELETE FROM opening_odds WHERE game_id IN (
                SELECT id FROM games WHERE commence_time < ?
            )
        """,
            (cutoff.isoformat(),),
        )

        cursor.execute(
            """
            DELETE FROM odds_history WHERE game_id IN (
                SELECT id FROM games WHERE commence_time < ?
            )
        """,
            (cutoff.isoformat(),),
        )

        cursor.execute(
            """
            DELETE FROM alerts WHERE game_id IN (
                SELECT id FROM games WHERE commence_time < ?
            )
        """,
            (cutoff.isoformat(),),
        )

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
        """Record a line snapshot during a live game (writes to both tables)."""
        conn = self._get_conn()
        cursor = conn.cursor()

        now = datetime.utcnow().isoformat()
        snapshot_type = "opening" if is_opening else "live"

        # Write to legacy table
        cursor.execute(
            """
            INSERT INTO line_snapshots
            (game_id, timestamp, bookmaker, spread_team, spread_value,
             spread_price, home_score, away_score, mins_remaining, is_opening)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                game_id,
                now,
                bookmaker,
                spread_team,
                spread_value,
                spread_price,
                home_score,
                away_score,
                mins_remaining,
                1 if is_opening else 0,
            ),
        )

        # Also write to unified table
        cursor.execute(
            """
            INSERT INTO odds_snapshots
            (game_id, timestamp, source, snapshot_type, bookmaker,
             spread_home, home_score, away_score, mins_remaining)
            VALUES (?, ?, 'live', ?, ?, ?, ?, ?, ?)
        """,
            (
                game_id,
                now,
                snapshot_type,
                bookmaker,
                spread_value,
                home_score,
                away_score,
                mins_remaining,
            ),
        )

        conn.commit()
        conn.close()

    def get_opening_snapshot(self, game_id: str) -> dict | None:
        """Get the opening line snapshot for a game."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM line_snapshots
            WHERE game_id = ? AND is_opening = 1
            ORDER BY timestamp ASC LIMIT 1
        """,
            (game_id,),
        )

        row = cursor.fetchone()
        conn.close()

        if row:
            return dict(row)
        return None

    def get_line_snapshots(self, game_id: str) -> list[dict]:
        """Get all line snapshots for a game."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM line_snapshots
            WHERE game_id = ?
            ORDER BY timestamp ASC
        """,
            (game_id,),
        )

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

        cursor.execute(
            """
            INSERT OR REPLACE INTO game_results
            (game_id, final_score_home, final_score_away, completed_at)
            VALUES (?, ?, ?, ?)
        """,
            (
                game_id,
                final_score_home,
                final_score_away,
                datetime.utcnow().isoformat(),
            ),
        )

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

    def get_all_games_with_odds_and_results(self) -> list[dict]:
        """Get all games that have cached odds and final results for backtesting.

        Returns list of dicts with game info, opening/midgame odds, and final margin.
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        # Get all games with results
        cursor.execute("""
            SELECT g.id, g.home_team, g.away_team, g.sport,
                   r.home_score, r.away_score
            FROM games g
            JOIN game_results r ON g.id = r.game_id
        """)

        games = []
        for row in cursor.fetchall():
            game = dict(row)
            game_id = game["id"]

            # Get opening odds
            cursor.execute(
                """
                SELECT spread_home, spread_away
                FROM historical_odds_cache
                WHERE game_id = ? AND snapshot_type = 'opening'
            """,
                (game_id,),
            )
            opening = cursor.fetchone()

            # Get midgame odds
            cursor.execute(
                """
                SELECT spread_home, spread_away
                FROM historical_odds_cache
                WHERE game_id = ? AND snapshot_type = 'midgame'
            """,
                (game_id,),
            )
            midgame = cursor.fetchone()

            if opening and midgame:
                game["open_home_spread"] = opening["spread_home"]
                game["mid_home_spread"] = midgame["spread_home"]
                game["final_margin"] = game["home_score"] - game["away_score"]
                games.append(game)

        conn.close()
        return games

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
        """Record a hypothetical bet for later analysis (writes to both tables)."""
        conn = self._get_conn()
        cursor = conn.cursor()

        now = datetime.utcnow().isoformat()

        # Write to legacy table
        cursor.execute(
            """
            INSERT INTO bet_outcomes
            (game_id, alert_id, bet_type, bet_team, bet_spread,
             mins_remaining, opening_spread, pct_change,
             final_margin, covered, profit)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)
        """,
            (
                game_id,
                alert_id,
                bet_type,
                bet_team,
                bet_spread,
                mins_remaining,
                opening_spread,
                pct_change,
            ),
        )

        legacy_bet_id = cursor.lastrowid

        # Also write to unified table
        cursor.execute(
            """
            INSERT INTO bets
            (game_id, source, strategy, bet_team, bet_spread,
             opening_spread, pct_change, mins_remaining, alert_id,
             final_margin, covered, profit, created_at)
            VALUES (?, 'live', ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?)
        """,
            (
                game_id,
                bet_type,
                bet_team,
                bet_spread,
                opening_spread,
                pct_change,
                mins_remaining,
                alert_id,
                now,
            ),
        )

        conn.commit()
        conn.close()

        return legacy_bet_id

    def update_bet_outcome(
        self,
        bet_id: int,
        final_margin: int,
        covered: bool,
        profit: float,
    ) -> None:
        """Update bet outcome after game completes (updates both tables)."""
        conn = self._get_conn()
        cursor = conn.cursor()

        # Update legacy table
        cursor.execute(
            """
            UPDATE bet_outcomes
            SET final_margin = ?, covered = ?, profit = ?
            WHERE id = ?
        """,
            (
                final_margin,
                1 if covered else 0,
                profit,
                bet_id,
            ),
        )

        # Also update unified table (match by alert_id since we don't store the mapping)
        # First get the alert_id from the legacy table
        cursor.execute("SELECT alert_id FROM bet_outcomes WHERE id = ?", (bet_id,))
        row = cursor.fetchone()
        if row and row["alert_id"]:
            cursor.execute(
                """
                UPDATE bets
                SET final_margin = ?, covered = ?, profit = ?
                WHERE alert_id = ? AND source = 'live'
            """,
                (
                    final_margin,
                    1 if covered else 0,
                    profit,
                    row["alert_id"],
                ),
            )

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

        cursor.execute(
            """
            SELECT g.*
            FROM games g
            LEFT JOIN game_results r ON g.id = r.game_id
            WHERE r.game_id IS NULL
            AND g.commence_time < ?
            ORDER BY g.commence_time DESC
        """,
            (cutoff,),
        )

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

        cursor.execute(
            """
            INSERT INTO alerts (game_id, alert_type, message, sent_at)
            VALUES (?, ?, ?, ?)
        """,
            (
                game_id,
                alert_type,
                message,
                datetime.utcnow().isoformat(),
            ),
        )

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

        cursor.execute(
            """
            INSERT OR REPLACE INTO historical_odds_cache
            (game_id, sport, snapshot_type, timestamp, bookmaker,
             spread_home, spread_away, spread_home_price, spread_away_price,
             moneyline_home, moneyline_away, total, over_price, under_price,
             fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
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
            ),
        )

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

        cursor.execute(
            """
            SELECT * FROM historical_odds_cache
            WHERE game_id = ? AND snapshot_type = ? AND bookmaker = ?
        """,
            (game_id, snapshot_type, bookmaker),
        )

        row = cursor.fetchone()
        conn.close()

        if row:
            return dict(row)
        return None

    def get_all_cached_odds_for_game(self, game_id: str) -> list[dict]:
        """Get all cached odds snapshots for a game."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM historical_odds_cache
            WHERE game_id = ?
            ORDER BY timestamp ASC
        """,
            (game_id,),
        )

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
        """Save a simulated bet from historical analysis (writes to both tables)."""
        conn = self._get_conn()
        cursor = conn.cursor()

        now = datetime.utcnow().isoformat()

        # Write to legacy table
        cursor.execute(
            """
            INSERT INTO simulated_bets
            (game_id, sport, strategy, bet_team, bet_spread,
             opening_spread, pct_change, snapshot_type,
             final_margin, covered, profit, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?)
        """,
            (
                game_id,
                sport,
                strategy,
                bet_team,
                bet_spread,
                opening_spread,
                pct_change,
                snapshot_type,
                now,
            ),
        )

        legacy_bet_id = cursor.lastrowid

        # Also write to unified table
        cursor.execute(
            """
            INSERT INTO bets
            (game_id, source, strategy, bet_team, bet_spread,
             opening_spread, pct_change, snapshot_type,
             final_margin, covered, profit, created_at)
            VALUES (?, 'backtest', ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?)
        """,
            (
                game_id,
                strategy,
                bet_team,
                bet_spread,
                opening_spread,
                pct_change,
                snapshot_type,
                now,
            ),
        )

        conn.commit()
        conn.close()

        return legacy_bet_id

    def update_simulated_bet(
        self,
        bet_id: int,
        final_margin: int,
        covered: bool,
        profit: float,
    ) -> None:
        """Update simulated bet with outcome (updates both tables)."""
        conn = self._get_conn()
        cursor = conn.cursor()

        # Update legacy table
        cursor.execute(
            """
            UPDATE simulated_bets
            SET final_margin = ?, covered = ?, profit = ?
            WHERE id = ?
        """,
            (
                final_margin,
                1 if covered else 0,
                profit,
                bet_id,
            ),
        )

        # Get info to find matching bet in unified table
        cursor.execute(
            "SELECT game_id, strategy, bet_spread FROM simulated_bets WHERE id = ?", (bet_id,)
        )
        row = cursor.fetchone()
        if row:
            # Update unified table by matching key fields
            cursor.execute(
                """
                UPDATE bets
                SET final_margin = ?, covered = ?, profit = ?
                WHERE game_id = ? AND strategy = ? AND bet_spread = ? 
                AND source = 'backtest' AND covered IS NULL
            """,
                (
                    final_margin,
                    1 if covered else 0,
                    profit,
                    row["game_id"],
                    row["strategy"],
                    row["bet_spread"],
                ),
            )

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
            resolved = [b for b in resolved if b.get("pct_change", 0) >= min_pct_change]

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
            "SELECT COUNT(*) as count FROM historical_odds_cache WHERE game_id = ?",
            (game_id,),
        )

        row = cursor.fetchone()
        conn.close()

        return row["count"] > 0

    def save_opening_line_cache(
        self,
        game_id: str,
        sport: str,
        spread_value: float,
        spread_team: str,
        home_team: str,
    ) -> None:
        """Cache opening line for a game to avoid repeated historical API calls.

        Args:
            game_id: Game ID
            sport: Sport key (e.g., 'basketball_nba')
            spread_value: The spread value (e.g., -3.5)
            spread_team: Team the spread is quoted for
            home_team: Home team name
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        now = datetime.utcnow().isoformat()

        cursor.execute(
            """
            INSERT OR REPLACE INTO opening_line_cache
            (game_id, sport, spread_value, spread_team, home_team, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (game_id, sport, spread_value, spread_team, home_team, now),
        )

        conn.commit()
        conn.close()

    def get_opening_line_cache(self, game_id: str) -> dict | None:
        """Get cached opening line for a game.

        Args:
            game_id: Game ID

        Returns:
            Dict with spread_value, spread_team, home_team, fetched_at or None
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT sport, spread_value, spread_team, home_team, fetched_at
            FROM opening_line_cache
            WHERE game_id = ?
            """,
            (game_id,),
        )

        row = cursor.fetchone()
        conn.close()

        if not row:
            return None

        return {
            "sport": row["sport"],
            "spread_value": row["spread_value"],
            "spread_team": row["spread_team"],
            "home_team": row["home_team"],
            "fetched_at": row["fetched_at"],
        }
