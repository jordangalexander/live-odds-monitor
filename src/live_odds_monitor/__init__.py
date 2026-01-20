"""Live odds monitoring for NCAA basketball."""

__version__ = "0.1.0"

from .core.alerts import AlertManager, create_default_alert_manager
from .config import MonitorConfig
from .data_store import OddsSnapshotStore, OpeningLine, OpeningLinesStore, create_data_stores
from .db.models import Alert, Game, GameScore, Odds
from .core.monitor import OddsMonitor
from .api.odds_api import OddsAPIClient
from .db.storage import Storage
from .core.tracker import HistoricalTracker

__all__ = [
    "OddsAPIClient",
    "OddsMonitor",
    "MonitorConfig",
    "Game",
    "Odds",
    "GameScore",
    "Alert",
    "Storage",
    "OpeningLinesStore",
    "OpeningLine",
    "OddsSnapshotStore",
    "create_data_stores",
    "AlertManager",
    "create_default_alert_manager",
    "HistoricalTracker",
]