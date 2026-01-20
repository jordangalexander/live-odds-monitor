"""Alert system for notifying about betting opportunities."""

import logging
from datetime import datetime
from typing import Optional, Callable, List
from abc import ABC, abstractmethod

from ..db.models import Alert


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class AlertHandler(ABC):
    """Abstract base class for alert handlers."""
    
    @abstractmethod
    def send(self, alert: Alert) -> bool:
        """Send an alert.
        
        Args:
            alert: Alert to send
            
        Returns:
            True if sent successfully
        """
        pass


class ConsoleAlertHandler(AlertHandler):
    """Prints alerts to console."""
    
    def send(self, alert: Alert) -> bool:
        """Print alert to console.
        
        Args:
            alert: Alert to send
            
        Returns:
            Always True
        """
        print("\n" + "="*60)
        print(alert.message)
        print("="*60 + "\n")
        return True


class LoggingAlertHandler(AlertHandler):
    """Logs alerts using Python logging."""
    
    def send(self, alert: Alert) -> bool:
        """Log alert.
        
        Args:
            alert: Alert to send
            
        Returns:
            Always True
        """
        logger.warning(f"ALERT: {alert.alert_type} - {alert.message}")
        return True


class FileAlertHandler(AlertHandler):
    """Writes alerts to a file."""
    
    def __init__(self, file_path: str = "alerts.log"):
        """Initialize file handler.
        
        Args:
            file_path: Path to alerts file
        """
        self.file_path = file_path
    
    def send(self, alert: Alert) -> bool:
        """Write alert to file.
        
        Args:
            alert: Alert to send
            
        Returns:
            True if written successfully
        """
        try:
            with open(self.file_path, "a") as f:
                f.write(f"\n{'='*60}\n")
                f.write(f"Time: {alert.timestamp.isoformat()}\n")
                f.write(f"Type: {alert.alert_type}\n")
                f.write(f"{alert.message}\n")
                f.write(f"{'='*60}\n")
            return True
        except Exception as e:
            logger.error(f"Failed to write alert to file: {e}")
            return False


class SMSAlertHandler(AlertHandler):
    """Sends alerts via SMS (placeholder for future implementation)."""
    
    def __init__(self, phone_number: str, api_key: Optional[str] = None):
        """Initialize SMS handler.
        
        Args:
            phone_number: Phone number to send SMS to
            api_key: API key for SMS service (e.g., Twilio)
        """
        self.phone_number = phone_number
        self.api_key = api_key
    
    def send(self, alert: Alert) -> bool:
        """Send alert via SMS.
        
        Args:
            alert: Alert to send
            
        Returns:
            True if sent successfully
        """
        # TODO: Implement Twilio or other SMS provider
        logger.info(f"SMS would be sent to {self.phone_number}: {alert.message}")
        print(f"[SMS PLACEHOLDER] Would send to {self.phone_number}")
        return True


class AlertManager:
    """Manages multiple alert handlers."""
    
    def __init__(self):
        """Initialize alert manager."""
        self.handlers: List[AlertHandler] = []
    
    def add_handler(self, handler: AlertHandler) -> None:
        """Add an alert handler.
        
        Args:
            handler: Handler to add
        """
        self.handlers.append(handler)
    
    def remove_handler(self, handler: AlertHandler) -> None:
        """Remove an alert handler.
        
        Args:
            handler: Handler to remove
        """
        self.handlers.remove(handler)
    
    def send_alert(self, alert: Alert) -> bool:
        """Send alert through all handlers.
        
        Args:
            alert: Alert to send
            
        Returns:
            True if at least one handler succeeded
        """
        if not self.handlers:
            logger.warning("No alert handlers configured!")
            return False
        
        success = False
        for handler in self.handlers:
            try:
                if handler.send(alert):
                    success = True
            except Exception as e:
                logger.error(f"Handler {handler.__class__.__name__} failed: {e}")
        
        if success:
            alert.sent = True
        
        return success


def create_default_alert_manager() -> AlertManager:
    """Create an alert manager with default handlers.
    
    Returns:
        AlertManager with console and file handlers
    """
    manager = AlertManager()
    manager.add_handler(ConsoleAlertHandler())
    manager.add_handler(FileAlertHandler())
    manager.add_handler(LoggingAlertHandler())
    return manager
