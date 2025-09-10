"""Alert management and notification system."""

import smtplib
import time
import threading
from abc import ABC, abstractmethod
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AlertStatus(Enum):
    """Alert status."""
    ACTIVE = "active"
    RESOLVED = "resolved"
    ACKNOWLEDGED = "acknowledged"


@dataclass
class Alert:
    """Alert data structure."""
    id: str
    title: str
    description: str
    severity: AlertSeverity
    status: AlertStatus
    source: str
    tags: Dict[str, str]
    created_at: datetime
    updated_at: datetime
    resolved_at: Optional[datetime] = None
    acknowledged_at: Optional[datetime] = None


class AlertChannel(ABC):
    """Abstract base class for alert notification channels."""
    
    @abstractmethod
    def send_alert(self, alert: Alert) -> bool:
        """Send an alert notification.
        
        Args:
            alert: Alert to send
            
        Returns:
            bool: True if successful, False otherwise
        """
        pass


class EmailAlertChannel(AlertChannel):
    """Email alert notification channel."""
    
    def __init__(self, smtp_host: str, smtp_port: int, username: str, 
                 password: str, from_email: str, to_emails: List[str],
                 use_tls: bool = True):
        """Initialize email alert channel.
        
        Args:
            smtp_host: SMTP server host
            smtp_port: SMTP server port
            username: SMTP username
            password: SMTP password
            from_email: From email address
            to_emails: List of recipient email addresses
            use_tls: Whether to use TLS
        """
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.from_email = from_email
        self.to_emails = to_emails
        self.use_tls = use_tls
    
    def send_alert(self, alert: Alert) -> bool:
        """Send alert via email.
        
        Args:
            alert: Alert to send
            
        Returns:
            bool: True if successful
        """
        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.from_email
            msg['To'] = ', '.join(self.to_emails)
            msg['Subject'] = f"[{alert.severity.value.upper()}] {alert.title}"
            
            # Email body
            body = self._format_alert_email(alert)
            msg.attach(MIMEText(body, 'html'))
            
            # Send email
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()
                server.login(self.username, self.password)
                server.send_message(msg)
            
            logger.info(f"Alert email sent: {alert.id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send alert email: {e}")
            return False
    
    def _format_alert_email(self, alert: Alert) -> str:
        """Format alert as HTML email.
        
        Args:
            alert: Alert to format
            
        Returns:
            str: HTML email body
        """
        severity_color = {
            AlertSeverity.LOW: "#28a745",
            AlertSeverity.MEDIUM: "#ffc107",
            AlertSeverity.HIGH: "#fd7e14",
            AlertSeverity.CRITICAL: "#dc3545"
        }
        
        color = severity_color.get(alert.severity, "#6c757d")
        
        return f"""
        <html>
        <body>
            <h2 style="color: {color};">{alert.title}</h2>
            <p><strong>Severity:</strong> <span style="color: {color};">{alert.severity.value.upper()}</span></p>
            <p><strong>Source:</strong> {alert.source}</p>
            <p><strong>Created:</strong> {alert.created_at.strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
            <p><strong>Status:</strong> {alert.status.value}</p>
            
            <h3>Description</h3>
            <p>{alert.description}</p>
            
            {self._format_tags(alert.tags)}
        </body>
        </html>
        """
    
    def _format_tags(self, tags: Dict[str, str]) -> str:
        """Format alert tags as HTML.
        
        Args:
            tags: Alert tags
            
        Returns:
            str: HTML formatted tags
        """
        if not tags:
            return ""
        
        tag_html = "<h3>Tags</h3><ul>"
        for key, value in tags.items():
            tag_html += f"<li><strong>{key}:</strong> {value}</li>"
        tag_html += "</ul>"
        
        return tag_html


class LogAlertChannel(AlertChannel):
    """Log-based alert channel (for testing/debugging)."""
    
    def send_alert(self, alert: Alert) -> bool:
        """Send alert to logs.
        
        Args:
            alert: Alert to send
            
        Returns:
            bool: Always True
        """
        logger.warning(
            f"ALERT [{alert.severity.value.upper()}] {alert.title}: {alert.description}",
            extra={
                "alert_id": alert.id,
                "severity": alert.severity.value,
                "source": alert.source,
                "tags": alert.tags
            }
        )
        return True


class AlertRule:
    """Alert rule definition."""
    
    def __init__(self, name: str, condition: Callable[[Dict[str, Any]], bool],
                 severity: AlertSeverity, title: str, description: str,
                 tags: Dict[str, str] = None, cooldown_minutes: int = 60):
        """Initialize alert rule.
        
        Args:
            name: Rule name
            condition: Function that evaluates the condition
            severity: Alert severity
            title: Alert title
            description: Alert description
            tags: Optional tags
            cooldown_minutes: Cooldown period between alerts
        """
        self.name = name
        self.condition = condition
        self.severity = severity
        self.title = title
        self.description = description
        self.tags = tags or {}
        self.cooldown_minutes = cooldown_minutes
        self.last_triggered: Optional[datetime] = None


class AlertManager:
    """Manage alerts and notification channels."""
    
    def __init__(self):
        """Initialize alert manager."""
        self.channels: List[AlertChannel] = []
        self.rules: List[AlertRule] = []
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_history: List[Alert] = []
        self._running = False
        self._thread: Optional[threading.Thread] = None
    
    def add_channel(self, channel: AlertChannel):
        """Add notification channel.
        
        Args:
            channel: Alert channel to add
        """
        self.channels.append(channel)
        logger.info(f"Added alert channel: {type(channel).__name__}")
    
    def add_rule(self, rule: AlertRule):
        """Add alert rule.
        
        Args:
            rule: Alert rule to add
        """
        self.rules.append(rule)
        logger.info(f"Added alert rule: {rule.name}")
    
    def trigger_alert(self, alert_id: str, title: str, description: str,
                     severity: AlertSeverity, source: str, 
                     tags: Dict[str, str] = None) -> Alert:
        """Trigger a new alert.
        
        Args:
            alert_id: Unique alert ID
            title: Alert title
            description: Alert description
            severity: Alert severity
            source: Alert source
            tags: Optional tags
            
        Returns:
            Alert: Created alert
        """
        # Check if alert already exists
        if alert_id in self.active_alerts:
            existing_alert = self.active_alerts[alert_id]
            existing_alert.updated_at = datetime.utcnow()
            return existing_alert
        
        # Create new alert
        alert = Alert(
            id=alert_id,
            title=title,
            description=description,
            severity=severity,
            status=AlertStatus.ACTIVE,
            source=source,
            tags=tags or {},
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        self.active_alerts[alert_id] = alert
        self.alert_history.append(alert)
        
        # Send notifications
        self._send_alert_notifications(alert)
        
        logger.warning(f"Alert triggered: {alert.title} ({alert.id})")
        return alert
    
    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an active alert.
        
        Args:
            alert_id: Alert ID to resolve
            
        Returns:
            bool: True if alert was resolved
        """
        if alert_id not in self.active_alerts:
            return False
        
        alert = self.active_alerts[alert_id]
        alert.status = AlertStatus.RESOLVED
        alert.resolved_at = datetime.utcnow()
        alert.updated_at = datetime.utcnow()
        
        del self.active_alerts[alert_id]
        
        logger.info(f"Alert resolved: {alert.title} ({alert.id})")
        return True
    
    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an active alert.
        
        Args:
            alert_id: Alert ID to acknowledge
            
        Returns:
            bool: True if alert was acknowledged
        """
        if alert_id not in self.active_alerts:
            return False
        
        alert = self.active_alerts[alert_id]
        alert.status = AlertStatus.ACKNOWLEDGED
        alert.acknowledged_at = datetime.utcnow()
        alert.updated_at = datetime.utcnow()
        
        logger.info(f"Alert acknowledged: {alert.title} ({alert.id})")
        return True
    
    def get_active_alerts(self) -> List[Alert]:
        """Get all active alerts.
        
        Returns:
            List[Alert]: Active alerts
        """
        return list(self.active_alerts.values())
    
    def get_alert_history(self, hours: int = 24) -> List[Alert]:
        """Get alert history.
        
        Args:
            hours: Number of hours to look back
            
        Returns:
            List[Alert]: Historical alerts
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        return [alert for alert in self.alert_history if alert.created_at >= cutoff_time]
    
    def start_monitoring(self, check_interval: int = 60):
        """Start monitoring with alert rules.
        
        Args:
            check_interval: Check interval in seconds
        """
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(
            target=self._monitoring_loop,
            args=(check_interval,),
            daemon=True
        )
        self._thread.start()
        logger.info("Alert monitoring started")
    
    def stop_monitoring(self):
        """Stop monitoring."""
        self._running = False
        if self._thread:
            self._thread.join()
        logger.info("Alert monitoring stopped")
    
    def _send_alert_notifications(self, alert: Alert):
        """Send alert notifications to all channels.
        
        Args:
            alert: Alert to send
        """
        for channel in self.channels:
            try:
                channel.send_alert(alert)
            except Exception as e:
                logger.error(f"Failed to send alert via {type(channel).__name__}: {e}")
    
    def _monitoring_loop(self, check_interval: int):
        """Main monitoring loop.
        
        Args:
            check_interval: Check interval in seconds
        """
        while self._running:
            try:
                self._check_alert_rules()
                time.sleep(check_interval)
            except Exception as e:
                logger.error(f"Error in alert monitoring loop: {e}")
                time.sleep(check_interval)
    
    def _check_alert_rules(self):
        """Check all alert rules against current metrics."""
        from .metrics import metrics
        
        current_metrics = metrics.get_all_metrics()
        
        for rule in self.rules:
            try:
                # Check cooldown period
                if (rule.last_triggered and 
                    datetime.utcnow() - rule.last_triggered < timedelta(minutes=rule.cooldown_minutes)):
                    continue
                
                # Evaluate condition
                if rule.condition(current_metrics):
                    alert_id = f"rule_{rule.name}"
                    self.trigger_alert(
                        alert_id=alert_id,
                        title=rule.title,
                        description=rule.description,
                        severity=rule.severity,
                        source=f"alert_rule:{rule.name}",
                        tags=rule.tags
                    )
                    rule.last_triggered = datetime.utcnow()
                else:
                    # Auto-resolve if condition no longer met
                    alert_id = f"rule_{rule.name}"
                    self.resolve_alert(alert_id)
                    
            except Exception as e:
                logger.error(f"Error checking alert rule {rule.name}: {e}")


# Default alert rules
def create_default_alert_rules() -> List[AlertRule]:
    """Create default alert rules for the application.
    
    Returns:
        List[AlertRule]: Default alert rules
    """
    rules = []
    
    # High error rate alert
    def high_error_rate(metrics: Dict[str, Any]) -> bool:
        api_errors = metrics.get("counters", {}).get("api.requests.error", 0)
        api_total = metrics.get("counters", {}).get("api.requests", 1)
        error_rate = api_errors / api_total if api_total > 0 else 0
        return error_rate > 0.1  # More than 10% error rate
    
    rules.append(AlertRule(
        name="high_api_error_rate",
        condition=high_error_rate,
        severity=AlertSeverity.HIGH,
        title="High API Error Rate",
        description="API error rate is above 10%",
        tags={"component": "api"},
        cooldown_minutes=30
    ))
    
    # Failed scraping jobs alert
    def failed_scraping_jobs(metrics: Dict[str, Any]) -> bool:
        failed_jobs = metrics.get("counters", {}).get("scraping.jobs.failed", 0)
        total_jobs = metrics.get("counters", {}).get("scraping.jobs.started", 1)
        failure_rate = failed_jobs / total_jobs if total_jobs > 0 else 0
        return failure_rate > 0.2  # More than 20% failure rate
    
    rules.append(AlertRule(
        name="high_scraping_failure_rate",
        condition=failed_scraping_jobs,
        severity=AlertSeverity.MEDIUM,
        title="High Scraping Failure Rate",
        description="Scraping job failure rate is above 20%",
        tags={"component": "scraping"},
        cooldown_minutes=60
    ))
    
    # System resource alerts
    def high_memory_usage(metrics: Dict[str, Any]) -> bool:
        memory_usage = metrics.get("gauges", {}).get("system.memory.usage", 0)
        return memory_usage > 90  # More than 90% memory usage
    
    rules.append(AlertRule(
        name="high_memory_usage",
        condition=high_memory_usage,
        severity=AlertSeverity.CRITICAL,
        title="High Memory Usage",
        description="System memory usage is above 90%",
        tags={"component": "system"},
        cooldown_minutes=15
    ))
    
    def high_cpu_usage(metrics: Dict[str, Any]) -> bool:
        cpu_usage = metrics.get("gauges", {}).get("system.cpu.usage", 0)
        return cpu_usage > 95  # More than 95% CPU usage
    
    rules.append(AlertRule(
        name="high_cpu_usage",
        condition=high_cpu_usage,
        severity=AlertSeverity.HIGH,
        title="High CPU Usage",
        description="System CPU usage is above 95%",
        tags={"component": "system"},
        cooldown_minutes=15
    ))
    
    return rules


# Global alert manager instance
alert_manager = AlertManager()
