"""Metrics collection and monitoring."""

import time
import threading
from collections import defaultdict, deque
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class MetricPoint:
    """Single metric data point."""
    timestamp: datetime
    value: float
    tags: Dict[str, str]


class MetricsCollector:
    """Collect and store application metrics."""
    
    def __init__(self, retention_hours: int = 24):
        """Initialize metrics collector.
        
        Args:
            retention_hours: How long to retain metrics in memory
        """
        self.retention_hours = retention_hours
        self.metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self.counters: Dict[str, float] = defaultdict(float)
        self.gauges: Dict[str, float] = defaultdict(float)
        self.histograms: Dict[str, List[float]] = defaultdict(list)
        self._lock = threading.Lock()
        
        # Start cleanup thread
        self._start_cleanup_thread()
    
    def increment_counter(self, name: str, value: float = 1.0, tags: Dict[str, str] = None):
        """Increment a counter metric.
        
        Args:
            name: Metric name
            value: Value to add
            tags: Optional tags
        """
        with self._lock:
            full_name = self._get_metric_name(name, tags)
            self.counters[full_name] += value
            
            # Store point for time series
            point = MetricPoint(
                timestamp=datetime.utcnow(),
                value=value,
                tags=tags or {}
            )
            self.metrics[name].append(point)
    
    def set_gauge(self, name: str, value: float, tags: Dict[str, str] = None):
        """Set a gauge metric value.
        
        Args:
            name: Metric name
            value: Current value
            tags: Optional tags
        """
        with self._lock:
            full_name = self._get_metric_name(name, tags)
            self.gauges[full_name] = value
            
            # Store point for time series
            point = MetricPoint(
                timestamp=datetime.utcnow(),
                value=value,
                tags=tags or {}
            )
            self.metrics[name].append(point)
    
    def record_histogram(self, name: str, value: float, tags: Dict[str, str] = None):
        """Record a value in a histogram.
        
        Args:
            name: Metric name
            value: Value to record
            tags: Optional tags
        """
        with self._lock:
            full_name = self._get_metric_name(name, tags)
            self.histograms[full_name].append(value)
            
            # Keep only recent values (last 1000)
            if len(self.histograms[full_name]) > 1000:
                self.histograms[full_name] = self.histograms[full_name][-1000:]
            
            # Store point for time series
            point = MetricPoint(
                timestamp=datetime.utcnow(),
                value=value,
                tags=tags or {}
            )
            self.metrics[name].append(point)
    
    def time_function(self, name: str, tags: Dict[str, str] = None):
        """Decorator to time function execution.
        
        Args:
            name: Metric name
            tags: Optional tags
            
        Returns:
            Decorator function
        """
        def decorator(func):
            def wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    return result
                finally:
                    duration = time.time() - start_time
                    self.record_histogram(f"{name}.duration", duration, tags)
                    self.increment_counter(f"{name}.calls", 1.0, tags)
            return wrapper
        return decorator
    
    def get_counter(self, name: str, tags: Dict[str, str] = None) -> float:
        """Get counter value.
        
        Args:
            name: Metric name
            tags: Optional tags
            
        Returns:
            float: Counter value
        """
        full_name = self._get_metric_name(name, tags)
        return self.counters.get(full_name, 0.0)
    
    def get_gauge(self, name: str, tags: Dict[str, str] = None) -> float:
        """Get gauge value.
        
        Args:
            name: Metric name
            tags: Optional tags
            
        Returns:
            float: Gauge value
        """
        full_name = self._get_metric_name(name, tags)
        return self.gauges.get(full_name, 0.0)
    
    def get_histogram_stats(self, name: str, tags: Dict[str, str] = None) -> Dict[str, float]:
        """Get histogram statistics.
        
        Args:
            name: Metric name
            tags: Optional tags
            
        Returns:
            Dict[str, float]: Histogram statistics
        """
        full_name = self._get_metric_name(name, tags)
        values = self.histograms.get(full_name, [])
        
        if not values:
            return {"count": 0, "min": 0, "max": 0, "mean": 0, "p50": 0, "p95": 0, "p99": 0}
        
        sorted_values = sorted(values)
        count = len(values)
        
        return {
            "count": count,
            "min": min(values),
            "max": max(values),
            "mean": sum(values) / count,
            "p50": self._percentile(sorted_values, 0.5),
            "p95": self._percentile(sorted_values, 0.95),
            "p99": self._percentile(sorted_values, 0.99)
        }
    
    def get_time_series(self, name: str, hours: int = 1) -> List[MetricPoint]:
        """Get time series data for a metric.
        
        Args:
            name: Metric name
            hours: Number of hours to look back
            
        Returns:
            List[MetricPoint]: Time series data
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        points = []
        
        for point in self.metrics.get(name, []):
            if point.timestamp >= cutoff_time:
                points.append(point)
        
        return points
    
    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all current metrics.
        
        Returns:
            Dict[str, Any]: All metrics data
        """
        with self._lock:
            return {
                "counters": dict(self.counters),
                "gauges": dict(self.gauges),
                "histograms": {
                    name: self.get_histogram_stats(name.split("|")[0], 
                                                  self._parse_tags(name.split("|")[1]) if "|" in name else None)
                    for name in self.histograms.keys()
                },
                "timestamp": datetime.utcnow().isoformat()
            }
    
    def reset_metrics(self):
        """Reset all metrics."""
        with self._lock:
            self.counters.clear()
            self.gauges.clear()
            self.histograms.clear()
            self.metrics.clear()
    
    def _get_metric_name(self, name: str, tags: Dict[str, str] = None) -> str:
        """Get full metric name with tags.
        
        Args:
            name: Base metric name
            tags: Optional tags
            
        Returns:
            str: Full metric name
        """
        if not tags:
            return name
        
        tag_string = ",".join(f"{k}={v}" for k, v in sorted(tags.items()))
        return f"{name}|{tag_string}"
    
    def _parse_tags(self, tag_string: str) -> Dict[str, str]:
        """Parse tags from string.
        
        Args:
            tag_string: Tag string
            
        Returns:
            Dict[str, str]: Parsed tags
        """
        tags = {}
        for tag in tag_string.split(","):
            if "=" in tag:
                key, value = tag.split("=", 1)
                tags[key] = value
        return tags
    
    def _percentile(self, sorted_values: List[float], percentile: float) -> float:
        """Calculate percentile from sorted values.
        
        Args:
            sorted_values: Sorted list of values
            percentile: Percentile to calculate (0.0 to 1.0)
            
        Returns:
            float: Percentile value
        """
        if not sorted_values:
            return 0.0
        
        index = int(percentile * (len(sorted_values) - 1))
        return sorted_values[index]
    
    def _cleanup_old_metrics(self):
        """Remove old metrics to prevent memory leaks."""
        cutoff_time = datetime.utcnow() - timedelta(hours=self.retention_hours)
        
        with self._lock:
            for name, points in list(self.metrics.items()):
                # Remove old points
                while points and points[0].timestamp < cutoff_time:
                    points.popleft()
                
                # Remove empty metrics
                if not points:
                    del self.metrics[name]
    
    def _start_cleanup_thread(self):
        """Start background thread for metrics cleanup."""
        def cleanup_loop():
            while True:
                try:
                    time.sleep(3600)  # Run every hour
                    self._cleanup_old_metrics()
                except Exception as e:
                    logger.error(f"Error in metrics cleanup: {e}")
        
        cleanup_thread = threading.Thread(target=cleanup_loop, daemon=True)
        cleanup_thread.start()


# Global metrics collector instance
metrics = MetricsCollector()


# Common metrics functions
def track_scraping_metrics(scraper_name: str, job_id: str = None):
    """Track scraping metrics.
    
    Args:
        scraper_name: Name of the scraper
        job_id: Optional job ID
    """
    tags = {"scraper": scraper_name}
    if job_id:
        tags["job_id"] = job_id
    
    def decorator(func):
        def wrapper(*args, **kwargs):
            metrics.increment_counter("scraping.jobs.started", 1.0, tags)
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                metrics.increment_counter("scraping.jobs.completed", 1.0, tags)
                return result
            except Exception as e:
                metrics.increment_counter("scraping.jobs.failed", 1.0, tags)
                raise
            finally:
                duration = time.time() - start_time
                metrics.record_histogram("scraping.job.duration", duration, tags)
        
        return wrapper
    return decorator


def track_api_metrics(endpoint: str, method: str):
    """Track API metrics.
    
    Args:
        endpoint: API endpoint
        method: HTTP method
    """
    tags = {"endpoint": endpoint, "method": method}
    
    def decorator(func):
        def wrapper(*args, **kwargs):
            metrics.increment_counter("api.requests", 1.0, tags)
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                metrics.increment_counter("api.requests.success", 1.0, tags)
                return result
            except Exception as e:
                metrics.increment_counter("api.requests.error", 1.0, tags)
                raise
            finally:
                duration = time.time() - start_time
                metrics.record_histogram("api.request.duration", duration, tags)
        
        return wrapper
    return decorator


def track_etl_metrics(process_name: str, batch_id: str = None):
    """Track ETL metrics.
    
    Args:
        process_name: Name of the ETL process
        batch_id: Optional batch ID
    """
    tags = {"process": process_name}
    if batch_id:
        tags["batch_id"] = batch_id
    
    def decorator(func):
        def wrapper(*args, **kwargs):
            metrics.increment_counter("etl.batches.started", 1.0, tags)
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                metrics.increment_counter("etl.batches.completed", 1.0, tags)
                
                # Track specific ETL metrics if result contains them
                if isinstance(result, dict):
                    if "processed" in result:
                        metrics.increment_counter("etl.records.processed", result["processed"], tags)
                    if "saved" in result:
                        metrics.increment_counter("etl.records.saved", result["saved"], tags)
                    if "errors" in result:
                        metrics.increment_counter("etl.records.errors", result["errors"], tags)
                
                return result
            except Exception as e:
                metrics.increment_counter("etl.batches.failed", 1.0, tags)
                raise
            finally:
                duration = time.time() - start_time
                metrics.record_histogram("etl.batch.duration", duration, tags)
        
        return wrapper
    return decorator


def update_system_metrics():
    """Update system-level metrics."""
    try:
        import psutil
        
        # CPU metrics
        cpu_percent = psutil.cpu_percent(interval=1)
        metrics.set_gauge("system.cpu.usage", cpu_percent)
        
        # Memory metrics
        memory = psutil.virtual_memory()
        metrics.set_gauge("system.memory.usage", memory.percent)
        metrics.set_gauge("system.memory.available", memory.available)
        metrics.set_gauge("system.memory.total", memory.total)
        
        # Disk metrics
        disk = psutil.disk_usage('/')
        metrics.set_gauge("system.disk.usage", disk.percent)
        metrics.set_gauge("system.disk.free", disk.free)
        metrics.set_gauge("system.disk.total", disk.total)
        
        # Network metrics
        net_io = psutil.net_io_counters()
        metrics.set_gauge("system.network.bytes_sent", net_io.bytes_sent)
        metrics.set_gauge("system.network.bytes_recv", net_io.bytes_recv)
        
    except ImportError:
        logger.warning("psutil not available, skipping system metrics")
    except Exception as e:
        logger.error(f"Error collecting system metrics: {e}")


def start_system_metrics_collection():
    """Start background collection of system metrics."""
    def collection_loop():
        while True:
            try:
                update_system_metrics()
                time.sleep(60)  # Update every minute
            except Exception as e:
                logger.error(f"Error in system metrics collection: {e}")
                time.sleep(60)
    
    metrics_thread = threading.Thread(target=collection_loop, daemon=True)
    metrics_thread.start()
    logger.info("System metrics collection started")
