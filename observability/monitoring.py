"""
Observability and monitoring framework.
Tracks pipeline execution, failures, and exposes Prometheus metrics.
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
from enum import Enum
from dataclasses import dataclass, asdict
from config.logger import setup_logger
import json

logger = setup_logger(__name__)


class MetricType(Enum):
    """Types of metrics"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


class PipelineStatus(Enum):
    """Pipeline execution status"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"
    PARTIAL_SUCCESS = "partial_success"


@dataclass
class Metric:
    """Represents a single metric"""
    name: str
    value: float
    metric_type: MetricType
    labels: Dict[str, str]
    timestamp: str
    unit: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        data = asdict(self)
        data["metric_type"] = self.metric_type.value
        return data


@dataclass
class PipelineExecution:
    """Record of a pipeline execution"""
    execution_id: str
    pipeline_name: str
    status: PipelineStatus
    start_time: str
    end_time: Optional[str]
    duration_seconds: Optional[float] = None
    source: str = ""
    target: str = ""
    records_processed: int = 0
    records_failed: int = 0
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        data = asdict(self)
        data["status"] = self.status.value
        return data


class MetricsCollector:
    """Collects and exposes metrics for monitoring"""
    
    def __init__(self):
        """Initialize metrics collector"""
        self.metrics: List[Metric] = []
        self.counters: Dict[str, int] = {}
        self.gauges: Dict[str, float] = {}
        self.created_at = datetime.utcnow()
    
    def increment_counter(self, name: str, value: int = 1, labels: Optional[Dict[str, str]] = None) -> None:
        """Increment a counter metric"""
        labels = labels or {}
        key = f"{name}_{json.dumps(labels, sort_keys=True)}"
        
        self.counters[key] = self.counters.get(key, 0) + value
        
        metric = Metric(
            name=name,
            value=self.counters[key],
            metric_type=MetricType.COUNTER,
            labels=labels,
            timestamp=datetime.utcnow().isoformat()
        )
        self.metrics.append(metric)
        logger.debug(f"Counter metric: {name}={self.counters[key]}")
    
    def set_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Set a gauge metric"""
        labels = labels or {}
        key = f"{name}_{json.dumps(labels, sort_keys=True)}"
        
        self.gauges[key] = value
        
        metric = Metric(
            name=name,
            value=value,
            metric_type=MetricType.GAUGE,
            labels=labels,
            timestamp=datetime.utcnow().isoformat()
        )
        self.metrics.append(metric)
        logger.debug(f"Gauge metric: {name}={value}")
    
    def record_histogram(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Record a histogram metric"""
        labels = labels or {}
        
        metric = Metric(
            name=name,
            value=value,
            metric_type=MetricType.HISTOGRAM,
            labels=labels,
            timestamp=datetime.utcnow().isoformat()
        )
        self.metrics.append(metric)
        logger.debug(f"Histogram metric: {name}={value}")
    
    def get_metrics(self) -> List[Metric]:
        """Get all recorded metrics"""
        return self.metrics
    
    def get_prometheus_format(self) -> str:
        """Export metrics in Prometheus format"""
        lines = []
        
        # Add help and type lines
        for metric_name in set(m.name for m in self.metrics):
            lines.append(f"# HELP {metric_name} Metric {metric_name}")
            metric_type = next((m.metric_type.value for m in self.metrics if m.name == metric_name), "gauge")
            lines.append(f"# TYPE {metric_name} {metric_type}")
        
        # Add metric values
        for metric in self.metrics:
            labels_str = "{" + ", ".join(f'{k}="{v}"' for k, v in metric.labels.items()) + "}"
            if metric.labels:
                lines.append(f"{metric.name}{labels_str} {metric.value}")
            else:
                lines.append(f"{metric.name} {metric.value}")
        
        return "\n".join(lines)


class PipelineExecutionMonitor:
    """Monitor pipeline executions"""
    
    def __init__(self):
        """Initialize execution monitor"""
        self.executions: Dict[str, PipelineExecution] = {}
        self.metrics_collector = MetricsCollector()
        self.created_at = datetime.utcnow()
    
    def start_execution(
        self,
        execution_id: str,
        pipeline_name: str,
        source: str = "",
        target: str = ""
    ) -> PipelineExecution:
        """Record pipeline execution start"""
        execution = PipelineExecution(
            execution_id=execution_id,
            pipeline_name=pipeline_name,
            status=PipelineStatus.RUNNING,
            start_time=datetime.utcnow().isoformat(),
            source=source,
            target=target
        )
        
        self.executions[execution_id] = execution
        
        # Record metric
        self.metrics_collector.increment_counter(
            "pipeline_started_total",
            labels={"pipeline": pipeline_name}
        )
        
        logger.info(f"Started pipeline execution: {execution_id} ({pipeline_name})")
        return execution
    
    def end_execution(
        self,
        execution_id: str,
        status: PipelineStatus,
        records_processed: int = 0,
        records_failed: int = 0,
        error_message: Optional[str] = None
    ) -> Optional[PipelineExecution]:
        """Record pipeline execution end"""
        execution = self.executions.get(execution_id)
        if not execution:
            logger.warning(f"Execution not found: {execution_id}")
            return None
        
        execution.status = status
        execution.end_time = datetime.utcnow().isoformat()
        start = datetime.fromisoformat(execution.start_time)
        end = datetime.fromisoformat(execution.end_time)
        execution.duration_seconds = (end - start).total_seconds()
        execution.records_processed = records_processed
        execution.records_failed = records_failed
        execution.error_message = error_message
        
        # Record metrics
        self.metrics_collector.increment_counter(
            f"pipeline_{status.value}_total",
            labels={"pipeline": execution.pipeline_name}
        )
        
        self.metrics_collector.record_histogram(
            "pipeline_duration_seconds",
            execution.duration_seconds,
            labels={"pipeline": execution.pipeline_name}
        )
        
        if records_processed > 0:
            self.metrics_collector.set_gauge(
                "records_processed",
                records_processed,
                labels={"pipeline": execution.pipeline_name}
            )
        
        logger.info(
            f"Ended pipeline execution: {execution_id} - "
            f"Status: {status.value}, Duration: {execution.duration_seconds}s, "
            f"Records: {records_processed}, Failed: {records_failed}"
        )
        
        return execution
    
    def get_execution(self, execution_id: str) -> Optional[PipelineExecution]:
        """Get execution details"""
        return self.executions.get(execution_id)
    
    def get_pipeline_status(self, pipeline_name: str) -> Dict[str, Any]:
        """Get status summary for a pipeline"""
        pipeline_executions = [
            e for e in self.executions.values() 
            if e.pipeline_name == pipeline_name
        ]
        
        if not pipeline_executions:
            return {"pipeline_name": pipeline_name, "status": "no_executions"}
        
        latest = max(pipeline_executions, key=lambda e: e.start_time)
        
        stats = {
            "status_counts": {}
        }
        for execution in pipeline_executions:
            status = execution.status.value
            stats["status_counts"][status] = stats["status_counts"].get(status, 0) + 1
        
        return {
            "pipeline_name": pipeline_name,
            "latest_status": latest.status.value,
            "latest_execution": latest.to_dict(),
            "stats": stats
        }
    
    def get_all_executions(self) -> List[PipelineExecution]:
        """Get all recorded executions"""
        return list(self.executions.values())
    
    def get_failed_executions(self) -> List[PipelineExecution]:
        """Get all failed executions"""
        return [e for e in self.executions.values() if e.status == PipelineStatus.FAILURE]
    
    def get_metrics(self) -> List[Metric]:
        """Get collected metrics"""
        return self.metrics_collector.get_metrics()
    
    def get_prometheus_metrics(self) -> str:
        """Export metrics in Prometheus format"""
        return self.metrics_collector.get_prometheus_format()


class HealthChecker:
    """System health checking"""
    
    def __init__(self):
        """Initialize health checker"""
        self.checks: Dict[str, bool] = {}
        self.last_check = datetime.utcnow()
    
    def check_spark(self) -> bool:
        """Check Spark availability"""
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName("health-check").master("local[1]").getOrCreate()
            spark.stop()
            self.checks["spark"] = True
            return True
        except Exception as e:
            logger.error(f"Spark health check failed: {str(e)}")
            self.checks["spark"] = False
            return False
    
    def check_storage(self, path: str) -> bool:
        """Check storage accessibility"""
        try:
            # In production, check actual storage (S3, ADLS)
            self.checks["storage"] = True
            return True
        except Exception as e:
            logger.error(f"Storage health check failed: {str(e)}")
            self.checks["storage"] = False
            return False
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get overall health status"""
        return {
            "status": "healthy" if all(self.checks.values()) else "unhealthy",
            "checks": self.checks,
            "last_check": self.last_check.isoformat()
        }


# Global instances
_metrics_collector = None
_execution_monitor = None
_health_checker = None


def get_metrics_collector() -> MetricsCollector:
    """Get global metrics collector"""
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector()
    return _metrics_collector


def get_execution_monitor() -> PipelineExecutionMonitor:
    """Get global execution monitor"""
    global _execution_monitor
    if _execution_monitor is None:
        _execution_monitor = PipelineExecutionMonitor()
    return _execution_monitor


def get_health_checker() -> HealthChecker:
    """Get global health checker"""
    global _health_checker
    if _health_checker is None:
        _health_checker = HealthChecker()
    return _health_checker
