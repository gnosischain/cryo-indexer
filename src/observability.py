"""
Observability module for cryo-indexer.
Provides Prometheus metrics, health endpoint, and structured JSON logging.
"""
import json
import time
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional, Dict, Any

from prometheus_client import (
    Counter, Histogram, Gauge,
    generate_latest, CONTENT_TYPE_LATEST
)


# ============================================================
# Prometheus Metrics
# ============================================================

RANGE_DURATION_BUCKETS = (0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000)

# Indexing metrics
blocks_indexed_total = Counter(
    'cryo_blocks_indexed_total',
    'Total ranges successfully indexed',
    ['dataset']
)

blocks_failed_total = Counter(
    'cryo_blocks_failed_total',
    'Total ranges that failed',
    ['dataset']
)

range_duration_seconds = Histogram(
    'cryo_range_duration_seconds',
    'Time to process one block range',
    ['dataset', 'operation'],
    buckets=RANGE_DURATION_BUCKETS
)

cryo_extract_duration_seconds = Histogram(
    'cryo_cryo_extract_duration_seconds',
    'Cryo extraction time',
    ['dataset'],
    buckets=RANGE_DURATION_BUCKETS
)

clickhouse_insert_duration_seconds = Histogram(
    'cryo_clickhouse_insert_duration_seconds',
    'ClickHouse insert time',
    ['dataset'],
    buckets=RANGE_DURATION_BUCKETS
)

rows_inserted_total = Counter(
    'cryo_rows_inserted_total',
    'Total rows inserted into ClickHouse',
    ['dataset']
)

highest_completed_block = Gauge(
    'cryo_highest_completed_block',
    'Highest completed block number',
    ['dataset']
)

chain_head_block = Gauge(
    'cryo_chain_head_block',
    'Latest block from RPC'
)

chain_lag_blocks = Gauge(
    'cryo_chain_lag_blocks',
    'Blocks behind chain head',
    ['dataset']
)

ranges_in_progress = Gauge(
    'cryo_ranges_in_progress',
    'Currently processing ranges',
    ['dataset']
)

auto_maintain_recovered_total = Counter(
    'cryo_auto_maintain_recovered_total',
    'Ranges recovered by auto-maintain',
    ['reason']  # stuck, failed, zero_rows
)


# ============================================================
# Health State (updated by the indexer)
# ============================================================

_health_state: Dict[str, Any] = {
    'status': 'starting',
    'clickhouse_connected': False,
    'rpc_connected': False,
    'current_block': 0,
    'chain_head': 0,
    'lag_blocks': 0,
    'operation': '',
    'datasets': []
}
_health_lock = threading.Lock()


def update_health(**kwargs):
    """Update health state. Thread-safe."""
    with _health_lock:
        _health_state.update(kwargs)


def get_health() -> Dict[str, Any]:
    """Get current health state. Thread-safe."""
    with _health_lock:
        return dict(_health_state)


# ============================================================
# HTTP Server for /metrics and /health
# ============================================================

class MetricsHandler(BaseHTTPRequestHandler):
    """HTTP handler for /metrics and /health endpoints."""

    def do_GET(self):
        if self.path == '/metrics':
            output = generate_latest()
            self.send_response(200)
            self.send_header('Content-Type', CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(output)
        elif self.path == '/health':
            health = get_health()
            status_code = 200 if health.get('clickhouse_connected') else 503
            self.send_response(status_code)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(health).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        """Suppress default HTTP logging."""
        pass


def start_metrics_server(port: int = 9090):
    """Start the metrics/health HTTP server in a background thread."""
    server = HTTPServer(('0.0.0.0', port), MetricsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logging.getLogger(__name__).info(f"Metrics server started on port {port}")
    return server


# ============================================================
# Structured JSON Logging
# ============================================================

class JsonFormatter(logging.Formatter):
    """JSON log formatter matching Cerebro MCP pattern."""

    RESERVED_ATTRS = {
        'args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename',
        'funcName', 'levelname', 'levelno', 'lineno', 'module', 'msecs',
        'message', 'msg', 'name', 'pathname', 'process', 'processName',
        'relativeCreated', 'stack_info', 'thread', 'threadName', 'taskName'
    }

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            'timestamp': self.formatTime(record, '%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage()
        }

        # Add extra fields (event, dataset, etc.)
        for key, value in record.__dict__.items():
            if key not in self.RESERVED_ATTRS and not key.startswith('_'):
                log_data[key] = value

        return json.dumps(log_data, default=str)


def log_event(logger_instance, event: str, **kwargs):
    """Log a structured event with extra fields."""
    logger_instance.info(
        kwargs.pop('message', event),
        extra={'event': event, **kwargs}
    )


def setup_json_logging(level: str = 'INFO'):
    """Configure all loggers to use JSON format."""
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
