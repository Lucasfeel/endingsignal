import json
import os
import time

from flask import current_app, g, has_request_context, jsonify, request


_TRUTHY_VALUES = {"1", "true", "t", "yes", "y", "on"}
_METRIC_KEY_DB_BORROW = "db_borrow_or_connect_ms"
_METRIC_KEY_DB_SQL = "db_sql_ms"
_METRIC_KEY_DB_FETCH = "db_fetch_ms"
_METRIC_KEY_JSONIFY = "jsonify_ms"
_REQUEST_METRIC_KEYS = (
    _METRIC_KEY_DB_BORROW,
    _METRIC_KEY_DB_SQL,
    _METRIC_KEY_DB_FETCH,
    _METRIC_KEY_JSONIFY,
)


def _env_truthy(name, default=False):
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in _TRUTHY_VALUES


def perf_logging_enabled():
    return _env_truthy("ES_PERF_LOG", default=False)


def init_request_perf():
    if not has_request_context():
        return
    g._req_start = time.perf_counter()
    g._req_perf_metrics = {key: 0.0 for key in _REQUEST_METRIC_KEYS}


def add_request_metric(metric_key, elapsed_ms):
    if not has_request_context():
        return
    metrics = getattr(g, "_req_perf_metrics", None)
    if metrics is None:
        metrics = {key: 0.0 for key in _REQUEST_METRIC_KEYS}
        g._req_perf_metrics = metrics
    metrics[metric_key] = float(metrics.get(metric_key, 0.0)) + max(0.0, float(elapsed_ms))


def add_db_borrow_or_connect_ms(elapsed_ms):
    add_request_metric(_METRIC_KEY_DB_BORROW, elapsed_ms)


def add_db_sql_ms(elapsed_ms):
    add_request_metric(_METRIC_KEY_DB_SQL, elapsed_ms)


def add_db_fetch_ms(elapsed_ms):
    add_request_metric(_METRIC_KEY_DB_FETCH, elapsed_ms)


def add_jsonify_ms(elapsed_ms):
    add_request_metric(_METRIC_KEY_JSONIFY, elapsed_ms)


def jsonify_timed(payload, status_code=None):
    start = time.perf_counter()
    response = jsonify(payload)
    add_jsonify_ms((time.perf_counter() - start) * 1000.0)
    if status_code is not None:
        response.status_code = int(status_code)
    return response


def log_request_perf(response):
    if not has_request_context():
        return response
    if not perf_logging_enabled():
        return response

    start = getattr(g, "_req_start", None)
    if start is None:
        return response

    total_ms = max(0.0, (time.perf_counter() - start) * 1000.0)
    metrics = getattr(g, "_req_perf_metrics", {}) or {}
    db_borrow_or_connect_ms = float(metrics.get(_METRIC_KEY_DB_BORROW, 0.0))
    db_sql_ms = float(metrics.get(_METRIC_KEY_DB_SQL, 0.0))
    db_fetch_ms = float(metrics.get(_METRIC_KEY_DB_FETCH, 0.0))
    jsonify_ms = float(metrics.get(_METRIC_KEY_JSONIFY, 0.0))
    python_other_ms = max(
        0.0,
        total_ms - db_borrow_or_connect_ms - db_sql_ms - db_fetch_ms - jsonify_ms,
    )

    payload = {
        "method": request.method,
        "path": request.path,
        "status_code": response.status_code,
        "total_ms": round(total_ms, 3),
        "db_borrow_or_connect_ms": round(db_borrow_or_connect_ms, 3),
        "db_sql_ms": round(db_sql_ms, 3),
        "db_fetch_ms": round(db_fetch_ms, 3),
        "jsonify_ms": round(jsonify_ms, 3),
        "python_other_ms": round(python_other_ms, 3),
    }
    current_app.logger.info("[perf] %s", json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return response
