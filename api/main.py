"""
QuakeWatch — REST API
FastAPI server that reads from DynamoDB and exposes earthquake,
alert, and stats data to the dashboard.

Endpoints:
  GET /health          — liveness check
  GET /earthquakes     — list earthquakes (filters: min_mag, limit)
  GET /alerts          — list active alerts
  GET /stats           — summary statistics

Owner: Rishi
"""

import logging
import os
import time as time_module
from decimal import Decimal
from typing import Optional

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# ── Configuration ────────────────────────────────────────────────────────────
EARTHQUAKES_TABLE = os.environ.get("EARTHQUAKES_TABLE", "earthquakes")
ALERTS_TABLE = os.environ.get("ALERTS_TABLE", "alerts")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
API_PORT = int(os.environ.get("API_PORT", 8000))

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [API] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("api")

# ── AWS ──────────────────────────────────────────────────────────────────────
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
eq_table = dynamodb.Table(EARTHQUAKES_TABLE)
alert_table = dynamodb.Table(ALERTS_TABLE)

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(
    title="QuakeWatch API",
    description="Real-time earthquake data served from AWS DynamoDB.",
    version="1.0.0",
)

# Allow the Nginx-proxied dashboard (and local dev) to call the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def decimal_to_float(obj):
    """
    Recursively convert Decimal values (returned by boto3) to float/int
    so FastAPI can serialise them as JSON.
    """
    if isinstance(obj, list):
        return [decimal_to_float(i) for i in obj]
    if isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        # Keep whole numbers as int for cleaner JSON
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj


def normalize_severity(value: Optional[str]) -> str:
    """Normalize severity labels to lower-case canonical values."""
    return str(value or "").strip().lower()


def scan_table(table, filter_expression=None) -> list[dict]:
    """
    Perform a full DynamoDB table scan with automatic pagination.
    Use sparingly — scans consume RCUs proportional to table size.
    """
    kwargs = {}
    if filter_expression is not None:
        kwargs["FilterExpression"] = filter_expression

    items = []
    try:
        while True:
            response = table.scan(**kwargs)
            items.extend(response.get("Items", []))
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break
            kwargs["ExclusiveStartKey"] = last_key
    except ClientError as exc:
        logger.error(f"DynamoDB scan error: {exc}")
        raise HTTPException(status_code=502, detail="Database error")

    return decimal_to_float(items)


# ── Routes ───────────────────────────────────────────────────────────────────

@app.get("/health", tags=["Meta"])
def health():
    """Liveness check — returns 200 if the API is running."""
    return {"status": "ok"}


@app.get("/earthquakes", tags=["Earthquakes"])
def get_earthquakes(
    hours: Optional[int] = Query(
        default=None,
        description="Only return earthquakes within the last N hours",
        ge=1,
        le=720,
    ),
    min_mag: Optional[float] = Query(
        default=None,
        description="Minimum magnitude filter (e.g. 4.5)",
        ge=0.0,
        le=10.0,
    ),
    min_impact: Optional[float] = Query(
        default=None,
        description="Minimum impact score filter (0–100)",
        ge=0.0,
        le=100.0,
    ),
    limit: int = Query(
        default=500,
        description="Maximum number of results to return",
        ge=1,
        le=1000,
    ),
):
    """
    Return a list of earthquakes from DynamoDB, newest first.

    - **hours**: only include events from the last N hours
    - **min_mag**: optional lower bound on magnitude
    - **min_impact**: optional lower bound on impact score
    - **limit**: cap on result count (default 500, max 1000)
    """
    filter_expr = None

    if hours is not None:
        cutoff_ms = int(time_module.time() * 1000) - hours * 3_600_000
        filter_expr = Attr("timestamp").gte(cutoff_ms) | Attr("time").gte(cutoff_ms)

    if min_mag is not None:
        mag_filter = Attr("magnitude").gte(Decimal(str(min_mag)))
        filter_expr = filter_expr & mag_filter if filter_expr else mag_filter

    if min_impact is not None:
        impact_filter = Attr("impact_score").gte(Decimal(str(min_impact)))
        filter_expr = filter_expr & impact_filter if filter_expr else impact_filter

    items = scan_table(eq_table, filter_expr)
    items.sort(key=lambda x: x.get("timestamp") or x.get("time") or 0, reverse=True)

    return {"count": len(items[:limit]), "earthquakes": items[:limit]}


@app.get("/alerts", tags=["Alerts"])
def get_alerts(
    severity: Optional[str] = Query(
        default=None,
        description="Filter by severity: HIGH or MEDIUM",
    ),
    hours: Optional[int] = Query(
        default=None,
        ge=1,
        le=720,
        description="Only return alerts in the last N hours",
    ),
    limit: int = Query(default=50, ge=1, le=500),
):
    """
    Return a list of alert records, newest first.

    - **severity**: optional filter — `HIGH` or `MEDIUM`
    - **limit**: cap on result count
    """
    filter_expr = None
    severity_filter = None
    if severity:
        sev = normalize_severity(severity)
        if sev not in ("high", "medium"):
            raise HTTPException(
                status_code=400,
                detail="severity must be HIGH or MEDIUM",
            )
        severity_filter = Attr("severity").is_in([sev, sev.upper()])

    hours_filter = None
    if hours is not None:
        cutoff_ms = int(time_module.time() * 1000) - hours * 3_600_000
        # Alert recency is based on the earthquake event time, not alert creation time.
        hours_filter = Attr("timestamp").gte(cutoff_ms) | Attr("time").gte(cutoff_ms)

    if severity_filter and hours_filter:
        filter_expr = severity_filter & hours_filter
    elif severity_filter:
        filter_expr = severity_filter
    elif hours_filter:
        filter_expr = hours_filter

    items = scan_table(alert_table, filter_expr)
    items.sort(
        key=lambda x: x.get("timestamp") or x.get("time") or x.get("created_at") or 0,
        reverse=True,
    )

    return {"count": len(items[:limit]), "alerts": items[:limit]}


@app.get("/stats", tags=["Stats"])
def get_stats():
    """
    Return summary statistics computed from the earthquakes table:
    - total event count
    - average magnitude
    - highest magnitude event
    - count of HIGH and MEDIUM alerts
    """
    earthquakes = scan_table(eq_table)
    alerts = scan_table(alert_table)

    cutoff_ms = int(time_module.time() * 1000) - 24 * 3_600_000
    earthquakes_24h = [
        e for e in earthquakes if (e.get("timestamp") or e.get("time") or 0) >= cutoff_ms
    ]
    alerts_24h = [
        a for a in alerts if (a.get("created_at") or a.get("timestamp") or 0) >= cutoff_ms
    ]

    total = len(earthquakes_24h)
    magnitudes = [
        e["magnitude"] for e in earthquakes_24h if e.get("magnitude") is not None
    ]

    avg_mag = round(sum(magnitudes) / len(magnitudes), 2) if magnitudes else None
    max_mag_event = (
        max(earthquakes_24h, key=lambda e: e.get("magnitude") or 0)
        if earthquakes_24h
        else None
    )

    high_alerts = sum(1 for a in alerts_24h if normalize_severity(a.get("severity")) == "high")
    medium_alerts = sum(
        1 for a in alerts_24h if normalize_severity(a.get("severity")) == "medium"
    )

    highest_mag = max_mag_event.get("magnitude") if max_mag_event else None
    impact_scores = [
        e["impact_score"] for e in earthquakes_24h if e.get("impact_score") is not None
    ]
    highest_impact = max(impact_scores) if impact_scores else None

    return {
        "total_events_24h": total,
        "average_magnitude": avg_mag,
        "highest_magnitude": highest_mag,
        "highest_impact": highest_impact,
        "total_alerts_24h": len(alerts_24h),
        "high_alerts": high_alerts,
        "medium_alerts": medium_alerts,
    }
