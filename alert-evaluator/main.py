"""
QuakeWatch - Alert Evaluator Service
Monitors new earthquake events in DynamoDB and creates alerts
for high and medium severity events.
Owner: Asha
"""

import os
import sys
import time
import logging
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

# ── Configuration ──────────────────────────────────────────
EARTHQUAKES_TABLE = os.environ.get("EARTHQUAKES_TABLE", "earthquakes")
ALERTS_TABLE = os.environ.get("ALERTS_TABLE", "alerts")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

POLL_INTERVAL = 30  # Check for new events every 30 seconds
ALERT_MAX_EVENT_AGE_HOURS = int(os.environ.get("ALERT_MAX_EVENT_AGE_HOURS", "24"))

# ── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ALERT-EVAL] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("alert-evaluator")

# ── AWS Clients ────────────────────────────────────────────
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
earthquakes_table = dynamodb.Table(EARTHQUAKES_TABLE)
alerts_table = dynamodb.Table(ALERTS_TABLE)

# Track which events we've already evaluated
evaluated_event_ids = set()


def load_existing_alerts():
    """
    On startup, load event_ids of existing alerts so we don't create duplicates.
    """
    global evaluated_event_ids
    try:
        response = alerts_table.scan(ProjectionExpression="event_id")
        items = response.get("Items", [])

        while "LastEvaluatedKey" in response:
            response = alerts_table.scan(
                ProjectionExpression="event_id",
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            items.extend(response.get("Items", []))

        evaluated_event_ids = {item["event_id"] for item in items}
        logger.info(f"Loaded {len(evaluated_event_ids)} existing alert event IDs")

    except ClientError as e:
        logger.error(f"Failed to load existing alerts: {e}")


def scan_for_new_events() -> list[dict]:
    """
    Scan DynamoDB for earthquake events that haven't been evaluated yet.
    Only returns high and medium severity events (low severity doesn't get alerts).
    """
    try:
        filter_expr = Attr("severity").is_in(["high", "medium", "HIGH", "MEDIUM"])
        if ALERT_MAX_EVENT_AGE_HOURS > 0:
            cutoff_ms = int(time.time() * 1000) - ALERT_MAX_EVENT_AGE_HOURS * 3_600_000
            recency_filter = Attr("timestamp").gte(cutoff_ms) | Attr("time").gte(cutoff_ms)
            filter_expr = filter_expr & recency_filter

        response = earthquakes_table.scan(
            FilterExpression=filter_expr,
        )
        items = response.get("Items", [])

        while "LastEvaluatedKey" in response:
            response = earthquakes_table.scan(
                FilterExpression=filter_expr,
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            items.extend(response.get("Items", []))

        # Filter out already-evaluated events
        new_events = [
            item
            for item in items
            if item.get("event_id") and item["event_id"] not in evaluated_event_ids
        ]

        return new_events

    except ClientError as e:
        logger.error(f"Failed to scan earthquakes: {e}")
        return []


def create_alert(event: dict):
    """
    Write an alert record to the alerts DynamoDB table.
    """
    try:
        alert_id = str(uuid.uuid4())[:8]
        severity = str(event.get("severity", "medium")).lower()
        magnitude = event.get("magnitude", Decimal("0"))

        alert = {
            "alert_id": alert_id,
            "event_id": event["event_id"],
            "severity": severity,
            "timestamp": int(event.get("timestamp", 0)),
            "description": f"M{magnitude} earthquake near {event.get('place')}",
            "magnitude": magnitude,
            "impact_score": event.get("impact_score", Decimal("0")),
            "place": event.get("place", "Unknown"),
            "nearest_city": event.get("nearest_city", "Unknown"),
            "lat": event.get("lat", Decimal("0")),
            "lon": event.get("lon", Decimal("0")),
            "created_at": int(datetime.now(timezone.utc).timestamp() * 1000),
        }

        alerts_table.put_item(Item=alert)

        # Mark as evaluated
        evaluated_event_ids.add(event["event_id"])

        logger.info(
            f"ALERT CREATED [{severity.upper()}]: {event['event_id']} | "
            f"mag={event.get('magnitude')} | impact={event.get('impact_score')} | "
            f"{event.get('place')}"
        )

    except ClientError as e:
        logger.error(f"Failed to create alert for {event['event_id']}: {e}")


def run_evaluation_cycle():
    """
    Single evaluation cycle: scan for new high/medium events and create alerts.
    """
    new_events = scan_for_new_events()

    if new_events:
        logger.info(f"Found {len(new_events)} new alertable events")
        for event in new_events:
            create_alert(event)
    # Only log periodically to avoid spam when there are no new events


def main():
    """
    Main loop: continuously check for new events that need alerts.
    """
    logger.info("=" * 60)
    logger.info("QuakeWatch Alert Evaluator starting")
    logger.info(f"Poll interval: {POLL_INTERVAL}s")
    logger.info(f"Max earthquake age for new alerts: {ALERT_MAX_EVENT_AGE_HOURS}h")
    logger.info("=" * 60)

    # Load existing alerts to avoid duplicates on restart
    load_existing_alerts()

    cycle_count = 0
    while True:
        try:
            run_evaluation_cycle()
            cycle_count += 1

            # Log heartbeat every 10 cycles (~5 minutes)
            if cycle_count % 10 == 0:
                logger.info(
                    f"Heartbeat: {len(evaluated_event_ids)} total events evaluated"
                )

            time.sleep(POLL_INTERVAL)

        except Exception as e:
            logger.error(f"Evaluation cycle failed: {e}")
            time.sleep(10)


if __name__ == "__main__":
    main()
