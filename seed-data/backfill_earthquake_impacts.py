"""
QuakeWatch - Earthquake Impact Backfill
Recompute impact/severity fields for existing earthquake rows using the
current city dataset and scoring logic.

Typical usage:
  set -a && source .env && set +a
  python seed-data/backfill_earthquake_impacts.py --hours 240 --apply
"""

import argparse
import math
import os
import re
import time
from dataclasses import dataclass
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError


# ── Configuration ─────────────────────────────────────────────────────────────
EARTHQUAKES_TABLE = os.environ.get("EARTHQUAKES_TABLE", "earthquakes")
CITIES_TABLE = os.environ.get("CITIES_TABLE", "cities")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

HIGH_SEVERITY_MAG = float(os.environ.get("HIGH_SEVERITY_MAG", "6.0"))
HIGH_SEVERITY_IMPACT = float(os.environ.get("HIGH_SEVERITY_IMPACT", "80"))
MEDIUM_SEVERITY_MAG = float(os.environ.get("MEDIUM_SEVERITY_MAG", "4.5"))
MEDIUM_SEVERITY_IMPACT = float(os.environ.get("MEDIUM_SEVERITY_IMPACT", "40"))
MIN_MAG_FOR_IMPACT_ALERT = float(os.environ.get("MIN_MAG_FOR_IMPACT_ALERT", "0"))

IMPACT_RADIUS_KM = float(os.environ.get("IMPACT_RADIUS_KM", "300"))
IMPACT_MAGNITUDE_EXPONENT = 2.0
IMPACT_POPULATION_DIVISOR = 700.0
IMPACT_DISTANCE_EXPONENT = 1.4
IMPACT_DISTANCE_FLOOR = 25.0
IMPACT_LOG_SCALE = 25.0


@dataclass
class Counters:
    scanned: int = 0
    considered: int = 0
    changed: int = 0
    updated: int = 0
    skipped_missing_fields: int = 0


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    lat1_r = math.radians(lat1)
    lat2_r = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def parse_place_reference(place: str) -> dict | None:
    if not isinstance(place, str):
        return None
    normalized = " ".join(place.strip().split())
    if not normalized:
        return None
    match = re.match(
        r"^(?P<distance>\d+(?:\.\d+)?)\s*km\s+[A-Z]{1,3}\s+of\s+(?P<locality>.+)$",
        normalized,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    locality_full = match.group("locality").strip(" -")
    if not locality_full:
        return None
    locality_name = locality_full.split(",", 1)[0].strip() or locality_full
    locality_country = ""
    if "," in locality_full:
        locality_country = locality_full.split(",", 1)[1].strip()
    return {
        "name": locality_name,
        "country": locality_country,
        "distance_km": float(match.group("distance")),
    }


def calculate_impact_score(magnitude: float, lat: float, lon: float, cities: list[dict]) -> dict:
    if not cities:
        return {
            "impact_score": 0.0,
            "nearest_city": "Unknown",
            "nearest_city_country": "",
            "nearest_city_dist_km": -1.0,
            "nearby_cities_count": 0,
        }

    raw_impact_sum = 0.0
    nearest_city = None
    nearest_distance = float("inf")
    nearby_count = 0

    for city in cities:
        dist = haversine(lat, lon, city["lat"], city["lon"])
        if dist < nearest_distance:
            nearest_distance = dist
            nearest_city = city
        if dist <= IMPACT_RADIUS_KM:
            nearby_count += 1
            city_impact = (
                (magnitude ** IMPACT_MAGNITUDE_EXPONENT)
                * (city["population"] / IMPACT_POPULATION_DIVISOR)
                / (dist ** IMPACT_DISTANCE_EXPONENT + IMPACT_DISTANCE_FLOOR)
            )
            raw_impact_sum += city_impact

    impact_score = (
        min(100.0, IMPACT_LOG_SCALE * math.log10(1 + raw_impact_sum))
        if raw_impact_sum > 0
        else 0.0
    )
    return {
        "impact_score": round(impact_score, 1),
        "nearest_city": nearest_city["name"] if nearest_city else "Unknown",
        "nearest_city_country": nearest_city["country"] if nearest_city else "",
        "nearest_city_dist_km": round(nearest_distance, 1) if nearest_city else -1.0,
        "nearby_cities_count": nearby_count,
    }


def apply_place_reference_override(impact: dict, place: str) -> dict:
    reference = parse_place_reference(place)
    if not reference:
        return impact

    current_name = str(impact.get("nearest_city") or "Unknown")
    try:
        current_distance = float(impact.get("nearest_city_dist_km", -1))
    except (TypeError, ValueError):
        current_distance = -1

    reference_distance = float(reference["distance_km"])
    should_override = (
        current_name == "Unknown"
        or current_distance < 0
        or (reference_distance + 25.0) < current_distance
    )
    if not should_override:
        return impact

    adjusted = dict(impact)
    adjusted["nearest_city"] = reference["name"]
    adjusted["nearest_city_country"] = reference["country"]
    adjusted["nearest_city_dist_km"] = round(reference_distance, 1)
    return adjusted


def determine_severity(magnitude: float, impact_score: float) -> str:
    impact_high = (
        impact_score >= HIGH_SEVERITY_IMPACT
        and magnitude >= MIN_MAG_FOR_IMPACT_ALERT
    )
    impact_medium = (
        impact_score >= MEDIUM_SEVERITY_IMPACT
        and magnitude >= MIN_MAG_FOR_IMPACT_ALERT
    )

    if magnitude >= HIGH_SEVERITY_MAG or impact_high:
        return "high"
    if magnitude >= MEDIUM_SEVERITY_MAG or impact_medium:
        return "medium"
    return "low"


def load_cities(table) -> list[dict]:
    cities = []
    response = table.scan()
    cities.extend(response.get("Items", []))
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        cities.extend(response.get("Items", []))
    return [
        {
            "name": c.get("name", "Unknown"),
            "country": c.get("country", ""),
            "lat": float(c.get("lat", 0)),
            "lon": float(c.get("lon", 0)),
            "population": int(c.get("population", 0)),
        }
        for c in cities
    ]


def decimal_or(value, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def is_changed(item: dict, impact: dict, severity: str) -> bool:
    old_impact = round(decimal_or(item.get("impact_score"), 0.0), 1)
    old_dist = round(decimal_or(item.get("nearest_city_dist_km"), -1.0), 1)
    old_count = int(decimal_or(item.get("nearby_cities_count"), 0))
    old_city = str(item.get("nearest_city", "Unknown"))
    old_country = str(item.get("nearest_city_country", ""))
    old_severity = str(item.get("severity", "")).lower()

    return any(
        [
            old_impact != impact["impact_score"],
            old_city != impact["nearest_city"],
            old_country != impact.get("nearest_city_country", ""),
            old_dist != impact["nearest_city_dist_km"],
            old_count != impact["nearby_cities_count"],
            old_severity != severity,
        ]
    )


def scan_recent_earthquakes(table, hours: int) -> list[dict]:
    cutoff_ms = int(time.time() * 1000) - hours * 3_600_000
    filter_expr = Attr("timestamp").gte(cutoff_ms) | Attr("time").gte(cutoff_ms)
    projection = (
        "event_id, #ts, #time, magnitude, lat, lon, place, impact_score, "
        "nearest_city, nearest_city_country, nearest_city_dist_km, nearby_cities_count, severity"
    )
    kwargs = {
        "FilterExpression": filter_expr,
        "ProjectionExpression": projection,
        "ExpressionAttributeNames": {"#ts": "timestamp", "#time": "time"},
    }
    items = []
    response = table.scan(**kwargs)
    items.extend(response.get("Items", []))
    while "LastEvaluatedKey" in response:
        kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
        response = table.scan(**kwargs)
        items.extend(response.get("Items", []))
    return items


def backfill(hours: int, limit: int, apply: bool) -> Counters:
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    eq_table = dynamodb.Table(EARTHQUAKES_TABLE)
    cities_table = dynamodb.Table(CITIES_TABLE)

    print(f"Loading cities from '{CITIES_TABLE}'...")
    cities = load_cities(cities_table)
    print(f"Loaded {len(cities)} cities")

    print(f"Scanning '{EARTHQUAKES_TABLE}' for last {hours} hour(s)...")
    events = scan_recent_earthquakes(eq_table, hours)
    events.sort(key=lambda x: int(x.get("timestamp") or x.get("time") or 0), reverse=True)
    if limit > 0:
        events = events[:limit]
    print(f"Found {len(events)} candidate events")

    counters = Counters(scanned=len(events))
    sample_changes = []

    for idx, item in enumerate(events, start=1):
        magnitude = decimal_or(item.get("magnitude"), None)
        lat = decimal_or(item.get("lat"), None)
        lon = decimal_or(item.get("lon"), None)
        if magnitude is None or lat is None or lon is None:
            counters.skipped_missing_fields += 1
            continue

        counters.considered += 1
        impact = calculate_impact_score(magnitude, lat, lon, cities)
        impact = apply_place_reference_override(impact, str(item.get("place", "")))
        severity = determine_severity(magnitude, impact["impact_score"])

        changed = is_changed(item, impact, severity)
        if changed:
            counters.changed += 1
            if len(sample_changes) < 8:
                sample_changes.append(
                    (
                        item.get("event_id"),
                        float(item.get("impact_score", 0)),
                        impact["impact_score"],
                        item.get("nearest_city", "Unknown"),
                        impact["nearest_city"],
                    )
                )

            if apply:
                eq_table.update_item(
                    Key={"event_id": item["event_id"]},
                    UpdateExpression=(
                        "SET impact_score=:impact, nearest_city=:nearest_city, "
                        "nearest_city_country=:nearest_city_country, "
                        "nearest_city_dist_km=:nearest_city_dist_km, "
                        "nearby_cities_count=:nearby_cities_count, "
                        "severity=:severity, processed_at=:processed_at"
                    ),
                    ExpressionAttributeValues={
                        ":impact": Decimal(str(impact["impact_score"])),
                        ":nearest_city": impact["nearest_city"],
                        ":nearest_city_country": impact.get("nearest_city_country", ""),
                        ":nearest_city_dist_km": Decimal(str(impact["nearest_city_dist_km"])),
                        ":nearby_cities_count": int(impact["nearby_cities_count"]),
                        ":severity": severity,
                        ":processed_at": int(time.time() * 1000),
                    },
                )
                counters.updated += 1

        if idx % 100 == 0:
            action = "updated" if apply else "would update"
            print(
                f"Processed {idx}/{len(events)} | changed={counters.changed} | "
                f"{action}={counters.updated if apply else counters.changed}"
            )

    print()
    print("Sample changes (event_id | old_impact -> new_impact | old_city -> new_city):")
    for entry in sample_changes:
        print(f"  {entry[0]} | {entry[1]} -> {entry[2]} | {entry[3]} -> {entry[4]}")
    if not sample_changes:
        print("  (no changed events)")

    return counters


def main():
    parser = argparse.ArgumentParser(description="Backfill earthquake impact/severity fields")
    parser.add_argument(
        "--hours",
        type=int,
        default=240,
        help="Only process events in the last N hours (default: 240)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process at most N most-recent events (default: all in window)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write updates to DynamoDB (default: dry-run)",
    )
    args = parser.parse_args()

    if args.hours < 1:
        raise SystemExit("--hours must be >= 1")
    if args.limit < 0:
        raise SystemExit("--limit must be >= 0")

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"Backfill mode: {mode}")
    print(
        f"Config: region={AWS_REGION}, eq_table={EARTHQUAKES_TABLE}, cities_table={CITIES_TABLE}, "
        f"radius_km={IMPACT_RADIUS_KM}, min_mag_for_impact_alert={MIN_MAG_FOR_IMPACT_ALERT}"
    )
    print()

    try:
        counters = backfill(hours=args.hours, limit=args.limit, apply=args.apply)
    except ClientError as exc:
        raise SystemExit(f"AWS error during backfill: {exc}") from exc

    print()
    print("Summary:")
    print(f"  scanned: {counters.scanned}")
    print(f"  considered: {counters.considered}")
    print(f"  changed: {counters.changed}")
    print(f"  updated: {counters.updated}")
    print(f"  skipped_missing_fields: {counters.skipped_missing_fields}")


if __name__ == "__main__":
    main()
