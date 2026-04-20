"""
QuakeWatch - City Seed Data Script
Downloads a GeoNames cities dataset and loads cities into DynamoDB.
Default behavior loads all records from `cities5000` for better regional
coverage while keeping performance practical.

Run once before starting the system:
    python seed_cities.py

Owner: Asha
"""

import os
import sys
import json
import csv
import logging
import zipfile
import io
from decimal import Decimal

import boto3
import requests
from botocore.exceptions import ClientError

# ── Configuration ──────────────────────────────────────────
CITIES_TABLE = os.environ.get("CITIES_TABLE", "cities")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
GEONAMES_DATASET = os.environ.get("GEONAMES_DATASET", "cities5000").strip()
GEONAMES_URL = os.environ.get(
    "GEONAMES_URL",
    f"https://download.geonames.org/export/dump/{GEONAMES_DATASET}.zip",
)
GEONAMES_TXT_FILENAME = os.environ.get(
    "GEONAMES_TXT_FILENAME",
    f"{GEONAMES_DATASET}.txt",
)
CITIES_MAX_COUNT = int(os.environ.get("CITIES_MAX_COUNT", "0"))
CITIES_MIN_POPULATION = int(os.environ.get("CITIES_MIN_POPULATION", "0"))

# ── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SEED] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("seed")

# GeoNames TSV column indices (from readme.txt)
# 0: geonameid, 1: name, 2: asciiname, 3: alternatenames,
# 4: latitude, 5: longitude, 6: feature class, 7: feature code,
# 8: country code, 9: cc2, 10: admin1, 11: admin2,
# 12: admin3, 13: admin4, 14: population, 15: elevation,
# 16: dem, 17: timezone, 18: modification date
COL_ID = 0
COL_NAME = 1
COL_LAT = 4
COL_LON = 5
COL_COUNTRY = 8
COL_POPULATION = 14


def download_and_parse_cities() -> list[dict]:
    """
    Download the selected GeoNames dataset and parse cities.
    If CITIES_MAX_COUNT is > 0, keep only the top N by population.
    """
    logger.info(f"Downloading GeoNames dataset from {GEONAMES_URL}...")
    response = requests.get(GEONAMES_URL, timeout=60)
    response.raise_for_status()

    logger.info("Extracting ZIP file...")
    z = zipfile.ZipFile(io.BytesIO(response.content))
    cities = []

    with z.open(GEONAMES_TXT_FILENAME) as f:
        reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"), delimiter="\t")
        for row in reader:
            try:
                population = int(row[COL_POPULATION])
                if population <= 0 or population < CITIES_MIN_POPULATION:
                    continue

                cities.append({
                    "city_id": row[COL_ID],
                    "name": row[COL_NAME],
                    "country": row[COL_COUNTRY],
                    "lat": float(row[COL_LAT]),
                    "lon": float(row[COL_LON]),
                    "population": population,
                })
            except (ValueError, IndexError):
                continue

    # Sort by population descending
    cities.sort(key=lambda c: c["population"], reverse=True)
    if CITIES_MAX_COUNT > 0:
        selected_cities = cities[:CITIES_MAX_COUNT]
    else:
        selected_cities = cities

    logger.info(
        "Parsed %s cities total, selected %s (dataset=%s, max_count=%s, min_population=%s)",
        len(cities),
        len(selected_cities),
        GEONAMES_DATASET,
        CITIES_MAX_COUNT if CITIES_MAX_COUNT > 0 else "ALL",
        CITIES_MIN_POPULATION,
    )
    if selected_cities:
        logger.info(
            "Largest: %s (%s), Smallest in set: %s (%s)",
            selected_cities[0]["name"],
            f"{selected_cities[0]['population']:,}",
            selected_cities[-1]["name"],
            f"{selected_cities[-1]['population']:,}",
        )

    return selected_cities


def save_cities_json(cities: list[dict], filepath: str = "cities.json"):
    """Save cities to a JSON file for reference / debugging."""
    if filepath == "cities.json":
        filepath = os.path.join(os.path.dirname(__file__), "cities.json")
    with open(filepath, "w") as f:
        json.dump(cities, f, indent=2)
    logger.info(f"Saved {len(cities)} cities to {filepath}")


def upload_to_dynamodb(cities: list[dict]):
    """
    Batch-write cities to DynamoDB. Uses batch_writer which handles
    batching and retries automatically.
    """
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamodb.Table(CITIES_TABLE)

    logger.info(f"Uploading {len(cities)} cities to DynamoDB table '{CITIES_TABLE}'...")

    with table.batch_writer() as batch:
        for i, city in enumerate(cities):
            batch.put_item(
                Item={
                    "city_id": city["city_id"],
                    "name": city["name"],
                    "country": city["country"],
                    "lat": Decimal(str(round(city["lat"], 4))),
                    "lon": Decimal(str(round(city["lon"], 4))),
                    "population": city["population"],
                }
            )

            if (i + 1) % 100 == 0:
                logger.info(f"  Uploaded {i + 1}/{len(cities)} cities...")

    logger.info(f"Successfully uploaded all {len(cities)} cities to DynamoDB")


def main():
    logger.info("=" * 60)
    logger.info("QuakeWatch City Seed Script")
    logger.info(
        "Config: dataset=%s, max_count=%s, min_population=%s",
        GEONAMES_DATASET,
        CITIES_MAX_COUNT if CITIES_MAX_COUNT > 0 else "ALL",
        CITIES_MIN_POPULATION,
    )
    logger.info("=" * 60)

    # Step 1: Download and parse
    cities = download_and_parse_cities()

    # Step 2: Save local JSON copy
    save_cities_json(cities)

    # Step 3: Upload to DynamoDB
    upload_to_dynamodb(cities)

    logger.info("Seeding complete!")


if __name__ == "__main__":
    main()
