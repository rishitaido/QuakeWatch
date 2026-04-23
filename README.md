# QuakeWatch

A real-time earthquake monitoring platform built with Python microservices, AWS (SQS + DynamoDB), Docker, and a Leaflet.js geo dashboard.

---

## Architecture

```text
USGS GeoJSON feed
  -> ingester (Python)
  -> SQS queue
  -> processor (Python)
  -> DynamoDB tables: earthquakes, alerts, cities

The API reads from DynamoDB and serves JSON to the dashboard.
The alert-evaluator reads from DynamoDB and writes alert records.
```

---

## Services

| Service | Owner | Description |
|---------|-------|-------------|
| `ingester` | Rishi | Polls USGS every 60 s, deduplicates by event ID, publishes to SQS |
| `api` | Rishi | FastAPI - serves `/earthquakes`, `/alerts`, `/stats` |
| `processor` | Asha | Consumes SQS messages, calculates Haversine impact scores, writes to DynamoDB |
| `alert-evaluator` | Asha | Monitors earthquakes table, creates alert records for high/medium severity events |
| `dashboard` | Hania | Nginx-served Leaflet map with real-time markers, sidebar, and filters |

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Compose v2)
- [AWS CLI](https://aws.amazon.com/cli/) configured (or equivalent AWS credentials available to the containers)
- An AWS account with:
  - An SQS queue named `quakewatch-queue`
  - Three DynamoDB tables: `earthquakes`, `alerts`, `cities`

---

## Quick Start

### 1. Clone the repo

```bash
git clone https://github.com/rishitaido/quakewatch.git
cd quakewatch
```

### 2. Create `.env`

```bash
cat > .env << 'EOF'
AWS_ACCESS_KEY_ID=your_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_access_key
AWS_REGION=us-east-1

SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/<account-id>/quakewatch-queue
EARTHQUAKES_TABLE=earthquakes
ALERTS_TABLE=alerts
CITIES_TABLE=cities

USGS_FEED_URL=https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson
POLL_INTERVAL_SECONDS=60
API_PORT=8000

HIGH_SEVERITY_MAG=6.0
HIGH_SEVERITY_IMPACT=80
MEDIUM_SEVERITY_MAG=4.5
MEDIUM_SEVERITY_IMPACT=40
MIN_MAG_FOR_IMPACT_ALERT=0
IMPACT_RADIUS_KM=300
EOF
```

If you already created the queue, you can fetch `SQS_QUEUE_URL` with:

```bash
aws sqs get-queue-url \
  --queue-name quakewatch-queue \
  --region us-east-1 \
  --query QueueUrl \
  --output text
```

### 3. Build images

```bash
docker compose build
```

### 4. Start the stack

```bash
docker compose up -d
```

### 5. Verify services

```bash
docker compose ps
curl -fsS http://localhost:8000/health
```

### 6. Access the dashboard

Open [http://localhost](http://localhost) in your browser.

The REST API is available at [http://localhost:8000](http://localhost:8000).

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/earthquakes` | List recent earthquakes (optional: `?min_mag=4.5&limit=50`) |
| `GET` | `/alerts` | List active alerts |
| `GET` | `/stats` | Summary stats (count, avg magnitude, top region) |
| `GET` | `/health` | Health check |

---

## DynamoDB Table Schemas

### `earthquakes`
| Attribute | Type | Notes |
|-----------|------|-------|
| `event_id` | String | Partition key (USGS event ID) |
| `magnitude` | Number | Richter magnitude |
| `place` | String | Human-readable location |
| `time` | Number | Unix timestamp (ms) |
| `lat` / `lon` | Number | Coordinates |
| `impact_score` | Number | Computed by processor |

### `alerts`
| Attribute | Type | Notes |
|-----------|------|-------|
| `alert_id` | String | Partition key (UUID) |
| `event_id` | String | FK -> earthquakes |
| `severity` | String | `HIGH` or `MEDIUM` |
| `created_at` | String | ISO 8601 timestamp |

### `cities`
| Attribute | Type | Notes |
|-----------|------|-------|
| `city_id` | String | Partition key |
| `name` | String | City name |
| `lat` / `lon` | Number | Coordinates |
| `population` | Number | Used for impact score calculation |

---

## AWS Setup

### Create the SQS Queue

```bash
aws sqs create-queue --queue-name quakewatch-queue --region us-east-1
```

### Create DynamoDB Tables

```bash
# Earthquakes table
aws dynamodb create-table \
  --table-name earthquakes \
  --attribute-definitions AttributeName=event_id,AttributeType=S \
  --key-schema AttributeName=event_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1

# Alerts table
aws dynamodb create-table \
  --table-name alerts \
  --attribute-definitions AttributeName=alert_id,AttributeType=S \
  --key-schema AttributeName=alert_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1

# Cities table
aws dynamodb create-table \
  --table-name cities \
  --attribute-definitions AttributeName=city_id,AttributeType=S \
  --key-schema AttributeName=city_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1
```

### Seed City Data

```bash
# from repo root
set -a && source .env && set +a
pip install -r seed-data/requirements.txt
python seed-data/seed_cities.py

# optional tuning (examples):
# GEONAMES_DATASET=cities15000 CITIES_MAX_COUNT=1000 python seed-data/seed_cities.py
# GEONAMES_DATASET=cities5000 CITIES_MIN_POPULATION=10000 python seed-data/seed_cities.py
# MIN_MAG_FOR_IMPACT_ALERT=4 controls when impact score can elevate severity

# restart processor so it reloads the refreshed cities table
docker compose restart processor
```

### Backfill Existing Earthquake Impact Scores

```bash
# dry-run first (recommended)
set -a && source .env && set +a
python seed-data/backfill_earthquake_impacts.py --hours 240

# apply updates
python seed-data/backfill_earthquake_impacts.py --hours 240 --apply
```

---

## Stopping Services

```bash
docker compose down
```

---

## Team

| Member | Role |
|--------|------|
| Rishi | Infrastructure, Seismic Ingester, REST API |
| Asha | Impact Processor, Alert Evaluator, Seed Data |
| Hania | Geo Dashboard (Leaflet + Nginx) |

---

## License

MIT
