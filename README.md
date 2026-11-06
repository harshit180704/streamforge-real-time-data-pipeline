# StreamForge — Real-Time E-commerce Event Analytics

StreamForge is a local, containerized data-engineering project that ingests synthetic e-commerce events, processes them with Apache Spark Structured Streaming, and persists event-time aggregations in PostgreSQL. A Streamlit dashboard reads the stored aggregates and presents operational and business-oriented views.

> **Project status:** Working local end-to-end prototype. The producer, Kafka broker, Spark streaming job, PostgreSQL sink, and Streamlit dashboard have been exercised together. Events are synthetic; this is not a production commerce system.

## Architecture

```mermaid
flowchart LR
    P[Python event producer] -->|JSON events| K[Apache Kafka<br/>ecommerce-events]
    K -->|Kafka source| S[Apache Spark<br/>Structured Streaming]
    S --> V[Parse and validate]
    V --> W[Event-time watermark<br/>10-second windows]
    W --> A[Aggregate by event type<br/>and region]
    A -->|JDBC staging batch| PG[(PostgreSQL)]
    A -->|foreachBatch upsert| PG
    PG --> D[Streamlit dashboard]
    D --> U[KPIs, charts, recent aggregates]
```

### Data flow

1. **Produce:** `src/producer.py` emits synthetic events such as `product_view`, `add_to_cart`, `purchase`, `payment_failed`, and `inventory_update`.
2. **Transport:** Kafka stores events in the `ecommerce-events` topic. The topic is configured with three partitions in the local setup.
3. **Process:** Spark Structured Streaming reads the topic, parses JSON payloads, validates required fields and event types, and filters invalid purchase amounts.
4. **Aggregate:** Valid events are grouped into 10-second event-time windows by event type and region. A 5-second watermark is configured in the streaming aggregation.
5. **Persist:** `foreachBatch` writes each micro-batch to a PostgreSQL staging table, then uses `INSERT ... ON CONFLICT DO UPDATE` to upsert aggregate rows.
6. **Visualize:** Streamlit reads `streamforge_event_metrics` and displays event counts, gross sales, regional activity, event-type distribution, a window timeline, and recent aggregation rows.

## Technology stack

- **Language:** Python
- **Streaming:** Apache Kafka, Apache Spark Structured Streaming
- **Storage:** PostgreSQL
- **Dashboard:** Streamlit, Pandas, Plotly
- **Containers:** Docker, Docker Compose
- **Database access:** JDBC from Spark and SQLAlchemy/psycopg2 from the dashboard

## Repository layout

```text
streamforge-starter/
├── src/
│   └── producer.py
├── spark/
│   ├── Dockerfile
│   └── src/
│       └── stream_job.py
├── docker-compose.yml
├── docker-compose-spark-addition.yml
├── event_schema.json
├── requirements.txt
├── requirements-dashboard.txt
├── dashboard.py
├── README.md
└── .gitignore
```

The exact set of optional documentation files may vary by checkout.

## Prerequisites

- Windows 11 with Docker Desktop using the WSL2 backend, or another Docker-compatible environment
- Docker Compose
- Python 3.12 recommended for the local producer and dashboard
- A virtual environment for local Python dependencies

The Spark container uses its own Java runtime. The local producer and dashboard do not require a local Spark installation.

## Run locally (Windows PowerShell)

Run Compose commands from the **inner project directory** containing `docker-compose.yml` and `docker-compose-spark-addition.yml`.

### 1. Create and activate a virtual environment

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install producer dependencies:

```powershell
pip install -r requirements.txt
```

Install dashboard dependencies:

```powershell
pip install -r requirements-dashboard.txt
```

### 2. Start the infrastructure

```powershell
docker compose -f .\docker-compose.yml -f .\docker-compose-spark-addition.yml up -d --build
```

Check service status:

```powershell
docker compose -f .\docker-compose.yml -f .\docker-compose-spark-addition.yml ps
```

### 3. Start the Spark streaming job

The Spark service is included in the combined Compose configuration. Follow its logs:

```powershell
docker compose -f .\docker-compose.yml -f .\docker-compose-spark-addition.yml logs -f spark
```

Expected startup messages include:

```text
StreamForge connecting to Kafka topic 'ecommerce-events' at kafka:9092
Event parsing and validation rules configured.
StreamForge is running. Waiting for Kafka events...
```

When windows complete, the job logs messages indicating that aggregation rows were upserted into PostgreSQL. Press `Ctrl+C` to stop following logs; this does **not** stop the Spark container.

### 4. Generate events

In a second PowerShell terminal, activate the virtual environment, move to the project directory, and run:

```powershell
.\.venv\Scripts\Activate.ps1
python src/producer.py --events 20 --interval 0.5
```

The producer sends 20 synthetic events at half-second intervals. You can change these arguments to generate a different test volume.

### 5. Launch the dashboard

The local PostgreSQL container is published on host port **5433** in this setup because port 5432 was already occupied by a native Windows PostgreSQL process. The container-to-container PostgreSQL address remains `postgres:5432`.

```powershell
$env:DATABASE_URL = "postgresql+psycopg2://streamforge:streamforge_dev@127.0.0.1:5433/streamforge"
python -m streamlit run dashboard.py
```

Open the local Streamlit URL printed in the terminal, normally `http://localhost:8501`.

## Event model

The producer emits JSON events with fields defined in `event_schema.json`. The schema includes fields such as:

- `event_id`, `event_type`, `event_time`
- `user_id`, `product_id`, `category`, `region`
- `quantity`, `unit_price`, `amount`, `currency`
- `payment_method`, `order_id`, `failure_reason`, `inventory_delta`

Event types currently generated:

| Event type | Meaning |
|---|---|
| `product_view` | A product page was viewed |
| `add_to_cart` | A product was added to a cart |
| `purchase` | A purchase event occurred |
| `payment_failed` | A payment attempt failed |
| `inventory_update` | An inventory change occurred |

## Validation and aggregation

The streaming job validates common event fields, including non-empty event IDs, supported event types, non-empty regions, and parseable event timestamps. Purchase events must also have a non-null, non-NaN, non-negative amount.

Valid events are aggregated by:

- Event-time window (10 seconds)
- Event type
- Region

The aggregate includes `event_count` and `gross_sales`. Gross sales are summed for purchase events; other event types contribute zero.

## PostgreSQL output

The dashboard reads from `streamforge_event_metrics`, which stores:

| Column | Description |
|---|---|
| `window_start` | Start of the event-time window |
| `window_end` | End of the event-time window |
| `event_type` | Event category |
| `region` | Region associated with the events |
| `event_count` | Number of events in the group |
| `gross_sales` | Purchase amount summed for the group |

The table uses a unique key across window start, window end, event type, and region. The Spark sink stages micro-batch results and performs an upsert into the target table.

## Dashboard

The Streamlit dashboard includes:

- Total validated event count
- Gross sales in INR
- Active regions
- Purchase share
- Event volume by type
- Regional activity
- Gross sales by region
- Events per aggregation window
- Latest aggregate rows
- PostgreSQL and Kafka connectivity checks

The Spark freshness indicator is inferred from the timestamp of the latest aggregate window. It is not a direct Spark health endpoint. A quiet or idle producer can therefore cause the indicator to show historical output even when the Spark process itself is still running.

Rows from regions whose names begin with `QA_` are excluded from headline business metrics when other business-region data is available.

## Design decisions and trade-offs

- **Kafka decouples production from processing.** The producer can publish events independently of the Spark consumer.
- **Event-time windows and watermarking** allow aggregation to follow event timestamps and bound late-event state.
- **Validation before aggregation** prevents malformed events and invalid purchase amounts from contributing to business metrics.
- **PostgreSQL provides queryable aggregate storage** for the dashboard without requiring the dashboard to consume Kafka directly.
- **Micro-batch staging plus upsert** makes aggregate writes repeatable at the database-key level. This implementation should not be described as end-to-end exactly-once processing.
- **Synthetic event generation** makes the project reproducible without exposing customer or payment data.

## Current limitations

- The event stream is synthetic and does not represent real customer behavior.
- Kafka is configured as a single-node local development broker; replication factor 1 is not highly available.
- The Spark job is a local-mode container deployment, not a multi-node production cluster.
- The dashboard infers Spark freshness from recent output; it does not query Spark's monitoring API.
- The current sink is a practical micro-batch staging/upsert implementation, not a formal exactly-once guarantee across Kafka, Spark, and PostgreSQL.
- No authentication, authorization, secrets manager, or production deployment hardening is included.

## Safe shutdown

Stop the Compose services without deleting persistent volumes:

```powershell
docker compose -f .\docker-compose.yml -f .\docker-compose-spark-addition.yml down
```

Do **not** add `-v` unless you intentionally want to delete the persistent PostgreSQL data and other named volumes.

## Resume-ready project description

**StreamForge — Real-Time E-commerce Event Analytics | Python, Kafka, Spark Structured Streaming, PostgreSQL, Docker, Streamlit**

- Built a containerized event pipeline that publishes synthetic e-commerce events to Kafka and processes them using Spark Structured Streaming.
- Implemented event parsing, validation, event-time windowing, and regional/event-type aggregations with a watermark.
- Persisted micro-batch aggregates to PostgreSQL using staging tables and conflict-aware upserts.
- Developed a Streamlit analytics dashboard for event volume, regional activity, gross sales, and aggregation-window trends.

Use these bullets only while the described implementation remains present in the repository. Do not claim production scale, exactly-once delivery, high availability, or real customer data.

## Dashboard screenshots

These captures show the local dashboard after synthetic events were produced, streamed, aggregated, and stored.

<details>
<summary>View dashboard screenshots</summary>

![Dashboard capture 1](ss/Screenshot%202026-11-06%20201825.png)

![Dashboard capture 2](ss/Screenshot%202026-11-06%20201836.png)

![Dashboard capture 3](ss/Screenshot%202026-11-06%20201851.png)

![Dashboard capture 4](ss/Screenshot%202026-11-06%20201858.png)

![Dashboard capture 5](ss/Screenshot%202026-11-06%20201910.png)

</details>

The editable Mermaid architecture source is available in [architecture.mmd](architecture.mmd).

