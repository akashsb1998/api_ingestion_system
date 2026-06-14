# High-Throughput API Ingestion System
### Complete Code + Documentation — R3 System Design Reference

---

## What this system does

Pulls data from **50 different REST APIs** reliably and lands it in **S3**, with:
- Rate limiting per API (token bucket via Redis)
- Pagination handling (next_page_token loop)
- Checkpoint / resume (no duplicate or missing records on failure)
- Automatic retry via SQS
- Dead letter queue for permanently failed jobs

---

## Architecture in one picture

```
Airflow (scheduler)
    │
    │  pushes 50 job messages
    ▼
SQS Queue  ←─── holds the "to-do list"
    │
    │  triggers automatically
    ▼
Lambda Workers (one per job, run in parallel)
    │
    ├── checks Redis token bucket  ←── shared rate limiter
    ├── calls REST API (paginated)
    ├── writes pages to S3
    └── saves checkpoint to DynamoDB
    │
    ▼
S3 raw-landing/
    source=fx_rates/date=2024-01-15/run_id=abc/page=0001.json
    source=trade_data/date=2024-01-15/run_id=xyz/page=0001.json
    ...
```

---

## Key concepts explained

### 1. Why SQS?
Airflow workers are limited and expensive. If Airflow calls APIs directly,
a slow API blocks the worker for the entire duration of the HTTP call.

With SQS: Airflow posts 50 messages in ~1 second and is done.
Lambda workers handle the actual HTTP work independently.

**Airflow = waiter taking orders**
**SQS = order tickets on the kitchen rail**
**Lambda = chefs cooking the food**

### 2. Why Redis for rate limiting?
Each Lambda is a separate process on a separate machine.
Without Redis, each Lambda has its own token counter.
5 Lambdas each think they have 10 tokens = 50 requests to a 10/min API.

With Redis: ONE shared counter. All Lambdas read the same number.
Total requests across all workers never exceeds the limit.

**Redis = shared scoreboard everyone can see**

### 3. How next_page_token works
The API returns 100 records + a token string (e.g. "eyJwYWdlIjoyf").
You send this token back in the next request to get the next 100 records.
When the token is null, you're done — no more pages.

```
Request 1: GET /rates              → 100 records + token="eyJ..."
Request 2: GET /rates?page_token=eyJ...  → 100 records + token="mNp..."
Request 3: GET /rates?page_token=mNp...  → 100 records + token=null (done)
```

### 4. Why DynamoDB for checkpoints?
After fetching all pages, Lambda saves the last record's timestamp
to DynamoDB. Next run reads this and passes it to the API as
"give me everything since this timestamp".

This is the same as the watermark table pattern used in Snowflake/MWAA
on the Convera project — just on AWS.

---

## File structure

```
api_ingestion_system/
├── config/
│   └── api_configs.yaml          ← API list with rate limits + schedules
├── src/
│   ├── airflow/
│   │   └── airflow_dispatcher_dag.py  ← Airflow DAG (dispatch only)
│   ├── lambda/
│   │   └── lambda_handler.py     ← Core Lambda: fetch + rate limit + write
│   ├── redis/
│   │   └── token_bucket.py       ← Token bucket implementation
│   └── checkpoint/
│       └── checkpoint_manager.py ← DynamoDB read/write for cursors
├── examples/
│   └── local_simulation.py       ← Run locally without AWS
└── docs/
    └── README.md                 ← This file
```

---

## Running locally (no AWS needed)

```bash
# Run the simulation — shows 3 Lambdas with rate limiting
python examples/local_simulation.py
```

You'll see:
- API1 (rpm=3) waiting at the token bucket
- API3 (rpm=30) finishing first
- Checkpoint saves after each API completes
- Zero 429 errors

---

## AWS setup (production)

### Infrastructure needed
| Service | Purpose |
|---|---|
| SQS FIFO Queue | Job message buffer |
| Lambda | API fetch workers |
| ElastiCache (Redis) | Shared token buckets |
| DynamoDB | Checkpoint storage |
| S3 | Raw data landing zone |
| Secrets Manager | API credentials |
| CloudWatch | Alerts + logs |

### Key Lambda environment variables
```
REDIS_HOST=your-elasticache.cache.amazonaws.com
CHECKPOINT_TABLE=api_checkpoints
S3_BUCKET=raw-landing
AWS_REGION=ap-south-1
```

---

## Failure scenarios

| Failure | What happens |
|---|---|
| Lambda crashes mid-fetch | SQS visibility timeout expires → message reappears → new Lambda retries from last checkpoint |
| API returns 429 | Read Retry-After header → sleep → retry (token bucket prevents most 429s) |
| S3 write fails | Exception raised → Lambda fails → SQS retries → checkpoint not updated so no data loss |
| 3 retries all fail | Message moves to Dead Letter Queue → CloudWatch alert fires |
| API is down for hours | Messages stay in SQS (up to 14 days) → auto-processed when API recovers |

---

## Rate limit cheat sheet

| API | Limit | Tokens in Redis | Effect |
|---|---|---|---|
| API1 FX Rates | 10 rpm | 10 tokens, refill/min | Workers often wait |
| API2 Trade Data | 60 rpm | 60 tokens, refill/min | Occasional wait |
| API3 Ref Data | 600 rpm | 600 tokens, refill/min | Almost never waits |

---

## Interview answer template

> "We use a four-component design: Airflow dispatches 50 job messages
> to SQS in under a second and exits. Lambda workers auto-scale on
> queue depth and handle all HTTP work. A Redis token bucket shared
> across all workers enforces per-API rate limits before each request —
> preventing 429s before they happen rather than handling them after.
> Checkpoints in DynamoDB make every run idempotent — a Lambda crash
> always resumes from where it left off, never duplicating or losing records."
