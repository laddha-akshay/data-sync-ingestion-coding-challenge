# DataSync Ingestion

## How to run
```bash
sh run-ingestion.sh
```

Docker and Docker Compose are the only requirements. The script builds the image, starts Postgres, and monitors progress automatically.

## Architecture

Two coroutines run concurrently inside a single container:

- **Fetcher** — walks the cursor chain one page at a time, enforcing a minimum delay between requests to stay within rate limits
- **Writer** — drains a write queue using PostgreSQL `COPY FROM STDIN` for fast bulk inserts, falling back to chunked `INSERT` if needed
```
API ──→ [fetcher] ──→ writeQueue ──→ [writer] ──→ PostgreSQL
            ↑                                          ↓
         nextCursor                            ingestion_state
                                               (cursor saved every 5k)
```

They run in parallel so DB writes overlap with the next API fetch. Only one fetch is in flight at a time — this was a deliberate choice after discovering that throwing more concurrent workers at the API just meant more workers sleeping through the same rate limit window simultaneously.

## Resumability

Cursor position is persisted to the `ingestion_state` table every 5,000 events. On restart, the fetcher picks up from the last saved cursor. If that cursor has gone stale (API returns 400), it resets to the beginning — some events get re-fetched but `ON CONFLICT DO NOTHING` handles deduplication in Postgres.

## API discoveries

- Response shape is `{ data: [], hasMore: boolean, nextCursor: string }` — not nested under `pagination` as I initially assumed
- Cursors have a TTL — if the process stalls long enough the cursor expires and returns 400
- Rate limit headers: `x-ratelimit-remaining` and `retry-after`
- The `retry-after` value varies (1s to 21s) — respecting it exactly plus a fixed 500ms inter-request delay keeps things stable
- Page size of 1000 was the sweet spot
- Explored `/events/export`, `/events/bulk`, `/events/stream` — all share the same rate limit bucket

## Throughput

~2 req/s sustained with `MIN_DELAY_MS=500`. At 1000 events/page that's roughly 2,000 events/sec, or about 25 minutes for 3M events. Higher concurrency triggers rate limiting that ends up being slower overall.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `PAGE_SIZE` | `1000` | Events per API page |
| `MIN_DELAY_MS` | `500` | Minimum ms between requests |

## What I'd improve with more time

- Dynamic throttling based on `x-ratelimit-remaining` instead of a fixed delay
- Investigate date-range or offset params to enable parallel independent cursor chains
- Integration tests with a mock API server that simulates pagination, rate limits, and stale cursors
- Metrics endpoint to monitor events/sec and queue depth without tailing logs

## Tools used

Claude (Anthropic) was used to help write and debug parts of this solution.