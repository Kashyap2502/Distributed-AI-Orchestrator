# AI Orchestrator

Go control plane and Python worker starter for an asynchronous AI task orchestration service.

## Current Vertical Slice

The service accepts a prompt, stores a run and task in PostgreSQL, writes a transactional outbox event, and returns `202 Accepted`. A background publisher sends durable messages to RabbitMQ. The Python worker consumes them and reports a placeholder result to the Go API. Clients poll the run by ID.

This is an infrastructure MVP, not yet a production AI service: the worker does not call an LLM, the planner does not decompose prompts, only the `logical` queue exists, and gRPC is not wired. The internal worker callback currently uses HTTP. These are the next milestones after proving the durable asynchronous path.

## Run Locally

Prerequisites: Docker Desktop with its Linux container engine running, Go 1.25 or newer for local Go commands, and Python 3.12 or newer if running the worker outside Compose.

1. Copy `.env.example` to `.env` and replace `INTERNAL_TOKEN` with a long random value.
2. Start from the repository root:

   ```powershell
   docker compose up --build
   ```

3. Check API health:

   ```powershell
   Invoke-RestMethod http://localhost:8080/healthz
   ```

4. Submit a prompt using a UUID for the temporary user identifier:

   ```powershell
   $body = @{
     user_id = '11111111-1111-4111-8111-111111111111'
     prompt = 'Explain why this test is failing'
   } | ConvertTo-Json
   $run = Invoke-RestMethod -Method Post -Uri http://localhost:8080/v1/runs -ContentType 'application/json' -Body $body
   $run
   ```

5. Poll for the result:

   ```powershell
   Invoke-RestMethod "http://localhost:8080/v1/runs/$($run.run_id)"
   ```

The worker result is a placeholder message. Stop the stack with `Ctrl+C`. PostgreSQL data is stored in the `postgres_data` volume. The initial migration runs only when that volume is created; use a migration tool for future schema changes rather than deleting production data.

## API

- `GET /healthz`: process liveness.
- `GET /readyz`: PostgreSQL readiness.
- `POST /v1/runs`: accepts `{ "user_id": "<uuid>", "prompt": "..." }`; responds with run/task IDs and `queued` status.
- `GET /v1/runs/{run_id}`: returns current status and result.
- `POST /internal/tasks/{task_id}/result`: worker-only completion callback, protected by `INTERNAL_TOKEN`.

Prompts are limited to 20,000 characters and request bodies to 64 KiB. Database operations have deadlines. The API does not wait for model execution or a worker before returning the run ID.

## Architecture And Reliability

- Gin and Go own request handling, workflow records, outbox publishing, and result persistence.
- PostgreSQL is the source of truth; task creation and its outbox event commit in one transaction.
- RabbitMQ carries durable messages. Publisher confirms are awaited before marking outbox events published. A crash between broker confirmation and database commit can still duplicate delivery, so consumers and callbacks must remain idempotent.
- The logical queue is configured as a quorum queue, with a two-second delayed retry queue and a dead-letter queue after three callback retries.
- Python workers use prefetch `8`; add worker replicas independently as measured demand grows.
- `DB_MAX_CONNS` tunes each Go API instance's PostgreSQL pool (default `50`). Account for the combined pool size across all replicas and the database connection limit.

Compose is single-node development infrastructure. It does not provide PostgreSQL failover or RabbitMQ quorum replication across nodes. Production needs a multi-node RabbitMQ cluster, managed/replicated PostgreSQL, TLS, secrets management, metrics/tracing, and backup/restore tests.

## Security And Capacity Status

Compose binds the API only to `127.0.0.1`. There is no end-user authentication or authorization yet, and `user_id` is currently caller-supplied; do not expose this API to a network or treat that value as identity. Add verified authentication and tenant-scoped queries before deployment. Set a unique `INTERNAL_TOKEN` even for shared development environments.

The async boundary keeps worker/model time off the HTTP request path, but this starter has not been load-tested and makes no claim of supporting 1,000 simultaneous requests. Capacity depends on request rate, database latency, model quotas, queue depth, and worker throughput. Add queue-depth admission limits, observability, and a representative load test before setting a concurrency or latency target.

## Local Checks

```powershell
go test ./...
go vet ./...
```

Docker integration checks require Docker Desktop to be running.
