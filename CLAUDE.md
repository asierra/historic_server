# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

FastAPI service ("QueryProcessor" in the wider LANOT architecture) that recovers historical GOES satellite files from LANOT's mass storage. It receives a query (satellite/level/bands/products/date-time ranges), validates it, queues it for background processing, and exposes polling endpoints for status/results. Its primary (currently only) client is the Django app in the sibling repo `historic_query`, which owns the end-to-end request state machine and treats this service as one step in its pipeline — see `historic_query_schema.json` for the contract shape. `README.md` documents the full HTTP API (request/response examples, error codes, all env vars) in detail — prefer it over re-deriving endpoint behavior from code.

## Commands

```bash
# Setup
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env

# Run (dev)
python main.py                      # uvicorn on 0.0.0.0:9041
# Run (prod-style, matches server.sh / historic-server.service)
gunicorn -w 4 -k uvicorn.workers.UvicornWorker main:app --bind 0.0.0.0:9041
./server.sh {start|stop|restart|status}   # kills existing gunicorn/uvicorn first, then backgrounds via nohup

# Tests
pytest                               # all tests (PROCESSOR_MODE forced to 'simulador' via pytest.ini)
pytest -m "not real_io"              # skip tests that hit real disk/network
pytest tests/test_api.py             # one module
pytest tests/test_circuit_breaker.py::test_name  # one test

# DB migration / inspection (standalone scripts, not part of the app process)
python migrate_db.py [path_to_db]    # idempotent schema migration, backs up the .db file first
python tools/get_query.py <consulta_id> [--db PATH] [--original]
python tools/diff_queries.py --base A.json --excluir B.json --out C.json
```

CI (`.github/workflows/ci.yml`) runs `pytest -q -m "not real_io"` — the same command you should run locally before pushing. It selects by marker on purpose: the previous version named files at the repo root, which stopped existing when the suite moved into `tests/` in `2331cc6` (2025-10-31), so CI failed on every push for 25 commits without anyone reading it. Don't reintroduce a file list. The suite needs no real storage (fixtures monkeypatch `SOURCE_PATH`/`DOWNLOAD_PATH`), so it passes on a bare runner.

There's no linter configured. `SECURITY.md` / `PRE_DEPLOYMENT_CHECKLIST.md` / `DEPLOYMENT_GUIDE.md` cover pre-deploy steps — nothing is enforced in CI beyond tests.

## Architecture

### Request lifecycle
`main.py` (FastAPI routes) → `_validate_and_prepare_request()` (Pydantic + per-satellite business rules + disk-space/file-count limits) → `processors.py` (`HistoricQueryProcessor.procesar_request`, expands date ranges/hours into an internal query object) → `database.py` (`ConsultasDatabase`, SQLite persistence) → queued in `recibido` and picked up by `cola.BucleDeCola` (which calls `recover.procesar_consulta`, either the real recoverer or the simulator) → polled via `GET /query/{id}`.

`consulta_id` is an 8-char random ID (`generar_id_consulta()`, `secrets.choice`), used as the SQLite PK — the same ID the caller (`historic_query`) generates/tracks on its side, sent as `id` in the request body or auto-generated here if absent.

### Query states and the in-flight lock
`estado` is this server's own vocabulary — `recibido` (accepted and queued) → `procesando` (pipeline
working) → `completado` / `error`. **These are not `historic_query`'s states**: Django never stores
`recibido`/`procesando`, it collapses both into its own `en_proceso` via `translate_api_estado()`.
Both map to HTTP 202 + `Retry-After` in `GET /query/{id}`, so they're indistinguishable to callers.

Two module-level tuples in `main.py` are the single source of truth — use them instead of hardcoding
state lists: `ESTADOS_EN_VUELO` (`recibido`, `procesando`) and `ESTADOS_REINICIABLES` (those plus
`error`, `completado`).

**The queue owns the work, not the process.** Endpoints no longer start anything: `POST /query`
just INSERTs, and `cola.BucleDeCola` — a thread started from `lifespan`, one per gunicorn worker —
claims rows and runs the pipeline. This replaced `BackgroundTasks`, which lived in process memory
and were lost on every restart, freezing queries forever (`GkpH6xne` sat at 89% for 29h in
Aug-2026 for exactly that reason). See `PLAN_COLA_DURABLE.md`.

Ownership is written in the row: `worker_id` plus `lease_hasta`, refreshed by `actualizar_estado`
on every pipeline advance. A dead process stops refreshing, the lease expires, and any loop's
`liberar_expiradas()` returns the query to `recibido`. **Recovery is the normal operation of the
queue, not a special startup case** — so don't add a startup rescue, it already happens.

Three things follow, and should not be "simplified" away:
- `reclamar_siguiente()` is a single conditional `UPDATE ... RETURNING`, which is what makes it
  safe for four gunicorn workers to each run a loop. Two of them once downloaded the same 2380
  files in parallel; that's what this prevents. Covered by
  `tests/test_cola.py::test_varios_workers_a_la_vez_no_se_pisan`.
- `POST /query/{id}/restart` calls `reencolar()`, which **refuses (409) if the lease is live**.
  Re-queueing a query someone is actively downloading would hand it to a second consumer writing
  into the same directory. Two rapid restarts both return 202 now — that's fine and idempotent;
  exclusion happens at the claim.
- `DELETE /query/{id}?purge=true` returns **409** for anything in `ESTADOS_EN_VUELO` unless
  `force=true`: a loop may claim a `recibido` query at any moment and recreate the directory just
  deleted. `historic_query`'s admin reject button always sends `force=true` for this reason.

The loop translates the pipeline's own `error` state into a retry decision (`fallar_con_reintento`,
3 attempts, 1/5/15 min backoff via `disponible_desde`) — `recover.procesar_consulta` catches its
own exceptions, so without that translation there would be no retries at all. On shutdown the loop
deliberately does **not** release an in-progress query: it lets the lease expire, because releasing
it while the pipeline is still writing invites a second writer.

`/health` reports `cola.hilo_vivo` and the queue depth, and returns 503 if the thread is dead with
work queued. That is the one failure mode invisible from outside: the API keeps answering 202 while
nothing drains.

### Open work
Tracked in `../historic_query/pendientes.md` §0-ter (the operational log lives in that repo).

For this one, the big one is **`PLAN_COLA_DURABLE.md`**: turning `consultas` into a lease-based
queue so orphaned work resumes on its own. **Entrega 1 is done** — the schema (four columns + an
index) and the primitives in `ConsultasDatabase` (`reclamar_siguiente`, `liberar_expiradas`,
`fallar_con_reintento`, `reencolar`), covered by `tests/test_cola.py`. Nothing calls them yet, so
runtime behavior is unchanged and it can ship on its own.

Entrega 2 is the actual cut and is still pending, but its shape is settled: **§6 was decided on
2026-08-25 in favour of a single service** — the queue loop runs in a thread off the API's own
`lifespan`, no `worker.py` and no new systemd unit. Decided on tahan's numbers (4 restarts in 6
months, one of which is what froze `GkpH6xne`): splitting the processes would buy ~30 min a year
against a new unit whose silent-failure mode has a precedent on that same machine. §6 keeps the
reasoning and what would reverse it.

That plan supersedes the old "startup rescue in `lifespan`" idea, which it lists as a discarded
alternative (§10): it re-queues orphans but leaves intact the thing that causes them, namely that
deploying the API kills in-flight downloads.

Also open: no test coverage for the 413 quota rejection.

### Storage backend (the pending sqlite→postgres migration)
`database.py`'s `ConsultasDatabase` is a hand-written SQLite wrapper (no ORM) using **WAL journal mode** for reader/writer concurrency, with a single `consultas` table (id, estado, query JSON blob, resultados JSON blob, progreso, mensaje, timestamps, usuario, plus the queue columns `intentos`/`lease_hasta`/`worker_id`/`disponible_desde` — declared once in `database.py`'s `COLUMNAS_COLA` and reused by `migrate_db.py` so the two paths can't drift). All queries are raw `sqlite3` calls with manual `try/except` + logging per method — there's no connection pooling or migrations framework; `migrate_db.py` is a standalone imperative script that inspects `PRAGMA table_info` and `ALTER TABLE`s as needed, run manually (not on app startup). A move to Postgres would need to replace this whole module (and `migrate_db.py`) since nothing here is DB-agnostic (raw SQL strings, sqlite-specific `PRAGMA` calls, file-path-based `DB_PATH` config). Given the low write/read volume (single client today), this has been deprioritized — revisit if concurrent multi-client access becomes real.

### Dual storage + background recovery (`recover.py`, `s3_recover.py`, `background_simulator.py`)
- `recover.py` — `RecoverFiles` orchestrates: `LustreRecoverFiles` for the primary POSIX filesystem (`SOURCE_PATH`, organized `sensor/nivel/dominio/año/semana/*.tgz`), extracting/copying via a `pebble.ProcessPool` (`MAX_WORKERS`) with per-file timeouts (`FILE_PROCESSING_TIMEOUT_SECONDS`) to avoid zombie processes. Files not found locally fall through to S3.
- `s3_recover.py` — S3 (NOAA GOES public bucket) fallback: listing, filtering by hour/minute window, retry-with-backoff downloads, and a **circuit breaker** (`_s3_circuit_breaker`, module-level singleton) that opens after 5 consecutive failures and stays open for 60s, exposed via `GET /health`.
- `background_simulator.py` — `BackgroundSimulator`, a drop-in replacement for `RecoverFiles` used when `PROCESSOR_MODE=simulador` (forced in tests via `pytest.ini`); simulates success/failure at configurable rates (`SIM_LOCAL_SUCCESS_RATE`, `SIM_S3_SUCCESS_RATE`) without touching real Lustre/S3.
- Both real and simulated paths write progress/results back through the same `ConsultasDatabase` API, so `main.py` and the polling contract are agnostic to which one is active.

### Satellite configuration (`config_base.py`, `config.py`)
`SatelliteConfigBase` (ABC) defines the contract (valid satellites/sensors/levels/domains/products/bands, defaults, file-count/size estimation). `config.py`'s `SatelliteConfigGOES` is the only concrete implementation currently registered in `main.py`'s `AVAILABLE_SATELLITE_CONFIGS`. Adding a new satellite means implementing this ABC and registering it there — validation, defaults, and file estimation all flow through this class, not scattered checks in `main.py`.

### Validation rule worth knowing
Band requirements depend on level/product (`main.py:_validate_and_prepare_request`): L1b always requires bands; L2 requires bands only for CMI/CMIP*-family products or `productos=["ALL"]` — other L2 products (e.g. `ACHA`) ignore/reject bands. This logic is duplicated conceptually between `main.py` (request shaping) and `config.py` (`validate_bandas`) — check both when changing band rules.

### Logging
`structlog` via `logging_config.py`: human-readable console output in an interactive terminal, JSON in production (systemd/containers). `consulta_id` and `request_id` (from the `X-Request-ID` header, bound in the `correlation_id_middleware` in `main.py`) are bound into `structlog.contextvars` so every log line during a request/query's lifecycle carries them automatically — don't manually thread IDs through function signatures for logging purposes, use `structlog.get_logger(__name__)` and bind/log with `key=value` pairs (see README's "Sistema de Logging" section for the exact convention, including why f-strings in log messages are discouraged here).

### Filesystem layout in production
Code, DB, and downloads are intentionally on separate paths (`/opt/historic_server` code, `/var/lib/historic_server` DB, `/data/historic_downloads` or similar for per-query working dirs) — see `FILESYSTEM_LAYOUT.md` for the full rationale and backup strategy (`sqlite3 .backup` + gzip + 30-day retention cron). `DOWNLOAD_PATH/{consulta_id}` is where each query's recovered files land; `DELETE /query/{id}?purge=true` removes it (path-traversal-guarded in `main.py`; needs `force=true` if the query still has work in flight — see the states section).
