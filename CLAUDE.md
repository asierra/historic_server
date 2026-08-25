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
`main.py` (FastAPI routes) → `_validate_and_prepare_request()` (Pydantic + per-satellite business rules + disk-space/file-count limits) → `processors.py` (`HistoricQueryProcessor.procesar_request`, expands date ranges/hours into an internal query object) → `database.py` (`ConsultasDatabase`, SQLite persistence) → background task (`recover.procesar_consulta`, either the real recoverer or the simulator) → polled via `GET /query/{id}`.

`consulta_id` is an 8-char random ID (`generar_id_consulta()`, `secrets.choice`), used as the SQLite PK — the same ID the caller (`historic_query`) generates/tracks on its side, sent as `id` in the request body or auto-generated here if absent.

### Query states and the in-flight lock
`estado` is this server's own vocabulary — `recibido` (accepted and queued) → `procesando` (pipeline
working) → `completado` / `error`. **These are not `historic_query`'s states**: Django never stores
`recibido`/`procesando`, it collapses both into its own `en_proceso` via `translate_api_estado()`.
Both map to HTTP 202 + `Retry-After` in `GET /query/{id}`, so they're indistinguishable to callers.

Two module-level tuples in `main.py` are the single source of truth — use them instead of hardcoding
state lists: `ESTADOS_EN_VUELO` (`recibido`, `procesando`) and `ESTADOS_REINICIABLES` (those plus
`error`, `completado`).

**Background tasks live in memory.** FastAPI's `BackgroundTasks` are lost whenever the process dies,
so a query stays frozen in whatever state it had, forever — there is no startup rescue. This is not
hypothetical: `GkpH6xne` sat at 89% for 29h in Aug-2026 because `historic-server` was restarted
mid-download. Two consequences are wired into the code and should not be "simplified" away:
- `POST /query/{id}/restart` accepts `recibido` precisely because that's where a never-started task
  lands. It's the endpoint's whole reason to exist.
- `DELETE /query/{id}?purge=true` returns **409** for anything in `ESTADOS_EN_VUELO` unless
  `force=true`, since the task may start at any moment and recreate the directory just deleted.
  `historic_query`'s admin reject button always sends `force=true` for this reason.

`gunicorn -w 4` means workers share no memory, so two restart requests land on different processes
and each queues its own task — this actually happened, with two workers downloading the same 2380
files in parallel. `database.reclamar_para_reproceso()` is the lock: one conditional `UPDATE` that
both resets the state and claims the query, so only one caller can see `rowcount == 1`. The lock
signal is `timestamp_actualizacion`, which the pipeline already refreshes on every advance — a
recent heartbeat means someone is really working, a stale one (older than `LATIDO_MAXIMO_S`, 900s)
means the task died. `error`/`completado` are always claimable; they have no work in flight.

### Open work
Tracked in `../historic_query/pendientes.md` §0-ter (the operational log lives in that repo).

For this one, the big one is **`PLAN_COLA_DURABLE.md`**: turning `consultas` into a lease-based
queue so orphaned work resumes on its own. **Entrega 1 is done** — the schema (four columns + an
index) and the primitives in `ConsultasDatabase` (`reclamar_siguiente`, `liberar_expiradas`,
`fallar_con_reintento`, `reencolar`), covered by `tests/test_cola.py`. Nothing calls them yet, so
runtime behavior is unchanged and it can ship on its own. Entrega 2 is the actual cut, and it was
**deferred on 2026-08-25**: the one-service-or-two decision in §6 needs two numbers from tahan
(restart frequency and real query durations) that aren't measurable from a dev box. The standing
recommendation there is one service. Entrega 1 is identical either way, so nothing is blocked.

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
