"""
Tests unitarios para S3CircuitBreaker y su exposición en /health.
"""
import time
import threading
import pytest
from unittest.mock import patch

from s3_recover import S3CircuitBreaker, _s3_circuit_breaker
import main


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def cb():
    """Circuit breaker fresco para cada prueba (no comparte estado con el singleton)."""
    return S3CircuitBreaker(failure_threshold=3, recovery_timeout=1)


# ---------------------------------------------------------------------------
# Estado inicial
# ---------------------------------------------------------------------------

def test_initial_state_is_closed(cb):
    assert cb.state == "closed"
    assert cb._failures == 0
    assert not cb.is_open


# ---------------------------------------------------------------------------
# Transición closed → open
# ---------------------------------------------------------------------------

def test_opens_after_threshold(cb):
    for _ in range(cb.failure_threshold):
        cb.record_failure()
    assert cb.state == "open"
    assert cb.is_open


def test_does_not_open_before_threshold(cb):
    for _ in range(cb.failure_threshold - 1):
        cb.record_failure()
    assert cb.state == "closed"
    assert not cb.is_open


# ---------------------------------------------------------------------------
# Estado open rechaza llamadas
# ---------------------------------------------------------------------------

def test_is_open_returns_true_while_open(cb):
    for _ in range(cb.failure_threshold):
        cb.record_failure()
    assert cb.is_open is True


# ---------------------------------------------------------------------------
# Transición open → half-open tras recovery_timeout
# ---------------------------------------------------------------------------

def test_transitions_to_half_open_after_timeout(cb):
    for _ in range(cb.failure_threshold):
        cb.record_failure()
    assert cb.state == "open"

    # Esperar a que expire el recovery_timeout (1 s en el fixture)
    time.sleep(cb.recovery_timeout + 0.05)
    assert cb.state == "half-open"
    assert not cb.is_open  # half-open no bloquea llamadas


# ---------------------------------------------------------------------------
# Transición half-open → closed (éxito) y half-open → open (fallo)
# ---------------------------------------------------------------------------

def test_half_open_closes_on_success(cb):
    for _ in range(cb.failure_threshold):
        cb.record_failure()
    time.sleep(cb.recovery_timeout + 0.05)
    assert cb.state == "half-open"

    cb.record_success()
    assert cb.state == "closed"
    assert cb._failures == 0


def test_half_open_reopens_on_failure(cb):
    for _ in range(cb.failure_threshold):
        cb.record_failure()
    time.sleep(cb.recovery_timeout + 0.05)
    assert cb.state == "half-open"

    # Un fallo adicional vuelve a cerrar el circuito SÓLO si supera el threshold.
    # Dado que _failures ya estaba en threshold, record_failure vuelve a abrir.
    cb.record_failure()
    assert cb.state == "open"


# ---------------------------------------------------------------------------
# record_success resetea el contador
# ---------------------------------------------------------------------------

def test_success_resets_failure_count(cb):
    cb.record_failure()
    cb.record_failure()
    cb.record_success()
    assert cb._failures == 0
    assert cb.state == "closed"


# ---------------------------------------------------------------------------
# Thread-safety: múltiples threads escribiendo concurrentemente
# ---------------------------------------------------------------------------

def test_thread_safety_concurrent_failures():
    """
    Varios threads registran fallos simultáneamente.
    El estado final debe ser 'open' y _failures >= failure_threshold.
    """
    cb = S3CircuitBreaker(failure_threshold=10, recovery_timeout=60)
    errors = []

    def worker():
        try:
            for _ in range(5):
                cb.record_failure()
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Excepción en thread: {errors}"
    assert cb._failures >= cb.failure_threshold
    assert cb.state == "open"


# ---------------------------------------------------------------------------
# __repr__
# ---------------------------------------------------------------------------

def test_repr_includes_state_and_failures(cb):
    r = repr(cb)
    assert "closed" in r
    assert "0" in r


# ---------------------------------------------------------------------------
# /health expone el circuit breaker
# ---------------------------------------------------------------------------

from fastapi.testclient import TestClient
from database import ConsultasDatabase
from background_simulator import BackgroundSimulator
import os
import shutil

TEST_DB_PATH_CB = "test_cb_health.db"
TEST_DOWNLOAD_PATH_CB = "./test_cb_downloads"


@pytest.fixture
def client_with_db(monkeypatch, tmp_path):
    test_db = ConsultasDatabase(db_path=TEST_DB_PATH_CB)
    os.makedirs(TEST_DOWNLOAD_PATH_CB, exist_ok=True)
    monkeypatch.setattr(main, "db", test_db)
    monkeypatch.setattr(main, "recover", BackgroundSimulator(test_db))
    monkeypatch.setattr(main, "DOWNLOAD_PATH", TEST_DOWNLOAD_PATH_CB)
    monkeypatch.setattr(main, "SOURCE_DATA_PATH", str(tmp_path))  # existe en disco
    yield TestClient(main.app)
    if os.path.exists(TEST_DB_PATH_CB):
        os.remove(TEST_DB_PATH_CB)
    if os.path.exists(TEST_DOWNLOAD_PATH_CB):
        shutil.rmtree(TEST_DOWNLOAD_PATH_CB)


def test_health_includes_circuit_breaker_field(client_with_db):
    response = client_with_db.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "s3_circuit_breaker" in data
    cb_info = data["s3_circuit_breaker"]
    assert "state" in cb_info
    assert "failures" in cb_info
    assert "failure_threshold" in cb_info
    assert "recovery_timeout_s" in cb_info
    assert cb_info["state"] in ("closed", "open", "half-open")


def test_health_circuit_breaker_state_reflects_singleton(client_with_db, monkeypatch):
    """El campo state en /health refleja el estado real del singleton."""
    # Parchear el singleton para que aparezca como open
    monkeypatch.setattr(_s3_circuit_breaker, "_state", "open")
    monkeypatch.setattr(_s3_circuit_breaker, "_opened_at", time.monotonic())

    response = client_with_db.get("/health")
    data = response.json()
    assert data["s3_circuit_breaker"]["state"] == "open"

    # Restaurar
    monkeypatch.setattr(_s3_circuit_breaker, "_state", "closed")
    monkeypatch.setattr(_s3_circuit_breaker, "_failures", 0)
