import os
import time
import pytest
import shutil
from fastapi.testclient import TestClient
import main
from background_simulator import BackgroundSimulator
from database import ConsultasDatabase
from settings import settings

TEST_DB_PATH = "test_consultas_sources.db"
TEST_DOWNLOAD_PATH = "./test_downloads_sources"

@pytest.fixture(autouse=True)
def override_db_for_tests(monkeypatch):
    """Parchea la DB y el recover con el simulador para estas pruebas."""
    test_db = ConsultasDatabase(db_path=TEST_DB_PATH)
    monkeypatch.setattr(main, "db", test_db)

    # Crear directorio de descarga para las pruebas y configurar la variable de entorno
    os.makedirs(TEST_DOWNLOAD_PATH, exist_ok=True)
    monkeypatch.setattr(main, "DOWNLOAD_PATH", TEST_DOWNLOAD_PATH)

    monkeypatch.setattr(main, "recover", BackgroundSimulator(test_db))

    try:
        yield
    finally:
        if os.path.exists(TEST_DB_PATH):
            os.remove(TEST_DB_PATH)
        if os.path.exists(TEST_DOWNLOAD_PATH):
            shutil.rmtree(TEST_DOWNLOAD_PATH)

client = TestClient(main.app)


def _wait_until_completed(consulta_id: str, timeout_s: int = 20):
    start = time.time()
    while time.time() - start < timeout_s:
        data = client.get(f"/query/{consulta_id}").json()
        if data.get("estado") == "completado":
            return True
        time.sleep(1)
    return False


def _assert_only_tgz(files):
    assert files, "No se generaron archivos"
    assert all(str(f).endswith(".tgz") for f in files), f"Se encontraron no .tgz: {files}"


def _assert_only_nc(files):
    assert files, "No se generaron archivos"
    assert all(str(f).endswith(".nc") for f in files), f"Se encontraron no .nc: {files}"


def test_lustre_returns_tgz_s3_returns_nc_for_all_cases(monkeypatch):
    """
    Verifica que:
    - Cuando todo se recupera en Lustre (local=1.0, s3=0.0):
        - L1b + bandas=ALL => lustre: .tgz ; s3: vacío
        - L2 + productos=ALL + bandas=ALL => lustre: .tgz ; s3: vacío
    - Cuando todo se recupera en S3 (local=0.0, s3=1.0):
        - En ambos casos anteriores => s3: .nc (nunca .tgz)
    """
    # Fase 1: Forzar Lustre
    monkeypatch.setattr(settings, "sim_local_success_rate", 1.0)
    monkeypatch.setattr(settings, "sim_s3_success_rate", 0.0)
    monkeypatch.setattr(main, "recover", BackgroundSimulator(main.db))

    # 1A) L1b + bandas ALL (FD)
    monkeypatch.setattr("main.generar_id_consulta", lambda: "TEST_LUSTRE_L1B_ALL")
    req_l1b = {
        "nivel": "L1b",
        "dominio": "fd",
        "bandas": ["ALL"],
        "fechas": {"20240510": ["12:00-12:10"]}
    }
    r = client.post("/query", json=req_l1b)
    assert r.status_code == 202
    assert _wait_until_completed("TEST_LUSTRE_L1B_ALL")
    resultados = client.get("/query/TEST_LUSTRE_L1B_ALL?resultados=true").json()["resultados"]
    lustre_files = resultados["fuentes"]["lustre"]["archivos"]
    s3_files = resultados["fuentes"]["s3"]["archivos"]
    _assert_only_tgz(lustre_files)
    assert s3_files == []

    # 1B) L2 + productos ALL + bandas ALL (FD)
    monkeypatch.setattr("main.generar_id_consulta", lambda: "TEST_LUSTRE_L2_ALL_ALL")
    req_l2 = {
        "nivel": "L2",
        "productos": ["ALL"],
        "bandas": ["ALL"],
        "dominio": "fd",
        "fechas": {"20240510": ["12:00"]}
    }
    r = client.post("/query", json=req_l2)
    assert r.status_code == 202
    assert _wait_until_completed("TEST_LUSTRE_L2_ALL_ALL")
    resultados = client.get("/query/TEST_LUSTRE_L2_ALL_ALL?resultados=true").json()["resultados"]
    lustre_files = resultados["fuentes"]["lustre"]["archivos"]
    s3_files = resultados["fuentes"]["s3"]["archivos"]
    _assert_only_tgz(lustre_files)
    assert s3_files == []

    # Fase 2: Forzar S3
    monkeypatch.setattr(settings, "sim_local_success_rate", 0.0)
    monkeypatch.setattr(settings, "sim_s3_success_rate", 1.0)
    monkeypatch.setattr(main, "recover", BackgroundSimulator(main.db))

    # 2A) L1b + bandas ALL (FD) -> S3 debe devolver .nc
    monkeypatch.setattr("main.generar_id_consulta", lambda: "TEST_S3_L1B_ALL")
    r = client.post("/query", json=req_l1b)
    assert r.status_code == 202
    assert _wait_until_completed("TEST_S3_L1B_ALL")
    resultados = client.get("/query/TEST_S3_L1B_ALL?resultados=true").json()["resultados"]
    lustre_files = resultados["fuentes"]["lustre"]["archivos"]
    s3_files = resultados["fuentes"]["s3"]["archivos"]
    assert lustre_files == []
    _assert_only_nc(s3_files)

    # 2B) L2 + productos ALL + bandas ALL (FD) -> S3 debe devolver .nc
    monkeypatch.setattr("main.generar_id_consulta", lambda: "TEST_S3_L2_ALL_ALL")
    r = client.post("/query", json=req_l2)
    assert r.status_code == 202
    assert _wait_until_completed("TEST_S3_L2_ALL_ALL")
    resultados = client.get("/query/TEST_S3_L2_ALL_ALL?resultados=true").json()["resultados"]
    lustre_files = resultados["fuentes"]["lustre"]["archivos"]
    s3_files = resultados["fuentes"]["s3"]["archivos"]
    assert lustre_files == []
    _assert_only_nc(s3_files)
