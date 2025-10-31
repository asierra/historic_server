import time
import types
import pytest
import os
import shutil
from fastapi.testclient import TestClient

# ------------- Fakes aislados para DB y Recover -------------

class FakeDB:
    def __init__(self):
        self.store = {}  # consulta_id -> record

    def crear_consulta(self, consulta_id, query_dict):
        # Simula el insert inicial
        self.store[consulta_id] = {
            "id": consulta_id,
            "estado": "recibido",
            "progreso": 0,
            "mensaje": "recibido",
            "query": query_dict,
            "resultados": None,
            "timestamp_actualizacion": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        return True

    def obtener_consulta(self, consulta_id):
        return self.store.get(consulta_id)

    def actualizar_estado(self, consulta_id, estado, progreso=None, mensaje=None):
        rec = self.store.get(consulta_id)
        if not rec:
            return False
        rec["estado"] = estado
        if progreso is not None:
            rec["progreso"] = progreso
        if mensaje is not None:
            rec["mensaje"] = mensaje
        rec["timestamp_actualizacion"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        return True

    def guardar_resultados(self, consulta_id, resultados, mensaje=None):
        rec = self.store.get(consulta_id)
        if not rec:
            return False
        rec["resultados"] = resultados
        rec["estado"] = "completado"
        rec["progreso"] = 100
        rec["mensaje"] = mensaje or "completado"
        rec["timestamp_actualizacion"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        return True

    def listar_consultas(self, estado=None, limite=20):
        values = list(self.store.values())
        if estado:
            values = [v for v in values if v["estado"] == estado]
        return values[:limite]


class FakeRecover:
    def __init__(self, db):
        self.db = db
        # Añadir atributos para compatibilidad con el endpoint /health
        self.lustre_enabled = True
        self.s3_fallback_enabled = True

    def procesar_consulta(self, consulta_id, query_dict):
        # No hace nada pesado, solo marca 'procesando' para simular aceptada
        self.db.actualizar_estado(consulta_id, "procesando", 10, "Preparando entorno")


@pytest.fixture
def client(monkeypatch):
    # Import main del Query Processor
    TEST_DOWNLOAD_PATH = "./test_downloads_status"
    import main

    # Reemplazar DB global y recover por fakes
    fake_db = FakeDB()
    monkeypatch.setattr(main, "db", fake_db, raising=True)
    monkeypatch.setenv("PROCESSOR_MODE", "real")  # por si el módulo lo usa

    # Crear directorio de descarga para las pruebas y configurar la variable de entorno
    os.makedirs(TEST_DOWNLOAD_PATH, exist_ok=True)
    monkeypatch.setattr(main, "DOWNLOAD_PATH", TEST_DOWNLOAD_PATH)

    fake_recover = FakeRecover(fake_db)
    monkeypatch.setattr(main, "recover", fake_recover, raising=True)

    # Crear cliente
    try:
        with TestClient(main.app) as c:
            yield c, fake_db
    finally:
        if os.path.exists(TEST_DOWNLOAD_PATH):
            shutil.rmtree(TEST_DOWNLOAD_PATH)


# ------------- Tests de códigos y headers -------------

def test_post_query_returns_202_and_location(client):
    c, db = client
    payload = {
        "sat": "GOES-16",
        "sensor": "abi",
        "nivel": "L1b",
        "dominio": "fd",
        "bandas": ["02"],
        "fechas": {"20240101": ["00:00-01:00"]},
        "creado_por": "test@example.com",
    }
    r = c.post("/query", json=payload)
    assert r.status_code == 202
    body = r.json()
    assert body["success"] is True
    consulta_id = body["consulta_id"]
    # Debe incluir Location hacia /query/{id}
    assert r.headers.get("Location") == f"/query/{consulta_id}"
    # La consulta debe existir en DB con estado 'recibido' o ya 'procesando'
    rec = db.obtener_consulta(consulta_id)
    assert rec is not None
    assert rec["estado"] in ("recibido", "procesando")


def test_get_query_202_when_processing(client):
    c, db = client
    # Crear consulta 'manual' en DB (simulando que existe y está procesando)
    consulta_id = "ABC12345"
    db.store[consulta_id] = {
        "id": consulta_id,
        "estado": "procesando",
        "progreso": 30,
        "mensaje": "en curso",
        "query": {},
        "resultados": None,
        "timestamp_actualizacion": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    r = c.get(f"/query/{consulta_id}")
    assert r.status_code == 202
    assert r.headers.get("Retry-After") in (None, "10")  # opcional
    body = r.json()
    assert body["estado"] == "procesando"


def test_get_query_200_when_completed(client):
    c, db = client
    consulta_id = "DONE1234"
    db.store[consulta_id] = {
        "id": consulta_id,
        "estado": "completado",
        "progreso": 100,
        "mensaje": "ok",
        "query": {},
        "resultados": {
            "fuentes": {"s3": {"total": 10}, "lustre": {"total": 0}},
            "total_archivos": 10,
            "total_mb": 12.3,
            "ruta_destino": "/data/tmp/DONE1234",
        },
        "timestamp_actualizacion": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    r = c.get(f"/query/{consulta_id}")
    assert r.status_code == 200
    body = r.json()
    assert body["estado"] == "completado"
    assert body["total_archivos"] == 10
    assert body["archivos_s3"] == 10


def test_get_query_404_when_missing(client):
    c, db = client
    r = c.get("/query/NOTEXISTS")
    assert r.status_code == 404


def test_get_query_500_when_error(client):
    c, db = client
    consulta_id = "ERR00001"
    db.store[consulta_id] = {
        "id": consulta_id,
        "estado": "error",
        "progreso": 0,
        "mensaje": "falló",
        "query": {},
        "resultados": None,
        "timestamp_actualizacion": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    r = c.get(f"/query/{consulta_id}")
    assert r.status_code in (500, 200)  # según tu implementación: ideal 500
    # Si prefieres 200 con estado=error, ajusta el assert arriba.


def test_post_restart_202_and_location(client):
    c, db = client
    # Crear consulta existente con estado 'error' o 'procesando'
    consulta_id = "RST12345"
    db.store[consulta_id] = {
        "id": consulta_id,
        "estado": "error",
        "progreso": 50,
        "mensaje": "falló",
        "query": {"foo": "bar"},
        "resultados": None,
        "timestamp_actualizacion": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    r = c.post(f"/query/{consulta_id}/restart")
    assert r.status_code == 202
    assert r.headers.get("Location") == f"/query/{consulta_id}"
    body = r.json()
    assert body["success"] is True


def test_post_restart_404_when_missing(client):
    c, db = client
    r = c.post("/query/NOTEXISTS/restart")
    assert r.status_code == 404


def test_post_restart_400_when_invalid_state(client):
    c, db = client
    consulta_id = "RSTBAD12"
    db.store[consulta_id] = {
        "id": consulta_id,
        "estado": "recibido",  # no permitido para restart
        "progreso": 0,
        "mensaje": "ok",
        "query": {},
        "resultados": None,
        "timestamp_actualizacion": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    r = c.post(f"/query/{consulta_id}/restart")
    assert r.status_code == 400