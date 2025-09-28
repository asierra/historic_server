import pytest
from fastapi.testclient import TestClient
import main  # Importamos el módulo principal
import time
from background_simulator import BackgroundSimulator
from database import ConsultasDatabase
import os

# --- Configuración de la Base de Datos de Prueba ---

TEST_DB_PATH = "test_consultas.db"

@pytest.fixture(autouse=True)
def override_db_for_tests(monkeypatch):
    """
    Fixture que se ejecuta automáticamente para cada prueba.
    Reemplaza la base de datos global en `main` con una de prueba.
    """
    # 1. Crear una instancia de la DB de prueba
    test_db = ConsultasDatabase(db_path=TEST_DB_PATH)
    
    # 2. Reemplazar los objetos globales en main.py
    monkeypatch.setattr(main, "db", test_db)
    monkeypatch.setattr(main, "recover", BackgroundSimulator(test_db))
    
    try:
        yield  # Aquí es donde se ejecuta la prueba
    finally:
        # 3. Limpiar la base de datos después de la prueba
        if os.path.exists(TEST_DB_PATH):
            os.remove(TEST_DB_PATH)

# El cliente de prueba ahora usará la app con los objetos ya parcheados
client = TestClient(main.app)

# --- Datos de Prueba ---

VALID_REQUEST = {
    "sat": "GOES-16",
    "nivel": "L2",
    "dominio": "fd",
    "bandas": ["02", "13"],
    "fechas": {
        "20231026": ["00:00-01:00", "15:30"],
        "20231027-20231028": ["23:00-23:59"]
    }
}

INVALID_SATELLITE_REQUEST = {
    "sat": "METEOSAT-9",
    "nivel": "L2",
    "fechas": { "20231026": ["00:00"] }
}

INVALID_BAND_REQUEST = {
    "sat": "GOES-16",
    "nivel": "L1b",
    "bandas": ["99", "02"], # La banda "99" es inválida
    "fechas": { "20231026": ["00:00"] }
}

MISSING_FECHAS_REQUEST = {
    "sat": "GOES-18",
    "nivel": "L1b"
}

# --- Pruebas para el endpoint /validate ---

def test_validate_success():
    """Prueba que una solicitud válida pasa la validación."""
    response = client.post("/validate", json=VALID_REQUEST)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["message"] == "La solicitud es válida."
    assert data["resumen_solicitud"]["satelite"] == "GOES-16"
    assert data["resumen_solicitud"]["total_fechas_expandidas"] == 3

def test_validate_invalid_satellite():
    """Prueba que una solicitud con un satélite no soportado falla."""
    response = client.post("/validate", json=INVALID_SATELLITE_REQUEST)
    assert response.status_code == 400
    assert "Satélite 'METEOSAT-9' no es soportado o es inválido" in response.json()["detail"]

def test_validate_invalid_band():
    """Prueba que una solicitud con una banda inválida falla."""
    response = client.post("/validate", json=INVALID_BAND_REQUEST)
    assert response.status_code == 400 # La excepción ValueError se convierte en 400
    assert "Bandas inválidas: ['99']" in response.json()["detail"]

def test_validate_missing_required_field():
    """Prueba que una solicitud sin el campo 'fechas' falla (error 422 de Pydantic)."""
    response = client.post("/validate", json=MISSING_FECHAS_REQUEST)
    assert response.status_code == 422 # Unprocessable Entity
    data = response.json()
    assert data["detail"][0]["msg"] == "Field required" # Pydantic v2 message
    assert data["detail"][0]["loc"] == ["fechas"]

# --- Pruebas para el endpoint /query ---

def test_query_success(monkeypatch):
    """Prueba que una solicitud de consulta válida se crea correctamente."""
    # Reemplazamos la función que genera IDs para usar uno predecible
    monkeypatch.setattr("main.generar_id_consulta", lambda: "TEST_SUCCESS")

    response = client.post("/query", json=VALID_REQUEST)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["estado"] == "recibido"
    assert data["consulta_id"] == "TEST_SUCCESS"
    assert data["resumen"]["satelite"] == "GOES-16"

def test_internal_date_format_is_julian(monkeypatch):
    """Verifica que el formato de fecha interno en la DB es YYYYJJJ."""
    TEST_ID = "TEST_JULIAN_DATE"
    monkeypatch.setattr("main.generar_id_consulta", lambda: TEST_ID)

    # Usamos una fecha conocida: 2023-10-26 es el día 299 del año.
    request_data = {
        "sat": "GOES-16",
        "nivel": "L1b",
        "bandas": ["02"],
        "fechas": {
            "20231026": ["12:00"]
        }
    }

    # 1. Crear la consulta
    response = client.post("/query", json=request_data)
    assert response.status_code == 200

    # 2. Obtener la consulta directamente de la DB de prueba
    consulta_guardada = main.db.obtener_consulta(TEST_ID)
    assert consulta_guardada is not None
    
    # 3. Verificar que la clave de fecha es YYYYJJJ
    fechas_internas = consulta_guardada['query']['fechas']
    assert "2023299" in fechas_internas
    assert "20231026" not in fechas_internas

def test_query_and_get_status(monkeypatch):
    """Prueba un flujo completo: crear, monitorear y verificar una consulta."""
    # Definimos un ID de prueba constante para este test
    TEST_ID = "TEST_FLUJO_COMPLETO"

    # Reemplazamos la función que genera IDs para usar nuestro ID de prueba
    monkeypatch.setattr("main.generar_id_consulta", lambda: TEST_ID)

    # 1. Crear la consulta y verificar la respuesta inicial
    create_response = client.post("/query", json=VALID_REQUEST)
    assert create_response.status_code == 200
    create_data = create_response.json()
    assert create_data["consulta_id"] == TEST_ID
    assert create_data["estado"] == "recibido"

    # 2. Monitorear la consulta hasta que se complete
    # Esto hace la prueba más robusta al esperar el estado final.
    for _ in range(10): # Intentar por un máximo de 10 segundos
        get_response = client.get(f"/query/{TEST_ID}")
        assert get_response.status_code == 200
        get_data = get_response.json()
        if get_data["estado"] == "completado":
            break
        time.sleep(1)
    
    # 3. Verificar el estado final y obtener los resultados
    assert get_data["estado"] == "completado"
    assert get_data["progreso"] == 100

    results_response = client.get(f"/query/{TEST_ID}?resultados=True")
    assert results_response.status_code == 200
    assert results_response.json().get("resultados") is not None

def test_get_nonexistent_query():
    """Prueba que al pedir una consulta con un ID falso se obtiene un 404."""
    response = client.get("/query/ID_FALSO_123")
    assert response.status_code == 404
    assert response.json()["detail"] == "Consulta no encontrada"

def test_list_queries():
    """Prueba que el endpoint de listado funciona y devuelve una lista."""
    response = client.get("/queries")
    assert response.status_code == 200
    data = response.json()
    assert "total" in data
    assert "consultas" in data
    assert isinstance(data["consultas"], list)

def test_recovery_query_is_generated_on_failure(monkeypatch):
    """
    Verifica que se genera una 'consulta_recuperacion' cuando el simulador
    fuerza un fallo en la recuperación de archivos.
    """
    TEST_ID = "TEST_RECOVERY_QUERY"

    # Forzar fallos: hacer que random.random() siempre devuelva un valor alto (e.g., 0.9)
    # Esto hará que falle la recuperación local (que requiere < 0.8) y la de S3 (que requiere < 0.5)
    monkeypatch.setattr("random.random", lambda: 0.9)
    monkeypatch.setattr("main.generar_id_consulta", lambda: TEST_ID)

    # Usar una solicitud simple para que el test sea rápido
    simple_request = {
        "sat": "GOES-16",
        "nivel": "L1b",
        "bandas": ["02"],
        "fechas": { "20231026": ["12:00"] }
    }

    # 1. Crear la consulta
    create_response = client.post("/query", json=simple_request)
    assert create_response.status_code == 200

    # 2. Esperar a que se complete
    for _ in range(10):
        get_response = client.get(f"/query/{TEST_ID}")
        if get_response.json()["estado"] == "completado":
            break
        time.sleep(1)

    # 3. Obtener los resultados y verificar la consulta de recuperación
    results_response = client.get(f"/query/{TEST_ID}?resultados=True")
    assert results_response.status_code == 200
    resultados = results_response.json()["resultados"]
    
    assert "consulta_recuperacion" in resultados
    assert resultados["consulta_recuperacion"] is not None
    assert "20231026" in resultados["consulta_recuperacion"]["fechas"]