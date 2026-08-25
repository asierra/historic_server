import pytest
from fastapi.testclient import TestClient
import main  # Importamos el módulo principal
from collections import namedtuple
import shutil
import time
import re
from background_simulator import BackgroundSimulator
from database import ConsultasDatabase, LEASE_POR_DEFECTO_S
import os
from datetime import datetime, timedelta
from recover import RecoverFiles  # Importar el procesador real para la prueba de integración
from processors import HistoricQueryProcessor
from settings import settings
from tests.conftest import cola_drenando

# --- Configuración de la Base de Datos de Prueba ---

TEST_DB_PATH = "test_consultas.db"
TEST_DOWNLOAD_PATH = "./test_downloads"

@pytest.fixture(autouse=True)
def override_db_for_tests(monkeypatch):
    """
    Fixture que se ejecuta automáticamente para cada prueba.
    Reemplaza la base de datos global en `main` con una de prueba.
    """
    # 1. Crear una instancia de la DB de prueba
    test_db = ConsultasDatabase(db_path=TEST_DB_PATH)
    
    # Establecer valores en el objeto de settings para el simulador
    monkeypatch.setattr(settings, "sim_local_success_rate", 0.9)
    monkeypatch.setattr(settings, "sim_s3_success_rate", 0.8)

    # Crear directorio de descarga para las pruebas y configurar la variable de entorno
    os.makedirs(TEST_DOWNLOAD_PATH, exist_ok=True)
    monkeypatch.setattr(main, "DOWNLOAD_PATH", TEST_DOWNLOAD_PATH)

    
    # 2. Reemplazar los objetos globales en main.py.
    # `processor` incluido: normalmente lo construye el lifespan, que TestClient
    # solo dispara si se usa como context manager. Como el cliente se crea a nivel
    # de módulo, sin `with`, sin esto queda en None y /query responde 400
    # ("'NoneType' object has no attribute 'procesar_request'").
    monkeypatch.setattr(main, "db", test_db)
    monkeypatch.setattr(main, "recover", BackgroundSimulator(test_db))
    monkeypatch.setattr(main, "processor", HistoricQueryProcessor())
    # 3. Desactivar el apagado del executor para evitar errores en las pruebas
    
    try:
        yield  # Aquí es donde se ejecuta la prueba
    finally:
        # 3. Limpiar la base de datos después de la prueba
        if os.path.exists(TEST_DB_PATH):
            os.remove(TEST_DB_PATH)
        if os.path.exists(TEST_DOWNLOAD_PATH):
            shutil.rmtree(TEST_DOWNLOAD_PATH)

@pytest.fixture
def cola_activa():
    """Arranca el bucle de cola mientras dure la prueba.

    Sólo lo piden las pruebas que esperan a ver 'completado'. No es autouse a
    propósito: las que comprueban en qué estado deja la fila un endpoint no
    quieren un consumidor reclamándola por detrás a mitad de aserción.
    """
    with cola_drenando() as bucle:
        yield bucle


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
    "dominio": "fd",
    "fechas": { "20231026": ["00:00"] }
}

INVALID_BAND_REQUEST = {
    "sat": "GOES-16",
    "nivel": "L1b",
    "bandas": ["99", "02"], # La banda "99" es inválida
    "dominio": "fd",
    "fechas": { "20231026": ["00:00"] }
}

MISSING_FECHAS_REQUEST = {
    "sat": "GOES-18",
    "nivel": "L1b",
    "dominio": "fd"
}

# --- Pruebas para el endpoint /validate ---

def test_validate_success():
    """Prueba que una solicitud válida pasa la validación."""
    response = client.post("/validate", json=VALID_REQUEST)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["message"] == "La solicitud es válida."
    # Verificar la nueva estructura plana
    assert "archivos_estimados" in data
    assert "tamanio_estimado_mb" in data

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

def test_validate_l2_acha_without_bandas_is_valid():
    """Nivel L2 con ACHA no requiere 'bandas'."""
    payload = {
        "nivel": "L2",
        "dominio": "conus",
        "productos": ["ACHA"],
        "fechas": {"20200101": ["19:19-22:19"]},
        "creado_por": "vescudero@geografia.unam.mx"
    }
    resp = client.post("/validate", json=payload)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    if isinstance(body, dict) and "success" in body:
        assert body["success"] is True

def test_validate_future_date_is_invalid():
    """Prueba que una solicitud con una fecha en el futuro falla."""
    future_date_request = {
        "sat": "GOES-16",
        "nivel": "L1b",
        "dominio": "fd",
        "fechas": { "20990101": ["12:00"] } # Fecha lejana en el futuro
    }
    response = client.post("/validate", json=future_date_request)
    assert response.status_code == 400
    assert "está en el futuro y no es válida" in response.json()["detail"]

def test_validate_rejects_string_all_for_bandas():
    """Prueba que una solicitud con 'bandas': 'ALL' (string) es rechazada."""
    request_with_string_all = {
        "sat": "GOES-16",
        "nivel": "L1b",
        "dominio": "fd",
        "bandas": "ALL", # Formato incorrecto
        "fechas": { "20231026": ["12:00"] }
    }
    response = client.post("/validate", json=request_with_string_all)
    assert response.status_code == 422 # Unprocessable Entity
    assert "Input should be a valid list" in response.text

def test_validate_accepts_list_all_for_bandas():
    """Prueba que una solicitud con 'bandas': ['ALL'] (lista) es aceptada."""
    request_with_list_all = {
        "sat": "GOES-16",
        "nivel": "L1b",
        "dominio": "fd",
        "bandas": ["ALL"], # Formato correcto
        "fechas": { "20231026": ["12:00"] }
    }
    response = client.post("/validate", json=request_with_list_all)
    assert response.status_code == 200

# --- Pruebas para límites de consulta y disco ---

def test_validate_passes_when_limits_are_zero(monkeypatch):
    """Prueba que una consulta grande pasa si los límites son 0 (ilimitados)."""
    monkeypatch.setattr(main, "MAX_FILES_PER_QUERY", 0)
    monkeypatch.setattr(main, "MAX_SIZE_MB_PER_QUERY", 0)

    # Simular que hay suficiente espacio en disco
    free_space_bytes = 50 * 1024 * 1024 * 1024 # 50 GB
    disk_usage_result = namedtuple('disk_usage_result', ['total', 'used', 'free'])
    mock_disk_usage = disk_usage_result(total=100, used=50, free=free_space_bytes)
    monkeypatch.setattr(shutil, "disk_usage", lambda path: mock_disk_usage)

    response = client.post("/validate", json=VALID_REQUEST)
    assert response.status_code == 200
    assert response.json()["success"] is True

def test_validate_passes_when_within_limits(monkeypatch):
    """Prueba que una consulta pasa si está dentro de todos los límites."""
    monkeypatch.setattr(main, "MAX_FILES_PER_QUERY", 100) # Consulta estima 40
    monkeypatch.setattr(main, "MAX_SIZE_MB_PER_QUERY", 2000) # Consulta estima ~1580
    monkeypatch.setattr(main, "MIN_FREE_SPACE_GB_BUFFER", 5)

    # Simular que hay suficiente espacio en disco
    free_space_bytes = 50 * 1024 * 1024 * 1024 # 50 GB
    disk_usage_result = namedtuple('disk_usage_result', ['total', 'used', 'free'])
    mock_disk_usage = disk_usage_result(total=100, used=50, free=free_space_bytes)
    monkeypatch.setattr(shutil, "disk_usage", lambda path: mock_disk_usage)

    response = client.post("/validate", json=VALID_REQUEST)
    assert response.status_code == 200
    assert response.json()["success"] is True

# --- Pruebas para el endpoint /query ---

def test_query_success(monkeypatch):
    """Prueba que una solicitud de consulta válida se crea correctamente."""
    class _DummyQueryObj:
        def to_dict(self):
            return {
                "satelite": "GOES-16",
                "sensor": "ABI",
                "nivel": "L2",
                "fechas": {"2023299": ["00:00"]},
                "total_horas": 1,
            }

    class _DummyProcessor:
        def procesar_request(self, data, config):
            return _DummyQueryObj()

    monkeypatch.setattr(main, "processor", _DummyProcessor())

    # Reemplazamos la función que genera IDs para usar uno predecible
    monkeypatch.setattr("main.generar_id_consulta", lambda: "TEST_SUCCESS")

    response = client.post("/query", json=VALID_REQUEST)
    assert response.status_code == 202
    data = response.json()
    assert data["success"] is True
    assert data["estado"] == "recibido"
    assert data["consulta_id"] == "TEST_SUCCESS"
    assert data["resumen"]["satelite"] == "GOES-16"


def test_query_uses_provided_id_from_payload():
    """Prueba que /query respeta el campo id enviado por el cliente."""
    class _DummyQueryObj:
        def to_dict(self):
            return {
                "satelite": "GOES-16",
                "sensor": "ABI",
                "nivel": "L2",
                "fechas": {"2023299": ["00:00"]},
                "total_horas": 1,
            }

    class _DummyProcessor:
        def procesar_request(self, data, config):
            return _DummyQueryObj()

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(main, "processor", _DummyProcessor())

    provided_id = "cliente-id-123"
    payload = dict(VALID_REQUEST)
    payload["id"] = provided_id

    response = client.post("/query", json=payload)
    assert response.status_code == 202
    data = response.json()
    assert data["success"] is True
    assert data["consulta_id"] == provided_id
    monkeypatch.undo()


def test_query_generates_eight_char_id_when_payload_has_no_id(monkeypatch):
    """Prueba que /query genera un ID alfanumérico de 8 caracteres cuando no se envía id."""
    class _DummyQueryObj:
        def to_dict(self):
            return {
                "satelite": "GOES-16",
                "sensor": "ABI",
                "nivel": "L2",
                "fechas": {"2023299": ["00:00"]},
                "total_horas": 1,
            }

    class _DummyProcessor:
        def procesar_request(self, data, config):
            return _DummyQueryObj()

    monkeypatch.setattr(main, "processor", _DummyProcessor())

    payload = dict(VALID_REQUEST)
    payload.pop("id", None)

    response = client.post("/query", json=payload)
    assert response.status_code == 202
    consulta_id = response.json()["consulta_id"]
    assert re.fullmatch(r"[A-Za-z0-9]{8}", consulta_id)

def test_internal_date_format_is_julian(monkeypatch):
    """Verifica que el formato de fecha interno en la DB es YYYYJJJ."""
    TEST_ID = "TEST_JULIAN_DATE"
    monkeypatch.setattr("main.generar_id_consulta", lambda: TEST_ID)

    # Usamos una fecha conocida: 2023-10-26 es el día 299 del año.
    request_data = {
        "sat": "GOES-16",
        "nivel": "L1b",
        "dominio": "fd",
        "bandas": ["02"],
        "fechas": {
            "20231026": ["12:00"]
        }
    }

    # 1. Crear la consulta
    response = client.post("/query", json=request_data)
    assert response.status_code == 202

    # 2. Obtener la consulta directamente de la DB de prueba
    consulta_guardada = main.db.obtener_consulta(TEST_ID)
    assert consulta_guardada is not None
    
    # 3. Verificar que la clave de fecha es YYYYJJJ
    fechas_internas = consulta_guardada['query']['fechas']
    assert "2023299" in fechas_internas
    assert "20231026" not in fechas_internas

def test_query_and_get_status(cola_activa, monkeypatch):
    """Prueba un flujo completo: crear, monitorear y verificar una consulta."""
    # Definimos un ID de prueba constante para este test
    TEST_ID = "TEST_FLUJO_COMPLETO"

    # Reemplazamos la función que genera IDs para usar nuestro ID de prueba
    monkeypatch.setattr("main.generar_id_consulta", lambda: TEST_ID)

    # 1. Crear la consulta y verificar la respuesta inicial
    create_response = client.post("/query", json=VALID_REQUEST)
    assert create_response.status_code == 202
    create_data = create_response.json()
    assert create_data["consulta_id"] == TEST_ID
    assert create_data["estado"] == "recibido"

    # 2. Monitorear la consulta hasta que se complete
    # Esto hace la prueba más robusta al esperar el estado final.
    # 60 s: el simulador tarda ~9 s y los presupuestos de 10-15 s dejaban un
    # margen de 1.1x-1.6x. Es el mismo defecto que ya se corrigió en las esperas
    # de test_simulator_sources_behavior.py, y desde la entrega 2 hay además el
    # sondeo del bucle de por medio. En el camino feliz se sale en cuanto está
    # 'completado'; el presupuesto sólo se agota si la prueba ya iba a fallar.
    for _ in range(60):
        get_response = client.get(f"/query/{TEST_ID}")
        assert get_response.status_code in (200, 202)
        get_data = get_response.json()
        if get_data["estado"] == "completado":
            break
        time.sleep(1)
    
    # 3. Verificar el estado final y obtener los resultados
    assert get_data["estado"] == "completado"
    assert get_data["progreso"] == 100
    # Verificar que los campos de resumen están en la respuesta de estado completado
    assert "total_archivos" in get_data
    assert "archivos_lustre" in get_data
    assert "archivos_s3" in get_data
    assert get_data["total_archivos"] == get_data["archivos_lustre"] + get_data["archivos_s3"]

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

def test_recovery_query_is_generated_on_failure(cola_activa, monkeypatch):
    """
    Verifica que se genera una 'consulta_recuperacion' cuando el simulador
    fuerza un fallo en la recuperación de archivos.
    """
    TEST_ID = "TEST_RECOVERY_QUERY"
    monkeypatch.setattr("main.generar_id_consulta", lambda: TEST_ID)

    # Forzar fallos en el simulador para que falle tanto en local como en S3.
    monkeypatch.setattr(settings, "sim_local_success_rate", 0.0)
    monkeypatch.setattr(settings, "sim_s3_success_rate", 0.0)
    
    # Es necesario recrear el simulador para que tome las nuevas variables de entorno.
    monkeypatch.setattr(main, "recover", BackgroundSimulator(main.db))

    # Usar una solicitud simple para que el test sea rápido
    simple_request = {
        "sat": "GOES-16",
        "nivel": "L1b",
        "dominio": "fd",
        "bandas": ["02"],
        # Usar un rango de fechas para probar la lógica de reconstrucción
        "fechas": { "20231026": ["12:00"] }
    }

    # 1. Crear la consulta
    create_response = client.post("/query", json=simple_request)
    assert create_response.status_code == 202

    # 2. Esperar a que se complete
    # 60 s: el simulador tarda ~9 s y los presupuestos de 10-15 s dejaban un
    # margen de 1.1x-1.6x. Es el mismo defecto que ya se corrigió en las esperas
    # de test_simulator_sources_behavior.py, y desde la entrega 2 hay además el
    # sondeo del bucle de por medio. En el camino feliz se sale en cuanto está
    # 'completado'; el presupuesto sólo se agota si la prueba ya iba a fallar.
    for _ in range(60):
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
    
    # Verificar que la consulta de recuperación es precisa
    rec_query = resultados["consulta_recuperacion"]
    assert "20231026" in rec_query["fechas"]
    assert rec_query["fechas"]["20231026"] == ["12:00"]

def test_simulator_report_has_correct_sources_structure(cola_activa, monkeypatch):
    """
    Verifica que el reporte final del simulador tiene la estructura correcta
    de 'fuentes' (lustre y s3), que implementamos anteriormente.
    """
    TEST_ID = "TEST_SOURCES_STRUCTURE"
    monkeypatch.setattr("main.generar_id_consulta", lambda: TEST_ID)

    # 1. Crear la consulta usando una solicitud válida
    create_response = client.post("/query", json=VALID_REQUEST)
    assert create_response.status_code == 202

    # 2. Esperar a que el simulador complete el trabajo.
    #    60 s y no 10: el simulador tarda ~9 s, así que el margen era de 1.1x
    #    —el mismo defecto que ya se corrigió en las otras esperas de la suite—
    #    y desde la entrega 2 hay además el sondeo del bucle de por medio. En el
    #    camino feliz se sale en cuanto está 'completado'.
    for _ in range(60):
        get_response = client.get(f"/query/{TEST_ID}")
        if get_response.json()["estado"] == "completado":
            break
        time.sleep(1)
    else:
        pytest.fail("La consulta del simulador no se completó a tiempo.")

    # 3. Obtener los resultados y verificar la estructura del reporte
    results_response = client.get(f"/query/{TEST_ID}?resultados=True")
    assert results_response.status_code == 200
    resultados = results_response.json()["resultados"]
    
    assert "fuentes" in resultados
    fuentes = resultados["fuentes"]
    
    assert "lustre" in fuentes
    assert "s3" in fuentes
    
    for source_name in ["lustre", "s3"]:
        assert "archivos" in fuentes[source_name]
        assert "total" in fuentes[source_name]
        assert isinstance(fuentes[source_name]["archivos"], list)
        assert isinstance(fuentes[source_name]["total"], int)

    assert "total_archivos" in resultados
    assert resultados["total_archivos"] == fuentes["lustre"]["total"] + fuentes["s3"]["total"]


# --- Pruebas de Integración (I/O Real) ---

@pytest.fixture
def real_io_fixture(monkeypatch):
    """Fixture para configurar el entorno para pruebas de I/O real."""
    # Configurar settings para habilitar S3 y deshabilitar Lustre
    monkeypatch.setattr(settings, "s3_enabled", True)
    monkeypatch.setattr(settings, "lustre_enabled", True)  # Mantener habilitado para probar el fallback
    
    # Usamos la misma DB de prueba, pero con RecoverFiles
    test_db = main.db # Ya está parcheada por el fixture autouse
    real_recover = RecoverFiles(
        db=test_db,
        source_data_path="/tmp/nonexistent_lustre", # Ruta que no existe para forzar fallo
        base_download_path=os.path.dirname(TEST_DB_PATH), # Guardar en el dir de test
        executor=main.executor
    )
    monkeypatch.setattr(main, "recover", real_recover)
    yield

@pytest.mark.real_io
def test_s3_fallback_integration(real_io_fixture, cola_activa, monkeypatch):
    """
    Prueba de integración que verifica el fallback a S3 con un archivo L1b real.
    """
    TEST_ID = "TEST_S3_FALLBACK"
    monkeypatch.setattr("main.generar_id_consulta", lambda: TEST_ID)

    # Usar una fecha/hora/banda que sabemos que existe en S3
    s3_request = {
        "sat": "GOES-16",
        "nivel": "L1b",
        "dominio": "fd",
        "bandas": ["13"],
        "fechas": { "20210501": ["19:00-19:20"] }  # 2021-05-01 es día juliano 121, hora 19
    }

    create_response = client.post("/query", json=s3_request)
    assert create_response.status_code == 202

    timeout = 60  # segundos
    for _ in range(timeout):
        get_response = client.get(f"/query/{TEST_ID}")
        get_data = get_response.json()
        if get_data["estado"] == "completado":
            break
        time.sleep(1)
    else:
        pytest.fail(f"La consulta de S3 no se completó en {timeout} segundos. Mensaje final: {get_data.get('mensaje')}")

    results_response = client.get(f"/query/{TEST_ID}?resultados=True")
    assert results_response.status_code == 200
    resultados = results_response.json()["resultados"]

    assert resultados["fuentes"]["s3"]["total"] > 0

@pytest.mark.real_io
def test_s3_fallback_integration_l2_multi_product(real_io_fixture, cola_activa, monkeypatch):
    """
    Prueba de integración que verifica el fallback a S3 para productos L2 múltiples (ACHA y CMIP).
    """
    TEST_ID = "TEST_S3_FALLBACK_L2_MULTI"
    monkeypatch.setattr("main.generar_id_consulta", lambda: TEST_ID)

    s3_request = {
        "sat": "GOES-16",
        "nivel": "L2",
        "productos": ["ACHA", "CMIP"],
        "dominio": "conus",
        "bandas": ["13"],
        "fechas": { "20210501": ["19:00-19:17"] }
    }

    create_response = client.post("/query", json=s3_request)
    assert create_response.status_code == 202

    timeout = 60
    for _ in range(timeout):
        get_response = client.get(f"/query/{TEST_ID}")
        data = get_response.json()
        if data.get("estado") == "completado":
            break
        time.sleep(1)
    else:
        pytest.fail("La consulta no se completó a tiempo.")

    results_response = client.get(f"/query/{TEST_ID}?resultados=True")
    assert results_response.status_code == 200
    resultados = results_response.json()["resultados"]

    # Puede haber archivos en lustre y/o S3
    archivos = resultados["fuentes"]["s3"]["archivos"] + resultados["fuentes"]["lustre"]["archivos"]
    assert len(archivos) > 0

    # CMIP debe respetar la banda 13 (M6C13)
    cmip_files = [a for a in archivos if "-L2-CMIP" in a]
    assert cmip_files, "No se generaron archivos CMIP"
    assert all("-M6C13_" in a for a in cmip_files)

    # ACHA debe estar presente y no lleva banda (solo M6)
    acha_files = [a for a in archivos if "-L2-ACHAC-" in a]
    assert acha_files, "No se generaron archivos ACHA"

def test_complex_query_does_not_get_stuck(cola_activa, monkeypatch):
    """
    Verifica que una consulta compleja con muchas fechas y rangos no se queda
    atorada en el procesamiento y se completa correctamente.
    """
    TEST_ID = "TEST_COMPLEX_QUERY"
    monkeypatch.setattr("main.generar_id_consulta", lambda: TEST_ID)

    # Una solicitud compleja similar a la que causó problemas
    complex_request = {
        "nivel": "L2",
        "dominio": "conus",
        "productos": ["CMIP", "ACTP"],
        "fechas": {
            "20200101": ["19:19-22:19"],
            "20200212": ["17:51-20:51", "19:31-22:31"],
            "20201002": ["19:11-22:11"],
            "20201006": ["06:46-09:46"]
        },
        "creado_por": "test@lanot.unam.mx"
    }

    # Simular que hay suficiente espacio en disco para esta consulta grande
    free_space_bytes = 500 * 1024 * 1024 * 1024 # 500 GB
    disk_usage_result = namedtuple('disk_usage_result', ['total', 'used', 'free'])
    mock_disk_usage = disk_usage_result(total=1000, used=500, free=free_space_bytes)
    monkeypatch.setattr(shutil, "disk_usage", lambda path: mock_disk_usage)

    # Desactivar las cuotas por consulta (0 = sin límite): esta solicitud estima
    # ~36 GB y el límite de despliegue es 20 GB, así que se rechazaría con 413
    # antes de llegar a procesarse. Lo que este test verifica es que el pipeline
    # no se atore con muchas fechas y rangos, no el rechazo por cuota.
    monkeypatch.setattr(main, "MAX_SIZE_MB_PER_QUERY", 0)
    monkeypatch.setattr(main, "MAX_FILES_PER_QUERY", 0)

    # 1. Crear la consulta
    create_response = client.post("/query", json=complex_request)
    assert create_response.status_code == 202

    # 2. Monitorear hasta que se complete, con un timeout generoso
    # Si el proceso se atora, este bucle fallará por timeout.
    # 60 s y no 20: es la consulta más pesada de la suite (30 fechas con varios
    # rangos cada una) y 20 s no daban margen en un runner de CI. Lo que se
    # quiere detectar aquí es que el pipeline se atore, no cuánto tarda.
    timeout = 60  # segundos
    start_time = time.time()
    while time.time() - start_time < timeout:
        get_response = client.get(f"/query/{TEST_ID}")
        assert get_response.status_code in (200, 202)
        get_data = get_response.json()
        if get_data["estado"] == "completado":
            break
        time.sleep(1)
    else:
        pytest.fail(f"La consulta compleja no se completó en {timeout} segundos. Posiblemente se atoró.")

    # 3. Verificar que el estado final es 'completado'
    assert get_data["estado"] == "completado"

def test_simulator_l2_cmip_respects_requested_band(cola_activa, monkeypatch):
    """CMIP con bandas=['13'] solo debe generar archivos C13."""
    TEST_ID = "TEST_CMIP_ONLY_C13"
    monkeypatch.setattr("main.generar_id_consulta", lambda: TEST_ID)

    req = {
        "sat": "GOES-16",
        "nivel": "L2",
        "productos": ["ACHA", "CMIP"],
        "dominio": "conus",
        "bandas": ["13"],
        "fechas": {"20210501": ["19:00-19:17"]}
    }

    create_response = client.post("/query", json=req)
    assert create_response.status_code == 202

    # 60 s: el simulador tarda ~9 s y los presupuestos de 10-15 s dejaban un
    # margen de 1.1x-1.6x. Es el mismo defecto que ya se corrigió en las esperas
    # de test_simulator_sources_behavior.py, y desde la entrega 2 hay además el
    # sondeo del bucle de por medio. En el camino feliz se sale en cuanto está
    # 'completado'; el presupuesto sólo se agota si la prueba ya iba a fallar.
    for _ in range(60):
        get_resp = client.get(f"/query/{TEST_ID}")
        if get_resp.json()["estado"] == "completado":
            break
        time.sleep(1)
    else:
        pytest.fail("La consulta del simulador no se completó a tiempo.")

    results = client.get(f"/query/{TEST_ID}?resultados=True").json()["resultados"]
    archivos = results["fuentes"]["lustre"]["archivos"] + results["fuentes"]["s3"]["archivos"]

    # Los archivos CMIP deben contener '-M6C13_' y no otras bandas
    cmip_files = [a for a in archivos if "-L2-CMIP" in a or "-L2-CMIPC" in a]
    assert cmip_files, "No se generaron archivos CMIP"
    assert all("-M6C13_" in a for a in cmip_files)
    assert all("-M6C" in a and not any(f"-M6C{b:02d}_" in a for b in range(1,17) if b != 13) for a in cmip_files)

def test_l2_cmip_without_bandas_expands_to_all(cola_activa, monkeypatch):
    """
    Si no se envían bandas para L2+CMIP, se debe expandir a ALL (01..16).
    Verificamos que el simulador genere archivos con múltiples bandas Cdd.
    """
    TEST_ID = "TEST_CMIP_EXPANDS_ALL"
    monkeypatch.setattr("main.generar_id_consulta", lambda: TEST_ID)

    req = {
        "sat": "GOES-16",
        "nivel": "L2",
        "productos": ["CMIP"],
        "dominio": "conus",
        "fechas": {"20210501": ["19:00-19:17"]}  # sin 'bandas'
    }

    resp = client.post("/query", json=req)
    assert resp.status_code == 202

    # 60 s: el simulador tarda ~9 s y los presupuestos de 10-15 s dejaban un
    # margen de 1.1x-1.6x. Es el mismo defecto que ya se corrigió en las esperas
    # de test_simulator_sources_behavior.py, y desde la entrega 2 hay además el
    # sondeo del bucle de por medio. En el camino feliz se sale en cuanto está
    # 'completado'; el presupuesto sólo se agota si la prueba ya iba a fallar.
    for _ in range(60):
        st = client.get(f"/query/{TEST_ID}").json()
        if st["estado"] == "completado":
            break
        time.sleep(1)
    else:
        pytest.fail("La consulta del simulador no se completó a tiempo.")

    resultados = client.get(f"/query/{TEST_ID}?resultados=True").json()["resultados"]
    archivos = resultados["fuentes"]["lustre"]["archivos"] + resultados["fuentes"]["s3"]["archivos"]
    cmip_files = [a for a in archivos if "-L2-CMIP" in a or "-L2-CMIPC" in a]
    assert cmip_files, "No se generaron archivos CMIP"

    # Extraer bandas Cdd del nombre
    bands = set()
    for a in cmip_files:
        m = re.search(r"-M6C(\d{2})_", a)
        if m:
            bands.add(m.group(1))
    # Debe haber 16 bandas (01..16)
    assert bands == {f"{i:02d}" for i in range(1, 17)}, f"Bandas detectadas: {sorted(bands)}"

# ---------------------------------------------------------------------------
# Estados con trabajo en vuelo: 'recibido' y 'procesando'
#
# 'recibido' = encolada y disponible; 'procesando' = alguien la tiene con el
# lease vivo. Los dos protegen el directorio frente a un purge sin force: en
# 'procesando' hay alguien escribiendo ahora mismo, y en 'recibido' el bucle
# puede reclamarla en cualquier momento y recrear lo que se acabe de borrar.
# ---------------------------------------------------------------------------

QUERY_DICT_MINIMO = {
    "satelite": "GOES-16",
    "sensor": "ABI",
    "nivel": "L2",
    "fechas": {"2023299": ["00:00"]},
    "total_horas": 1,
}


@pytest.fixture
def recover_espia(monkeypatch):
    """Sustituye el procesador de fondo por un doble que solo registra llamadas."""
    class _RecoverEspia:
        def __init__(self):
            self.llamadas = []

        def procesar_consulta(self, consulta_id, query_dict):
            self.llamadas.append(consulta_id)

    espia = _RecoverEspia()
    monkeypatch.setattr(main, "recover", espia)
    return espia


def _crear_consulta_en_estado(consulta_id, estado):
    """Inserta una consulta directamente en la DB de prueba con el estado dado."""
    assert main.db.crear_consulta(consulta_id, QUERY_DICT_MINIMO)
    if estado != "recibido":
        main.db.actualizar_estado(consulta_id, estado, 0, "estado de prueba")


def test_restart_acepta_recibido(recover_espia):
    """Una consulta encolada cuya tarea nunca arrancó debe poder reiniciarse.

    Es el caso de uso exacto del endpoint: el servidor se reinició y la tarea
    en memoria se perdió, dejando la consulta congelada en 'recibido'.
    """
    _crear_consulta_en_estado("TEST_RESTART_RECIBIDO", "recibido")
    # Sin latido reciente: la tarea murió hace rato, que es el escenario real.
    _envejecer_latido("TEST_RESTART_RECIBIDO", LEASE_POR_DEFECTO_S + 60)

    response = client.post("/query/TEST_RESTART_RECIBIDO/restart")

    assert response.status_code == 202
    assert response.json()["success"] is True
    assert _esta_encolada("TEST_RESTART_RECIBIDO")


def test_restart_acepta_procesando_error_y_completado(recover_espia):
    """Los tres estados que ya se aceptaban siguen aceptándose."""
    for i, estado in enumerate(("procesando", "error", "completado")):
        consulta_id = f"TEST_RESTART_{estado.upper()}"
        _crear_consulta_en_estado(consulta_id, estado)
        # 'procesando' necesita latido viejo para no chocar con el cerrojo;
        # a 'error' y 'completado' no les aplica.
        _envejecer_latido(consulta_id, LEASE_POR_DEFECTO_S + 60)

        response = client.post(f"/query/{consulta_id}/restart")

        assert response.status_code == 202, f"estado {estado} rechazado"
        assert _esta_encolada(consulta_id)


def test_restart_rechaza_estado_no_reiniciable(recover_espia):
    """Un estado fuera de la lista sigue devolviendo 400 y no encola nada."""
    _crear_consulta_en_estado("TEST_RESTART_RARO", "estado_desconocido")

    response = client.post("/query/TEST_RESTART_RARO/restart")

    assert response.status_code == 400
    assert "estado_desconocido" in response.json()["detail"]
    # Y la fila se queda como estaba, no medio reencolada.
    assert _fila("TEST_RESTART_RARO")["estado"] == "estado_desconocido"


def test_purge_bloqueado_en_recibido():
    """Purgar una consulta encolada sin force debe dar 409.

    Sin esto hay una carrera: se borra el directorio y la tarea arranca justo
    después, lo recrea y lo llena, pero el registro en DB ya no existe.
    """
    _crear_consulta_en_estado("TEST_PURGE_RECIBIDO", "recibido")

    response = client.delete("/query/TEST_PURGE_RECIBIDO?purge=true")

    assert response.status_code == 409
    # El registro sigue en la DB: no se borró nada.
    assert main.db.obtener_consulta("TEST_PURGE_RECIBIDO") is not None


def test_purge_bloqueado_en_procesando():
    """La protección que ya existía para 'procesando' se mantiene."""
    _crear_consulta_en_estado("TEST_PURGE_PROCESANDO", "procesando")

    response = client.delete("/query/TEST_PURGE_PROCESANDO?purge=true")

    assert response.status_code == 409


def test_purge_con_force_procede_en_recibido():
    """force=true sigue siendo la vía de escape para purgar de todas formas."""
    _crear_consulta_en_estado("TEST_PURGE_FORCE", "recibido")
    destino = os.path.join(TEST_DOWNLOAD_PATH, "TEST_PURGE_FORCE")
    os.makedirs(destino, exist_ok=True)

    response = client.delete("/query/TEST_PURGE_FORCE?purge=true&force=true")

    assert response.status_code == 200
    assert not os.path.isdir(destino)
    assert main.db.obtener_consulta("TEST_PURGE_FORCE") is None


def test_purge_no_bloqueado_en_completado():
    """Una consulta terminada no tiene trabajo en vuelo: se purga sin force."""
    _crear_consulta_en_estado("TEST_PURGE_COMPLETADO", "completado")

    response = client.delete("/query/TEST_PURGE_COMPLETADO?purge=true")

    assert response.status_code == 200


# ---------------------------------------------------------------------------
# Cerrojo de reproceso
#
# gunicorn corre con -w 4 y los workers no comparten memoria, así que dos
# peticiones de reinicio se reparten entre procesos distintos y cada uno encola
# su propia tarea sobre el mismo consulta_id. Ocurrió en producción con
# GkpH6xne el 18-ago-2026: dos workers descargando los mismos 2380 archivos en
# paralelo, duplicando cada línea de progreso en el journal.
# ---------------------------------------------------------------------------

def _envejecer_latido(consulta_id, segundos):
    """Retrasa el latido y vence el lease, para simular un consumidor muerto.

    Toca los dos porque son dos épocas del mismo mecanismo: el latido era la
    señal antes de la cola y sigue rescatando huérfanas de entonces; el lease
    es la señal ahora. Una consulta 'procesando' con lease vivo está viva, sin
    importar lo viejo que sea el latido.
    """
    viejo = (datetime.now() - timedelta(seconds=segundos)).isoformat()
    with main.db._connect() as conn:
        conn.execute(
            "UPDATE consultas SET timestamp_actualizacion = ?, lease_hasta = ? WHERE id = ?",
            (viejo, viejo, consulta_id),
        )
        conn.commit()


def _fila(consulta_id):
    """Fila cruda: _row_to_dict no expone las columnas de la cola a propósito."""
    import sqlite3
    with main.db._connect() as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            "SELECT * FROM consultas WHERE id = ?", (consulta_id,)
        ).fetchone()


def _esta_encolada(consulta_id):
    """¿Queda lista para que un consumidor la coja?

    Sustituye a comprobar que el endpoint llamó al pipeline. Desde la entrega 2
    ningún endpoint lanza trabajo: dejan la fila en 'recibido', sin dueño y sin
    aplazar, y el bucle la recoge. Que la fila quede así **es** el contrato.
    """
    fila = _fila(consulta_id)
    return (
        fila is not None
        and fila["estado"] == "recibido"
        and fila["lease_hasta"] is None
        and fila["worker_id"] is None
        and fila["disponible_desde"] is None
    )


def test_restart_dos_veces_encola_una_sola_vez(recover_espia):
    """Dos reinicios seguidos son inocuos, y de ahí sólo sale un consumidor.

    **El invariante cambió con la cola.** Antes el primer reinicio refrescaba el
    latido y el segundo recibía 409. Ahora los dos devuelven 202 —reencolar es
    idempotente, deja la misma fila en 'recibido'— y la exclusión se hace donde
    de verdad importa: al reclamar. Que dos peticiones no acaben en dos
    descargas del mismo archivo sigue siendo el punto; sólo cambió dónde se
    impide, y ahora cubre también a quien no pasó por /restart.
    """
    _crear_consulta_en_estado("TEST_LOCK_DOBLE", "procesando")
    _envejecer_latido("TEST_LOCK_DOBLE", LEASE_POR_DEFECTO_S + 60)

    primera = client.post("/query/TEST_LOCK_DOBLE/restart")
    segunda = client.post("/query/TEST_LOCK_DOBLE/restart")

    assert primera.status_code == 202
    assert segunda.status_code == 202
    assert _esta_encolada("TEST_LOCK_DOBLE")

    # Y de los dos reinicios sale un único consumidor, que es lo que importa.
    assert main.db.reclamar_siguiente("w1") is not None
    assert main.db.reclamar_siguiente("w2") is None


def test_restart_permite_reclamar_tarea_muerta(recover_espia):
    """Una consulta 'procesando' sin latido desde hace rato sí se reclama.

    Es el caso GkpH6xne: el reinicio del servicio se llevó el BackgroundTask y
    la fila quedó congelada. El cerrojo no debe convertir eso en irrecuperable.
    """
    _crear_consulta_en_estado("TEST_LOCK_MUERTA", "procesando")
    _envejecer_latido("TEST_LOCK_MUERTA", LEASE_POR_DEFECTO_S + 3600)

    response = client.post("/query/TEST_LOCK_MUERTA/restart")

    assert response.status_code == 202
    assert _esta_encolada("TEST_LOCK_MUERTA")


def test_restart_bloquea_tarea_viva(recover_espia):
    """Una consulta con el lease vivo no se puede reiniciar.

    Es la protección que impide que un reinicio a destiempo ponga a disposición
    de otro consumidor una consulta que se está descargando ahora mismo. El
    lease lo renueva `actualizar_estado` en cada avance, así que 'procesando'
    recién puesto significa que hay alguien trabajando.
    """
    _crear_consulta_en_estado("TEST_LOCK_VIVA", "procesando")  # lease = ahora + 15 min

    response = client.post("/query/TEST_LOCK_VIVA/restart")

    assert response.status_code == 409
    assert "ya se está procesando" in response.json()["detail"]
    assert _fila("TEST_LOCK_VIVA")["estado"] == "procesando"


def test_restart_de_completado_ignora_el_latido(recover_espia):
    """'completado' y 'error' no tienen trabajo en curso: se reclaman siempre.

    Sin esta excepción, reprocesar algo recién terminado quedaría bloqueado por
    una señal que no representa a nadie trabajando. `actualizar_estado` sólo
    renueva el lease en 'procesando', justo por esto.
    """
    for estado in ("completado", "error"):
        consulta_id = f"TEST_LOCK_{estado.upper()}"
        _crear_consulta_en_estado(consulta_id, estado)  # latido = ahora

        response = client.post(f"/query/{consulta_id}/restart")

        assert response.status_code == 202, f"{estado} quedó bloqueado por el latido"


# ---------------------------------------------------------------------------
# API key
#
# `API_KEY` es opcional: si no se configura, el servicio queda abierto (útil en
# desarrollo, y es como corre hoy en el laboratorio). Lo que estas pruebas fijan
# es que, cuando SÍ está configurada, la protección cubre los tres endpoints que
# comprometen recursos o destruyen datos —crear, reiniciar y borrar— y no las
# lecturas, que el cliente sondea constantemente.
#
# Antes de esto el mecanismo entero estaba sin cobertura, incluidos /restart y
# DELETE, que ya lo usaban.
# ---------------------------------------------------------------------------

CLAVE_DE_PRUEBA = "clave-de-prueba"


@pytest.fixture
def con_api_key(monkeypatch):
    """Configura una API key, como en un despliegue protegido."""
    monkeypatch.setattr(main, "API_KEY", CLAVE_DE_PRUEBA)
    return CLAVE_DE_PRUEBA


def test_crear_consulta_sin_api_key_es_rechazada(con_api_key, recover_espia):
    """POST /query es el endpoint que compromete disco: exige la clave."""
    response = client.post("/query", json=VALID_REQUEST)

    assert response.status_code == 401
    # Y no encoló nada: el rechazo ocurre antes de tocar la DB.
    assert main.db.listar_consultas() == []


def test_crear_consulta_con_api_key_incorrecta_es_rechazada(con_api_key, recover_espia):
    response = client.post(
        "/query", json=VALID_REQUEST, headers={"X-API-Key": "no-es-la-buena"}
    )

    assert response.status_code == 401
    assert main.db.listar_consultas() == []


def test_crear_consulta_con_api_key_correcta_procede(con_api_key, recover_espia, monkeypatch):
    """Con la clave correcta el flujo es exactamente el de siempre.

    Es el caso de historic_query, que manda X-API-Key en todas sus llamadas
    desde un único helper (`call_api` en historic/utils.py).
    """
    monkeypatch.setattr(main, "generar_id_consulta", lambda: "TEST_APIKEY_OK")

    response = client.post(
        "/query", json=VALID_REQUEST, headers={"X-API-Key": CLAVE_DE_PRUEBA}
    )

    assert response.status_code == 202
    assert response.json()["consulta_id"] == "TEST_APIKEY_OK"
    assert _esta_encolada("TEST_APIKEY_OK")


def test_crear_consulta_sin_api_key_configurada_sigue_abierta(recover_espia, monkeypatch):
    """Sin API_KEY configurada no se pide nada: es como corre hoy el servicio."""
    # Explícito y no heredado del entorno: si el .env de quien corre las pruebas
    # tuviera API_KEY, este caso probaría lo contrario de lo que dice su nombre.
    monkeypatch.setattr(main, "API_KEY", None)
    monkeypatch.setattr(main, "generar_id_consulta", lambda: "TEST_APIKEY_ABIERTA")

    response = client.post("/query", json=VALID_REQUEST)

    assert response.status_code == 202
    assert _esta_encolada("TEST_APIKEY_ABIERTA")


def test_restart_exige_api_key(con_api_key, recover_espia):
    """Cobertura del mecanismo tal como ya existía en /restart."""
    _crear_consulta_en_estado("TEST_APIKEY_RESTART", "error")

    sin_clave = client.post("/query/TEST_APIKEY_RESTART/restart")
    assert sin_clave.status_code == 401
    assert _fila("TEST_APIKEY_RESTART")["estado"] == "error"

    con_clave = client.post(
        "/query/TEST_APIKEY_RESTART/restart", headers={"X-API-Key": CLAVE_DE_PRUEBA}
    )
    assert con_clave.status_code == 202
    assert _esta_encolada("TEST_APIKEY_RESTART")


def test_delete_exige_api_key(con_api_key):
    """Cobertura del mecanismo tal como ya existía en DELETE."""
    _crear_consulta_en_estado("TEST_APIKEY_DELETE", "completado")

    sin_clave = client.delete("/query/TEST_APIKEY_DELETE")
    assert sin_clave.status_code == 401
    assert main.db.obtener_consulta("TEST_APIKEY_DELETE") is not None

    con_clave = client.delete(
        "/query/TEST_APIKEY_DELETE", headers={"X-API-Key": CLAVE_DE_PRUEBA}
    )
    assert con_clave.status_code == 200
    assert main.db.obtener_consulta("TEST_APIKEY_DELETE") is None


def test_sondeo_de_estado_no_exige_api_key(con_api_key):
    """Las lecturas siguen abiertas: el cliente las sondea cada pocos segundos."""
    _crear_consulta_en_estado("TEST_APIKEY_GET", "completado")

    assert client.get("/query/TEST_APIKEY_GET").status_code == 200
    assert client.get("/queries").status_code == 200
    assert client.get("/health").status_code in (200, 503)
