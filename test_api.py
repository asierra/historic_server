import pytest
from fastapi.testclient import TestClient
import main  # Importamos el módulo principal
from collections import namedtuple
import shutil
import time
import re
from background_simulator import BackgroundSimulator
from database import ConsultasDatabase
import os
from recover import RecoverFiles  # Importar el procesador real para la prueba de integración

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
    
    # Establecer variables de entorno para el simulador para evitar errores de inicialización
    monkeypatch.setenv("SIM_LOCAL_SUCCESS_RATE", "0.9")
    monkeypatch.setenv("SIM_S3_SUCCESS_RATE", "0.8")

    # Crear directorio de descarga para las pruebas y configurar la variable de entorno
    os.makedirs(TEST_DOWNLOAD_PATH, exist_ok=True)
    monkeypatch.setattr(main, "DOWNLOAD_PATH", TEST_DOWNLOAD_PATH)

    
    # 2. Reemplazar los objetos globales en main.py
    monkeypatch.setattr(main, "db", test_db)
    monkeypatch.setattr(main, "recover", BackgroundSimulator(test_db))
    # 3. Desactivar el apagado del executor para evitar errores en las pruebas
    
    try:
        yield  # Aquí es donde se ejecuta la prueba
    finally:
        # 3. Limpiar la base de datos después de la prueba
        if os.path.exists(TEST_DB_PATH):
            os.remove(TEST_DB_PATH)
        if os.path.exists(TEST_DOWNLOAD_PATH):
            shutil.rmtree(TEST_DOWNLOAD_PATH)

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
    # Reemplazamos la función que genera IDs para usar uno predecible
    monkeypatch.setattr("main.generar_id_consulta", lambda: "TEST_SUCCESS")

    response = client.post("/query", json=VALID_REQUEST)
    assert response.status_code == 202
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

def test_query_and_get_status(monkeypatch):
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
    for _ in range(10): # Intentar por un máximo de 10 segundos
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

def test_recovery_query_is_generated_on_failure(monkeypatch):
    """
    Verifica que se genera una 'consulta_recuperacion' cuando el simulador
    fuerza un fallo en la recuperación de archivos.
    """
    TEST_ID = "TEST_RECOVERY_QUERY"
    monkeypatch.setattr("main.generar_id_consulta", lambda: TEST_ID)

    # Forzar fallos en el simulador para que falle tanto en local como en S3.
    # Esto se hace configurando las tasas de éxito a 0 a través de variables de entorno.
    monkeypatch.setenv("SIM_LOCAL_SUCCESS_RATE", "0.0")
    monkeypatch.setenv("SIM_S3_SUCCESS_RATE", "0.0")
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
    
    # Verificar que la consulta de recuperación es precisa
    rec_query = resultados["consulta_recuperacion"]
    assert "20231026" in rec_query["fechas"]
    assert rec_query["fechas"]["20231026"] == ["12:00"]

def test_simulator_report_has_correct_sources_structure(monkeypatch):
    """
    Verifica que el reporte final del simulador tiene la estructura correcta
    de 'fuentes' (lustre y s3), que implementamos anteriormente.
    """
    TEST_ID = "TEST_SOURCES_STRUCTURE"
    monkeypatch.setattr("main.generar_id_consulta", lambda: TEST_ID)

    # 1. Crear la consulta usando una solicitud válida
    create_response = client.post("/query", json=VALID_REQUEST)
    assert create_response.status_code == 202

    # 2. Esperar a que el simulador complete el trabajo
    for _ in range(10):
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
def test_s3_fallback_integration(real_io_fixture, monkeypatch):
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
def test_s3_fallback_integration_l2_multi_product(real_io_fixture, monkeypatch):
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

def test_complex_query_does_not_get_stuck(monkeypatch):
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

    # 1. Crear la consulta
    create_response = client.post("/query", json=complex_request)
    assert create_response.status_code == 202

    # 2. Monitorear hasta que se complete, con un timeout generoso
    # Si el proceso se atora, este bucle fallará por timeout.
    timeout = 20  # segundos
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

def test_simulator_l2_cmip_respects_requested_band(monkeypatch):
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

    for _ in range(15):
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

def test_l2_cmip_without_bandas_expands_to_all(monkeypatch):
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

    for _ in range(15):
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