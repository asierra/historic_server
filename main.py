from fastapi import FastAPI, HTTPException, BackgroundTasks, Body
from database import ConsultasDatabase, DATABASE_PATH
from background_simulator import BackgroundSimulator
from recover import RecoverFiles # Importar el procesador real
from processors import HistoricQueryProcessor
from schemas import HistoricQueryRequest
from datetime import datetime
from typing import Dict, Any
import logging
from pydantic import ValidationError
from config import SatelliteConfigGOES
import uvicorn
import os # Importar os para leer variables de entorno
import secrets
import string

# --- Configuración de Logging ---
# Configura el logging para escribir en un archivo en un entorno de producción.
# En un entorno real, podrías usar una configuración más avanzada (ej. JSON, rotación de archivos).
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler() # Escribe a la consola. Reemplazar con logging.FileHandler("app.log") para producción.
    ]
)
app = FastAPI(
    title="LANOT Historic Request",
    description="API para solicitudes de datos históricos del LANOT",
    version="1.0.0"
)

# Registro de configuraciones de satélites disponibles
# A medida que agregues soporte para más satélites, importa su config y añádela aquí.
AVAILABLE_SATELLITE_CONFIGS = {
    "GOES": SatelliteConfigGOES(),
}

# --- Configuración y Componentes Dinámicos ---

# Usar variables de entorno para configurar rutas clave
DB_PATH = os.getenv("HISTORIC_DB_PATH", DATABASE_PATH)
SOURCE_DATA_PATH = os.getenv("HISTORIC_SOURCE_PATH", "/depot/goes16")
DOWNLOAD_PATH = os.getenv("HISTORIC_DOWNLOAD_PATH", "/data/tmp")

# Selección del procesador de background mediante variable de entorno
PROCESSOR_MODE = os.getenv("PROCESSOR_MODE", "real") # 'real' o 'simulador'

# Inicializar componentes
db = ConsultasDatabase(db_path=DB_PATH)
processor = HistoricQueryProcessor()

# Instanciar el procesador de background según el modo
recover = RecoverFiles(db, source_data_path=SOURCE_DATA_PATH, base_download_path=DOWNLOAD_PATH, max_workers=int(os.getenv("HISTORIC_MAX_WORKERS", "8"))) if PROCESSOR_MODE == "real" else BackgroundSimulator(db)


def generar_id_consulta() -> str:
    return ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(8))


@app.get("/")
async def health_check():
    """Health check"""
    return {"status": "active", "timestamp": datetime.now().isoformat()}

@app.get("/health")
async def health_check_detailed():
    """
    Verifica la salud de la aplicación y sus dependencias clave.
    """
    db_status = "ok"
    storage_status = "ok"
    overall_status = "ok"
    
    # 1. Verificar la conexión a la base de datos
    try:
        db.listar_consultas(limite=1) # Intenta una operación simple
    except Exception as e:
        db_status = f"error: {e}"
        overall_status = "error"

    # 2. Verificar que el almacenamiento primario (Lustre) esté accesible
    if not os.path.exists(SOURCE_DATA_PATH):
        storage_status = f"error: La ruta de origen '{SOURCE_DATA_PATH}' no existe o no es accesible."
        overall_status = "error"

    status_code = 200 if overall_status == "ok" else 503 # Service Unavailable
    
    return {"status": overall_status, "database": db_status, "storage": storage_status}


def _validate_and_prepare_request(request_data: Dict[str, Any]) -> (Dict[str, Any], Any):
    """
    Función de ayuda reutilizable para validar y preparar una solicitud.
    Levanta HTTPException en caso de error.
    Devuelve (datos_validados, clase_de_configuracion).
    """
    # 1. Validar la estructura básica con Pydantic
    try:
        request = HistoricQueryRequest(**request_data)
    except ValidationError as e:
        # Pydantic genera errores detallados, los pasamos directamente.
        raise HTTPException(status_code=422, detail=e.errors())

    # 2. Determinar la configuración correcta (ej. por prefijo del satélite)
    sat_name = request.sat or AVAILABLE_SATELLITE_CONFIGS["GOES"].DEFAULT_SATELLITE
    config = None
    if sat_name.startswith("GOES"):
        config = AVAILABLE_SATELLITE_CONFIGS["GOES"]
    # Agrega aquí lógica para otros satélites:
    # elif sat_name.startswith("JPSS"):
    #     config = AVAILABLE_SATELLITE_CONFIGS["JPSS"]

    if not config:
        raise HTTPException(status_code=400, detail=f"Satélite '{sat_name}' no es soportado o es inválido.")

    # 3. Aplicar valores por defecto y validar con la configuración específica
    data = request.model_dump()
    data['sat'] = sat_name
    data['sensor'] = request.sensor or config.DEFAULT_SENSOR
    data['nivel'] = request.nivel or config.DEFAULT_LEVEL
    data['bandas'] = request.bandas or config.DEFAULT_BANDAS

    if not config.is_valid_satellite(data['sat']):
        raise ValueError(f"Satélite debe ser uno de: {config.VALID_SATELLITES}")
    if not config.is_valid_sensor(data['sensor']):
        raise ValueError(f"Sensor debe ser uno de: {config.VALID_SENSORS}")
    if not config.is_valid_level(data['nivel']):
        raise ValueError(f"Nivel debe ser uno de: {config.VALID_LEVELS}")
    if data['dominio'] and not config.is_valid_domain(data['dominio']):
        raise ValueError(f"Dominio debe ser uno de: {config.VALID_DOMAINS}")
    
    data['bandas'] = config.validate_bandas(data['bandas'])
    return data, config

@app.post("/query")
async def crear_solicitud(
    background_tasks: BackgroundTasks,
    request_data: Dict[str, Any] = Body(...),
):
    """
    ✅ ENDPOINT PRINCIPAL: Crear y procesar solicitud
    """
    try:
        # 1. Validar y preparar la solicitud usando la función de ayuda
        data, config = _validate_and_prepare_request(request_data)
        # 2. Procesar la solicitud ya validada y completada
        query_obj = processor.procesar_request(data, config)
        query_dict = query_obj.to_dict()
        
        consulta_id = generar_id_consulta()
        
        if not db.crear_consulta(consulta_id, query_dict):
            raise HTTPException(status_code=500, detail="Error almacenando consulta")
        
        # Procesar en background
        background_tasks.add_task(recover.procesar_consulta, consulta_id, query_dict)
        
        return {
            "success": True,
            "consulta_id": consulta_id,
            "estado": "recibido",
            "resumen": {
                "satelite": query_dict['satelite'],
                "sensor": query_dict['sensor'],
                "nivel": query_dict['nivel'],
                "fechas": len(query_dict['fechas']),
                "horas": query_dict['total_horas']
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/validate")
async def validar_solicitud(request_data: Dict[str, Any] = Body(...)):
    """
    ✅ ENDPOINT DE VALIDACIÓN: Valida el JSON de entrada sin crear una consulta.
    Devuelve un resumen si es válido, o un error detallado si no lo es.
    """
    try:
        # 1. Validar y preparar la solicitud usando la función de ayuda
        data, config = _validate_and_prepare_request(request_data)

        # 2. Procesar para obtener el resumen (sin guardar en DB)
        query_obj = processor.procesar_request(data, config)
        query_dict = query_obj.to_dict() # Mantenemos to_dict si es un método custom de la dataclass

        return {
            "success": True,
            "message": "La solicitud es válida.",
            "resumen_solicitud": {
                "satelite": query_dict['satelite'],
                "sensor": query_dict['sensor'],
                "nivel": query_dict['nivel'],
                "total_fechas_expandidas": query_dict['total_fechas_expandidas'],
                "total_horas": query_dict['total_horas'],
                "bandas_procesadas": query_dict['bandas']
            }
        }
    except ValueError as e:
        # Convertir errores de validación de lógica de negocio en 400
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException as e:
        # Relanzar excepciones HTTP que ya vienen preparadas (ej. 422 de Pydantic)
        raise e

@app.get("/query/{consulta_id}")
async def obtener_consulta(
    consulta_id: str,
    resultados: bool = False,
):
    """
    ✅ ENDPOINT ÚNICO PARA CONSULTAR: Estado y resultados
    Reemplaza a: /api/query/{id}, /api/query/{id}/resultados, /api/queries
    """
    consulta = db.obtener_consulta(consulta_id)
    if not consulta:
        raise HTTPException(status_code=404, detail="Consulta no encontrada")
    
    # Si se piden resultados específicos y la consulta está completada
    if resultados and consulta["estado"] == "completado" and consulta.get("resultados"):
        return {
            "consulta_id": consulta_id,
            "estado": "completado",
            "resultados": consulta["resultados"]
        }
    
    # Estado normal de la consulta
    return {
        "consulta_id": consulta_id,
        "estado": consulta["estado"],
        "progreso": consulta["progreso"],
        "mensaje": consulta["mensaje"],
        "timestamp": consulta["timestamp_actualizacion"],
        "query": consulta["query"] if consulta["estado"] == "recibido" else None
    }

@app.get("/queries")
async def listar_consultas(
    estado: str = None,
    limite: int = 20,
):
    """
    ✅ LISTADO SIMPLE: Para monitoreo
    """
    consultas = db.listar_consultas(estado=estado, limite=limite)
    
    # Formato mínimo para listado
    consultas_simples = []
    for c in consultas:
        consultas_simples.append({
            "id": c["id"],
            "estado": c["estado"],
            "progreso": c["progreso"],
            "satelite": c["query"]["satelite"],
            "timestamp": c["timestamp_creacion"]
        })
    
    return {
        "total": len(consultas_simples),
        "consultas": consultas_simples
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
