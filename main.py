from fastapi import FastAPI, HTTPException, BackgroundTasks, Body
from database import ConsultasDatabase, DATABASE_PATH
from background_simulator import BackgroundSimulator
from processors import HistoricQueryProcessor
from schemas import HistoricQueryRequest
from datetime import datetime
from typing import Dict, Any
from pydantic import ValidationError
from config import SatelliteConfigGOES
import uvicorn
import secrets
import string

app = FastAPI(
    title="LANOT Historic Request",
    description="API para solicitudes históricas de datos del LANOT",
    version="1.0.0"
)

# Registro de configuraciones de satélites disponibles
# A medida que agregues soporte para más satélites, importa su config y añádela aquí.
AVAILABLE_SATELLITE_CONFIGS = {
    "GOES": SatelliteConfigGOES(),
}

# Inicializar componentes
db = ConsultasDatabase(db_path=DATABASE_PATH)
processor = HistoricQueryProcessor()
recover = BackgroundSimulator(db)


def generar_id_consulta() -> str:
    return ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(8))


@app.get("/")
async def health_check():
    """Health check"""
    return {"status": "active", "timestamp": datetime.now().isoformat()}

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
    data['nivel'] = request.nivel or config.DEFAULT_LEVEL
    data['bandas'] = request.bandas or config.DEFAULT_BANDAS

    if not config.is_valid_satellite(data['sat']):
        raise ValueError(f"Satélite debe ser uno de: {config.VALID_SATELLITES}")
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
