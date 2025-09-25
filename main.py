from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from schemas import HistoricQueryRequest, HistoricQueryResponse, AnalysisResponse
from processors import HistoricQueryProcessor
from config import SatelliteConfigGOES
from datetime import datetime
import uvicorn
import json
from typing import Dict, List, Optional
import secrets
import string

from database import ConsultasDatabase

db = ConsultasDatabase()

app = FastAPI(
    title="Historic Query API - GOES",
    description="API para procesamiento de solicitudes históricas de datos satelitales GOES",
    version="1.0.0"
)

processor = HistoricQueryProcessor()

# Simulador temporal en lo que está el real
from background_simulator import BackgroundSimulator
procesador = BackgroundSimulator(db)


@app.get("/api/config")
async def get_config():
    """Devuelve la configuración completa"""
    return {
        "satellites": {
            "validos": SatelliteConfigGOES.VALID_SATELLITES,
            "default": SatelliteConfigGOES.DEFAULT_SATELLITE
        },
        "levels": {
            "validos": SatelliteConfigGOES.VALID_LEVELS,
            "default": SatelliteConfigGOES.DEFAULT_LEVEL
        },
        "domains": {
            "validos": SatelliteConfigGOES.VALID_DOMAINS
        },
        "bandas": {
            "validos": SatelliteConfigGOES.VALID_BANDAS_INCLUDING_ALL,
            "default": SatelliteConfigGOES.DEFAULT_BANDAS,
            "total_bandas": len(SatelliteConfigGOES.VALID_BANDAS)
        },
        "products": {
            "validos": SatelliteConfigGOES.VALID_PRODUCTS
        }
    }

@app.get("/api/config/bandas")
async def get_bandas_config():
    """Devuelve configuración de bandas"""
    return {
        "bandas_validas": SatelliteConfigGOES.VALID_BANDAS_INCLUDING_ALL,
        "bandas_por_defecto": SatelliteConfigGOES.DEFAULT_BANDAS,
        "banda_all": SatelliteConfigGOES.ALL_BANDAS,
        "total_bandas": len(SatelliteConfigGOES.VALID_BANDAS)
    }

@app.post("/api/validate", response_model=HistoricQueryResponse)
async def validate_query(request: HistoricQueryRequest):
    """Valida y procesa una query histórica"""
    try:
        query = processor.procesar_request(request.dict())
        
        return HistoricQueryResponse(
            success=True,
            message="Query válida y procesada exitosamente",
            data=request.dict(),
            total_horas=query.total_horas,
            total_fechas=query.total_fechas,
            timestamp=datetime.now()
        )
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error procesando query: {str(e)}")


@app.get("/")
async def root():
    return {"message": "Historic Query API", "status": "active"}


@app.post("/api/analyze", response_model=AnalysisResponse)
async def analyze_query(request: HistoricQueryRequest):
    """Analiza una query histórica y devuelve estadísticas"""
    try:
        query = processor.procesar_request(request.dict())
        analisis = processor.generar_analisis(query)
        
        return AnalysisResponse(**analisis)
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error analizando query: {str(e)}"
        )


@app.get("/api/satelites")
async def get_satelites():
    """Devuelve la lista de satélites válidos"""
    return {
        "satelites": SatelliteConfigGOES.VALID_SATELLITES,
        "default": "GOES-EAST"
    }


def generar_id_consulta() -> str:
    """Genera un ID único de 8 caracteres para la consulta"""
    return ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(8))


@app.post("/api/query")
async def crear_consulta(
    request: HistoricQueryRequest,
    background_tasks: BackgroundTasks
):
    """
    Crea una nueva consulta histórica y devuelve un ID único para seguimiento
    """
    try:
        # Procesar y convertir a dict en un solo paso
        query_obj = processor.procesar_request(request.dict())
        query_dict = query_obj.to_dict()  # ← Única versión necesaria
        
        # Generar ID único
        consulta_id = generar_id_consulta()
        
        # Guardar en SQLite (solo la query procesada)
        if not db.crear_consulta(consulta_id, query_dict):
            raise HTTPException(status_code=500, detail="Error almacenando consulta")
        
        # Iniciar procesamiento
        background_tasks.add_task(procesador.procesar_consulta, consulta_id, query_dict)

        return {
            "success": True,
            "message": "Consulta creada exitosamente",
            "consulta_id": consulta_id,
            "estado": "recibido",
            "timestamp": datetime.now().isoformat(),
            "resumen": {
                "satelite": query_dict['satelite'],
                "nivel": query_dict['nivel'],
                "total_fechas": query_dict['total_fechas_expandidas'],
                "total_horas": query_dict['total_horas'],
                "bandas": len(query_dict['bandas']),
                "productos": len(query_dict['productos']) if query_dict.get('productos') else 0
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error creando consulta: {str(e)}")

@app.post("/api/query/{consulta_id}/simular-error")
async def simular_error_consulta(consulta_id: str, background_tasks: BackgroundTasks):
    """Simula un error en el procesamiento (solo para testing)"""
    background_tasks.add_task(procesador.simular_error, consulta_id)
    return {"message": "Simulación de error iniciada"}


@app.get("/api/procesador/estadisticas")
async def obtener_estadisticas_procesador():
    """Mismo endpoint para ambos procesadores"""
    # ✅ MISMA LÍNEA PARA AMBOS
    return procesador.obtener_estadisticas()


@app.get("/api/query/{consulta_id}")
async def obtener_estado_consulta(consulta_id: str):
    """Obtiene el estado de una consulta (versión simplificada)"""
    consulta = db.obtener_consulta(consulta_id)
    if not consulta:
        raise HTTPException(status_code=404, detail="Consulta no encontrada")
    
    # Respuesta más limpia
    return {
        "consulta_id": consulta_id,
        "estado": consulta["estado"],
        "progreso": consulta["progreso"],
        "mensaje": consulta["mensaje"],
        "resumen": {
            "satelite": consulta["query"]["satelite"],
            "nivel": consulta["query"]["nivel"],
            "total_fechas": consulta["query"]["total_fechas_expandidas"],
            "fechas_ejemplo": list(consulta["query"]["fechas"].keys())[:3]  # Primeras 3
        },
        "timestamp_creacion": consulta["timestamp_creacion"],
        "timestamp_actualizacion": consulta["timestamp_actualizacion"]
    }


@app.get("/api/query/{consulta_id}/resultados")
async def obtener_resultados_consulta(consulta_id: str):
    """
    Obtiene los resultados completos de una consulta completada
    """
    if consulta_id not in consultas_almacenadas:
        raise HTTPException(status_code=404, detail="Consulta no encontrada")
    
    if consulta_id not in RESULTADOS_TEMPORALES:
        raise HTTPException(status_code=404, detail="Resultados no disponibles")
    
    resultados = RESULTADOS_TEMPORALES[consulta_id]
    
    if resultados["estado"] != "completado":
        raise HTTPException(
            status_code=400, 
            detail=f"Consulta aún en procesamiento. Estado: {resultados['estado']}"
        )
    
    return {
        "consulta_id": consulta_id,
        "estado": "completado",
        "query_original": consultas_almacenadas[consulta_id]["query_original"],
        "resultados": resultados["resultados"],
        "timestamp_completado": resultados["timestamp"]
    }

@app.get("/api/queries")
async def listar_consultas(estado: Optional[str] = None):
    """
    Lista todas las consultas (opcionalmente filtradas por estado)
    """
    consultas = []
    
    for consulta_id, info in consultas_almacenadas.items():
        estado_actual = "en_cola"
        if consulta_id in RESULTADOS_TEMPORALES:
            estado_actual = RESULTADOS_TEMPORALES[consulta_id]["estado"]
        
        if estado and estado_actual != estado:
            continue
            
        consultas.append({
            "id": consulta_id,
            "estado": estado_actual,
            "satelite": info["query_procesada"]["satelite"],
            "nivel": info["query_procesada"]["nivel"],
            "timestamp_creacion": info["timestamp_creacion"],
            "total_fechas": info["query_procesada"]["total_fechas_expandidas"]
        })
    
    return {
        "total_consultas": len(consultas),
        "consultas": sorted(consultas, key=lambda x: x["timestamp_creacion"], reverse=True)
    }

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        workers=4
    )
