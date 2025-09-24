from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from schemas import HistoricQueryRequest, HistoricQueryResponse, AnalysisResponse
from processors import HistoricQueryProcessor
from config import SatelliteConfigGOES
from datetime import datetime
import uvicorn

app = FastAPI(
    title="Historic Query API - GOES",
    description="API para procesamiento de queries históricas de datos satelitales GOES",
    version="1.0.0"
)

processor = HistoricQueryProcessor()

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

@app.post("/api/query", response_model=HistoricQueryResponse)
async def init_query(request: HistoricQueryRequest):
    """Inicia una query histórica en el servidor de datos"""
    try:
        query = processor.procesar_request(request.dict())
        analisis = processor.generar_analisis(query)
        
        return AnalysisResponse(**analisis)
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error analizando query: {str(e)}"
        )
        
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        workers=4
    )
