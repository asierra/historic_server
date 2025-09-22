from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from schemas import HistoricQueryRequest, HistoricQueryResponse, AnalysisResponse
from processors import HistoricQueryProcessor
from config import SatelliteConfig  # ← Importación centralizada
from datetime import datetime
import uvicorn

app = FastAPI(
    title="Historic Query API",
    description="API para procesamiento de queries históricas de datos satelitales",
    version="1.0.0"
)

processor = HistoricQueryProcessor()

@app.get("/api/config/satelites")
async def get_satelites_config():
    """Devuelve la configuración completa de satélites"""
    return {
        "satelites_validos": SatelliteConfig.VALID_SATELLITES,
        "satelite_por_defecto": SatelliteConfig.DEFAULT_SATELLITE,
        "niveles_validos": SatelliteConfig.VALID_LEVELS,
        "productos_validos": SatelliteConfig.VALID_PRODUCTS,
        "metadata": SatelliteConfig.SATELLITE_METADATA
    }

@app.get("/api/config/satelites/validos")
async def get_satelites_validos():
    """Devuelve solo la lista de satélites válidos"""
    return {
        "satelites": SatelliteConfig.VALID_SATELLITES,
        "default": SatelliteConfig.DEFAULT_SATELLITE
    }

# Los demás endpoints usan los esquemas que ya importan la configuración centralizada
@app.get("/")
async def root():
    return {"message": "Historic Query API", "status": "active"}

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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error procesando query: {str(e)}"
        )

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
        "satelites": SatelliteConfig.VALID_SATELLITES,
        "default": "GOES-EAST"
    }

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        workers=4
    )