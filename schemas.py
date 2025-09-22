from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime
from config import SatelliteConfig  # ← Importación centralizada

class HistoricQueryRequest(BaseModel):
    sat: Optional[str] = Field(
        default=SatelliteConfig.DEFAULT_SATELLITE,
        description=f"Satélite. Valores válidos: {SatelliteConfig.VALID_SATELLITES}"
    )
    nivel: str = Field(
        ...,
        description=f"Nivel de procesamiento. Valores válidos: {SatelliteConfig.VALID_LEVELS}"
    )
    dominio: Optional[str] = Field(None, description="Dominio geográfico")
    productos: Optional[List[str]] = Field(None, description="Productos solicitados")
    bandas: Optional[List[str]] = Field(None, description="Bandas espectrales")
    fechas: Dict[str, List[str]] = Field(..., description="Fechas con horarios")
    
    @validator('sat')
    def validate_sat(cls, v):
        if not SatelliteConfig.is_valid_satellite(v):
            raise ValueError(f"Satélite debe ser uno de: {SatelliteConfig.VALID_SATELLITES}")
        return v
    
    @validator('nivel')
    def validate_nivel(cls, v):
        if v not in SatelliteConfig.VALID_LEVELS:
            raise ValueError(f"Nivel debe ser uno de: {SatelliteConfig.VALID_LEVELS}")
        return v
    
    @validator('productos')
    def validate_productos(cls, v):
        if v is not None:
            for producto in v:
                if producto not in SatelliteConfig.VALID_PRODUCTS:
                    raise ValueError(f"Producto inválido: {producto}. Válidos: {SatelliteConfig.VALID_PRODUCTS}")
        return v

    @validator('fechas')
    def validate_fechas(cls, v):
        if not v:
            raise ValueError("Debe haber al menos una fecha")
        return v

class HistoricQueryResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    total_horas: Optional[float] = None
    total_fechas: Optional[int] = None
    timestamp: datetime

class AnalysisResponse(BaseModel):
    satelite: str
    nivel: str
    total_horas: float
    total_fechas: int
    dominio: Optional[str] = None
    productos: Optional[List[str]] = None
    bandas: Optional[List[str]] = None
    distribucion_horaria: Dict[str, float]
    timestamp: datetime