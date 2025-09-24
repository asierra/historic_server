from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime
from config import SatelliteConfigGOES

class HistoricQueryRequest(BaseModel):
    sat: Optional[str] = Field(
        default=SatelliteConfigGOES.DEFAULT_SATELLITE,
        description=f"Satélite. Valores válidos: {SatelliteConfigGOES.VALID_SATELLITES}"
    )
    nivel: str = Field(
        default=SatelliteConfigGOES.DEFAULT_LEVEL,
        description=f"Nivel de procesamiento. Valores válidos: {SatelliteConfigGOES.VALID_LEVELS}"
    )
    dominio: Optional[str] = Field(
        None, 
        description=f"Dominio geográfico. Valores válidos: {SatelliteConfigGOES.VALID_DOMAINS}"
    )
    productos: Optional[List[str]] = Field(
        None, 
        description=f"Productos solicitados. Valores válidos: {SatelliteConfigGOES.VALID_PRODUCTS}"
    )
    bandas: Optional[List[str]] = Field(
        default=SatelliteConfigGOES.DEFAULT_BANDAS,
        description=f"Bandas espectrales. Valores válidos: {SatelliteConfigGOES.VALID_BANDAS_INCLUDING_ALL}. Use 'ALL' para todas las bandas."
    )
    fechas: Dict[str, List[str]] = Field(..., description="Fechas con horarios")
    
    @validator('sat')
    def validate_sat(cls, v):
        if not SatelliteConfigGOES.is_valid_satellite(v):
            raise ValueError(f"Satélite debe ser uno de: {SatelliteConfigGOES.VALID_SATELLITES}")
        return v
    
    @validator('nivel')
    def validate_nivel(cls, v):
        if not SatelliteConfigGOES.is_valid_level(v):
            raise ValueError(f"Nivel debe ser uno de: {SatelliteConfigGOES.VALID_LEVELS}")
        return v
    
    @validator('dominio')
    def validate_dominio(cls, v):
        if v is not None and not SatelliteConfigGOES.is_valid_domain(v):
            raise ValueError(f"Dominio debe ser uno de: {SatelliteConfigGOES.VALID_DOMAINS}")
        return v
    
    @validator('bandas')
    def validate_bandas(cls, v):
	    """Valida bandas y DEVUELVE ERROR si hay bandas inválidas"""
	    if v is None:
	        return SatelliteConfigGOES.DEFAULT_BANDAS
	    
	    try:
	        bandas_validadas = SatelliteConfigGOES.validate_bandas(v)
	        return bandas_validadas
	    except ValueError as e:
	        raise ValueError(str(e))
    
    @validator('productos')
    def validate_productos(cls, v):
        if v is not None:
            for producto in v:
                if producto not in SatelliteConfigGOES.VALID_PRODUCTS:
                    raise ValueError(f"Producto inválido: {producto}. Válidos: {SatelliteConfigGOES.VALID_PRODUCTS}")
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
