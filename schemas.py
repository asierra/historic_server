from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any, Union
from datetime import datetime

class HistoricQueryRequest(BaseModel):
    """
    A generic Pydantic model for historic queries.
    It defines the structure of the request. The specific validation logic
    (e.g., valid satellites, bands) is handled dynamically in the endpoint
    using a configuration class.
    """
    sat: Optional[str] = Field(None, description="Satélite a consultar.")
    sensor: Optional[str] = Field(None, description="Sensor del satélite (ej. abi, suvi, glm).")
    nivel: Optional[str] = Field(None, description="Nivel de procesamiento.")
    dominio: Optional[str] = Field(None, description="Dominio geográfico.")
    productos: Optional[List[str]] = Field(None, description="Lista de productos derivados.")
    bandas: Optional[List[str]] = Field(None, description="Bandas espectrales. Use 'ALL' para todas.")
    fechas: Dict[str, List[str]] = Field(..., description="Fechas con horarios para la consulta.")

    @validator('bandas', pre=True)
    def allow_string_for_all_bands(cls, v):
        """Permite que 'bandas' sea la cadena "ALL" y la convierte en ["ALL"]."""
        if isinstance(v, str) and v.upper() == 'ALL':
            return ['ALL']
        return v


class HistoricQueryResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    total_horas: Optional[float] = None
    total_fechas: Optional[int] = None
    timestamp: datetime
