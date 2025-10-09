from pydantic import BaseModel, Field, field_validator
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
    dominio: str = Field(..., description="Dominio geográfico (ej. fd, conus).")
    productos: Optional[Union[List[str], str]] = Field(None, description="Lista de productos derivados. Use 'ALL' para todos.")
    bandas: Optional[Union[List[str], str]] = Field(None, description="Bandas espectrales. Use 'ALL' para todas.")
    fechas: Dict[str, List[str]] = Field(..., description="Fechas con horarios para la consulta.")

    @field_validator('bandas', mode='before')
    def allow_string_for_all_bands(cls, v):
        """Permite que 'bandas' sea la cadena "ALL" y la convierte en ["ALL"]."""
        if isinstance(v, str):
            if v.upper() == 'ALL':
                return ['ALL']
            raise TypeError(f"bandas debe ser una lista de strings o 'ALL', no '{v}'")
        if isinstance(v, list):
            return v
        if v is None:
            return None
        raise TypeError(f"bandas debe ser una lista de strings o 'ALL', no '{v}'")

    @field_validator('productos', mode='before')
    def allow_string_for_all_productos(cls, v):
        """Permite que 'productos' sea la cadena "ALL" y la convierte en ["ALL"]."""
        if isinstance(v, str):
            if v.upper() == 'ALL':
                return ['ALL']
            raise TypeError(f"productos debe ser una lista de strings o 'ALL', no '{v}'")
        if isinstance(v, list):
            return v
        if v is None:
            return None
        raise TypeError(f"productos debe ser una lista de strings o 'ALL', no '{v}'")


class HistoricQueryResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    total_horas: Optional[float] = None
    total_fechas: Optional[int] = None
    timestamp: datetime
