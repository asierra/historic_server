from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime

class HistoricQueryRequest(BaseModel):
    """
    Modelo Pydantic para las solicitudes de consultas históricas.
    Define la estructura y los tipos de datos esperados en el JSON de entrada.
    """
    sat: Optional[str] = Field(None, description="Satélite a consultar, p.ej., 'GOES-16'. Si es nulo, se usa el default de la configuración.")
    sensor: Optional[str] = Field(None, description="Sensor a consultar, p.ej., 'abi'.")
    nivel: Optional[str] = Field(None, description="Nivel de procesamiento, p.ej., 'L1b' o 'L2'.")
    bandas: Optional[List[str]] = Field(None, description="Lista de bandas a recuperar, p.ej., ['02', '13'] o ['ALL'].")
    productos: Optional[List[str]] = Field(None, description="Lista de productos L2 a recuperar, p.ej., ['ACHA', 'CMIP'] o ['ALL'].")
    dominio: str = Field(..., description="Dominio geográfico, p.ej., 'fd', 'conus'.")
    fechas: Dict[str, List[str]] = Field(..., description="Diccionario de fechas y rangos horarios.")
    creado_por: Optional[str] = Field(None, description="Email o identificador del usuario que crea la solicitud.")


class HistoricQueryResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    total_horas: Optional[float] = None
    total_fechas: Optional[int] = None
    timestamp: datetime
