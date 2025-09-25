from datetime import datetime, time, timedelta
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from config import SatelliteConfigGOES

@dataclass
class Horario:
    inicio: time
    fin: time
    duracion_horas: float = field(init=False)
    
    def __post_init__(self):
        inicio_dt = datetime.combine(datetime.today(), self.inicio)
        fin_dt = datetime.combine(datetime.today(), self.fin)
        self.duracion_horas = (fin_dt - inicio_dt).total_seconds() / 3600

@dataclass
class Fecha:
    fecha: str
    horarios: List[Horario]
    total_horas: float = field(init=False)
    
    def __post_init__(self):
        self.total_horas = sum(horario.duracion_horas for horario in self.horarios)
    
    # ✅ AGREGAR ESTOS MÉTODOS NUEVOS:
    def es_intervalo(self) -> bool:
        """Determina si la fecha es un intervalo"""
        return '-' in self.fecha
    
    def expandir_fechas(self) -> List[str]:
        """Expande un intervalo de fechas a fechas individuales"""
        if not self.es_intervalo():
            return [self.fecha]
        
        fecha_inicio_str, fecha_fin_str = self.fecha.split('-')
        fecha_inicio = datetime.strptime(fecha_inicio_str, "%Y%m%d")
        fecha_fin = datetime.strptime(fecha_fin_str, "%Y%m%d")
        
        fechas_expandidas = []
        current_date = fecha_inicio
        while current_date <= fecha_fin:
            fechas_expandidas.append(current_date.strftime("%Y%m%d"))
            current_date += timedelta(days=1)
        
        return fechas_expandidas
    
    def obtener_horarios_str(self) -> List[str]:
        """Convierte horarios a formato string HH:mm-HH:mm"""
        horarios_str = []
        for horario in self.horarios:
            if horario.inicio == horario.fin:
                # Horario individual
                horarios_str.append(horario.inicio.strftime("%H:%M"))
            else:
                # Intervalo de horarios
                horarios_str.append(
                    f"{horario.inicio.strftime('%H:%M')}-{horario.fin.strftime('%H:%M')}"
                )
        return horarios_str

@dataclass
class HistoricQuery:
    satelite: str
    nivel: str
    fechas: List[Fecha]
    dominio: Optional[str] = None
    productos: Optional[List[str]] = None
    bandas: Optional[List[str]] = None
    total_horas: float = field(init=False)
    total_fechas: int = field(init=False)
    
    def __post_init__(self):
        self.total_horas = sum(fecha.total_horas for fecha in self.fechas)
        self.total_fechas = len(self.fechas)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convierte a diccionario expandiendo intervalos de fechas
        """
        fechas_dict = {}
        
        for fecha_obj in self.fechas:
            fechas_individuales = fecha_obj.expandir_fechas()
            horarios_str = fecha_obj.obtener_horarios_str()
            
            for fecha_str in fechas_individuales:
                fechas_dict[fecha_str] = horarios_str.copy()
        
        return {
            'satelite': self.satelite,
            'nivel': self.nivel,
            'dominio': self.dominio,
            'productos': self.productos,
            'bandas': self.bandas,
            'fechas': fechas_dict,
            'total_horas': self.total_horas,
            'total_fechas_expandidas': len(fechas_dict)
        }
 
    
    def obtener_fechas_individuales(self) -> List[str]:
        """Devuelve todas las fechas individuales (expandidas)"""
        fechas_individuales = []
        for fecha_obj in self.fechas:
            fechas_individuales.extend(fecha_obj.expandir_fechas())
        return sorted(fechas_individuales)
    
    def contar_fechas_reales(self) -> int:
        """Cuenta fechas individuales después de expandir intervalos"""
        return len(self.obtener_fechas_individuales())


class HistoricQueryProcessor:
    def __init__(self):
        self.satellites_validos = SatelliteConfigGOES.VALID_SATELLITES
        self.default_satellite = SatelliteConfigGOES.DEFAULT_SATELLITE
        self.levels_validos = SatelliteConfigGOES.VALID_LEVELS
    
    def procesar_request(self, request_data: Dict) -> HistoricQuery:
        """Convierte JSON request a estructura de datos"""
        def parsear_horario(horario_str: str) -> Horario:
            if '-' in horario_str:
                inicio_str, fin_str = horario_str.split('-')
                inicio = datetime.strptime(inicio_str, "%H:%M").time()
                fin = datetime.strptime(fin_str, "%H:%M").time()
                return Horario(inicio, fin)
            else:
                tiempo = datetime.strptime(horario_str, "%H:%M").time()
                return Horario(tiempo, tiempo)
        
        # Convertir fechas
        fechas = []
        for fecha_str, horarios_str in request_data['fechas'].items():
            horarios = [parsear_horario(h) for h in horarios_str]
            fechas.append(Fecha(fecha_str, horarios))
        
        # Validar y expandir bandas si es necesario
        bandas = request_data.get('bandas', SatelliteConfigGOES.DEFAULT_BANDAS)
        bandas_expandidas = SatelliteConfigGOES.expand_bandas(bandas)
        
        return HistoricQuery(
            satelite=request_data.get('sat', self.default_satellite),
            nivel=request_data.get('nivel', SatelliteConfigGOES.DEFAULT_LEVEL),
            fechas=fechas,
            dominio=request_data.get('dominio'),
            productos=request_data.get('productos'),
            bandas=bandas_expandidas
        )
        
    def generar_analisis(self, query: HistoricQuery) -> Dict:
        """Genera análisis de la query"""
        from collections import defaultdict

        # Distribución horaria
        distribucion = defaultdict(float)
        for fecha in query.fechas:
            for horario in fecha.horarios:
                hora_inicio = horario.inicio.hour
                rango = f"{hora_inicio:02d}:00-{(hora_inicio + 1) % 24:02d}:00"
                distribucion[rango] += horario.duracion_horas

        return {
            'satelite': query.satelite,
            'nivel': query.nivel,
            'total_horas': round(query.total_horas, 2),
            'total_fechas': query.total_fechas,
            'dominio': query.dominio,
            'productos': query.productos,
            'bandas': query.bandas,
            'distribucion_horaria': dict(sorted(distribucion.items())),
            'timestamp': datetime.now()
        }
	
