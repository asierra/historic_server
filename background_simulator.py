import time
import logging
import random
from datetime import datetime
from typing import Dict
from database import ConsultasDatabase


class BackgroundSimulator():
    """
    Simulador que implementa la misma interfaz que el procesador real
    """
    
    def __init__(self, db: ConsultasDatabase):
        self.db = db
        self.nombre = "simulador"
        self.etapas_simuladas = {
            "rapido": [
                (10, "Validando parámetros...", 1),
                (40, "Consultando base de datos satelital...", 2),
                (70, "Procesando datos...", 3),
                (90, "Generando resultados...", 2),
                (100, "Completado", 1)
            ],
            "normal": [
                (5, "Iniciando validación...", 2),
                (15, "Verificando disponibilidad de datos...", 3),
                (30, "Descargando metadatos...", 4),
                (50, "Procesando bandas espectrales...", 5),
                (70, "Generando productos derivados...", 4),
                (85, "Comprimiendo resultados...", 3),
                (95, "Finalizando...", 2),
                (100, "Procesamiento completado", 1)
            ],
            "lento": [
                (5, "Preparando entorno de procesamiento...", 3),
                (15, "Validando grandes volúmenes de datos...", 5),
                (25, "Consultando múltiples fuentes...", 6),
                (40, "Descargando datos satelitales...", 8),
                (60, "Procesando imágenes de alta resolución...", 10),
                (75, "Aplicando algoritmos complejos...", 7),
                (90, "Generando reportes detallados...", 4),
                (100, "Procesamiento de larga duración completado", 2)
            ]
        }
    
    def procesar_consulta(self, consulta_id: str, query_dict: Dict):
        """
        Mismo nombre de método que el procesador real
        """
        try:
            # Determinar velocidad
            velocidad = self._determinar_velocidad(query_dict, "normal")
            etapas = self.etapas_simuladas[velocidad]
            
            logging.info(f"Simulando procesamiento para {consulta_id}")
            
            for progreso, mensaje, duracion_segundos in etapas:
                self.db.actualizar_estado(consulta_id, "procesando", progreso, mensaje)
                time.sleep(duracion_segundos)
            
            resultados = self._generar_resultados_simulados(consulta_id, query_dict)
            self.db.guardar_resultados(consulta_id, resultados)
            
        except Exception as e:
            logging.error(f"Error en simulación: {e}")
            self.db.actualizar_estado(consulta_id, "error", 0, f"Error: {str(e)}")
    
    def obtener_estadisticas(self) -> Dict:
        """Mismo nombre de método"""
        consultas = self.db.listar_consultas(limite=50)
        completadas = [c for c in consultas if c['estado'] == 'completado']
        
        return {
            "tipo_procesador": self.nombre,
            "total_consultas": len(consultas),
            "completadas": len(completadas),
            "en_proceso": len([c for c in consultas if c['estado'] == 'procesando']),
            "tasa_exito": len(completadas) / max(1, len(consultas)) * 100
        }
    
    def _determinar_velocidad(self, query_dict: Dict, velocidad_default: str) -> str:
        """Determina la velocidad basada en la complejidad de la query"""
        total_fechas = query_dict.get('total_fechas_expandidas', 1)
        total_bandas = len(query_dict.get('bandas') or [])
        total_productos = len(query_dict.get('productos') or [])
        
        complejidad = total_fechas * total_bandas * max(1, total_productos)
        
        if complejidad > 100:
            return "lento"
        elif complejidad > 30:
            return "normal"
        else:
            return "rapido"
    
    def _generar_resultados_simulados(self, consulta_id: str, query_dict: Dict) -> Dict:
        """Genera resultados simulados realistas"""
        fechas = list(query_dict.get('fechas', {}).keys())
        satelite = query_dict.get('satelite', 'unknown')
        nivel = query_dict.get('nivel', 'unknown')
        productos = query_dict.get('productos') or []
        bandas = query_dict.get('bandas') or []
        
        # Simular archivos generados
        archivos_generados = []
        for i, fecha in enumerate(fechas[:10]):  # Máximo 10 archivos en simulación
            for producto in productos[:3]:  # Máximo 3 productos
                archivo = f"GOES16_{satelite}_{nivel}_{producto}_{fecha}_B{bandas[i % len(bandas)] if bandas else 'ALL'}.nc"
                archivos_generados.append(archivo)
        
        # Calcular tamaño total simulado
        tamaño_mb = len(archivos_generados) * random.uniform(10.0, 50.0)
        
        return {
            "archivos_generados": archivos_generados,
            "total_archivos": len(archivos_generados),
            "tamaño_total_mb": round(tamaño_mb, 2),
            "url_descarga": f"https://storage.ejemplo.com/download/{consulta_id}.zip",
            "fechas_procesadas": fechas,
            "productos_generados": productos,
            "bandas_utilizadas": bandas,
            "timestamp_procesamiento": datetime.now().isoformat(),
            "estadisticas": {
                "fechas_exitosas": len(fechas),
                "fechas_fallidas": 0,
                "tiempo_promedio_por_fecha": "45.2 segundos",
                "calidad_datos": "95.8%"
            }
        }
    
    def simular_error(self, consulta_id: str, mensaje_error: str = "Error simulado en procesamiento"):
        """Simula un error en el procesamiento (útil para testing)"""
        try:
            self.db.actualizar_estado(consulta_id, "procesando", 30, "Procesamiento normal...")
            time.sleep(2)
            raise Exception(mensaje_error)
        except Exception as e:
            self.db.actualizar_estado(consulta_id, "error", 0, f"Error simulado: {str(e)}")
    