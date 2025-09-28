import time
import logging
import random
from datetime import datetime, timedelta
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
        """Genera resultados simulados realistas, imitando la lógica de `RecoverFiles`."""
        # Extraer parámetros de la consulta
        satelite = query_dict.get('satelite', 'GOES-16')
        sensor = query_dict.get('sensor', 'abi')
        nivel = query_dict.get('nivel', 'unknown')
        dominio = query_dict.get('dominio', 'fd')
        bandas_solicitadas = query_dict.get('bandas') or []
        productos_solicitados = query_dict.get('productos') or []
        
        archivos_generados = []
        sat_code = f"G{satelite.split('-')[-1]}" if '-' in satelite else satelite

        for fecha_jjj, horarios_list in query_dict.get('fechas', {}).items():
            fecha_dt = datetime.strptime(fecha_jjj, "%Y%j")

            for horario_str in horarios_list:
                # Parsear el rango de tiempo solicitado
                partes = horario_str.split('-')
                inicio_str = partes[0]
                fin_str = partes[1] if len(partes) > 1 else inicio_str
                
                inicio_dt = fecha_dt.replace(hour=int(inicio_str[:2]), minute=int(inicio_str[3:]))
                fin_dt = fecha_dt.replace(hour=int(fin_str[:2]), minute=int(fin_str[3:]))

                # Generar timestamps según la frecuencia del dominio
                current_dt = inicio_dt
                while current_dt <= fin_dt:
                    # Determinar si el timestamp actual es válido para el dominio
                    es_valido = False
                    if dominio == 'conus' and current_dt.minute % 5 == 1:
                        es_valido = True
                    elif dominio == 'fd' and current_dt.minute % 10 == 0:
                        es_valido = True

                    if es_valido:
                        # Formato de timestamp para el nombre del archivo (ej. sYYYYJJJHHMMSS)
                        timestamp_archivo = f"s{current_dt.strftime('%Y%j%H%M')}00"

                        # Construir el nombre del archivo .tgz según el nivel
                        if nivel == 'L1b':
                            nombre_tgz = f"OR_{sensor.upper()}-{nivel}-RadF-M6_{sat_code}_{timestamp_archivo}.tgz"
                        elif nivel == 'L2':
                            dominio_part = f"_{dominio.upper()}" if dominio else ""
                            nombre_tgz = f"OR_{sensor.upper()}-{nivel}-Products{dominio_part}_{sat_code}_{timestamp_archivo}.tgz"
                        else:
                            nombre_tgz = f"OR_{sensor.upper()}-{nivel}_G{sat_code}_{timestamp_archivo}.tgz"
                        
                        # Simular la extracción si no se pidieron todas las bandas
                        copiar_tgz_completo = False
                        if nivel == 'L1b':
                            copiar_tgz_completo = len(bandas_solicitadas) == 16
                        elif nivel == 'L2':
                            copiar_tgz_completo = not productos_solicitados

                        if copiar_tgz_completo:
                            archivos_generados.append(nombre_tgz)
                        else:
                            # Si se pidió un subconjunto, simular los archivos .nc extraídos
                            if nivel == 'L1b':
                                for banda in bandas_solicitadas:
                                    nombre_nc = f"OR_{sensor.upper()}-{nivel}-RadF-M6C{banda}_{sat_code}_{timestamp_archivo}_e..._c....nc"
                                    archivos_generados.append(nombre_nc)
                            elif nivel == 'L2':
                                for producto in productos_solicitados:
                                    dominio_code = dominio[0].upper() if dominio else 'F'
                                    nombre_nc = f"OR_{sensor.upper()}-{nivel}-{producto}{dominio_code}-M6_{sat_code}_{timestamp_archivo}_e..._c....nc"
                                    archivos_generados.append(nombre_nc)
                    
                    current_dt += timedelta(minutes=1)
        
        # Calcular tamaño total simulado
        # El tamaño por archivo es menor si son .nc individuales
        tamaño_por_archivo = random.uniform(100.0, 500.0) if copiar_tgz_completo else random.uniform(20.0, 150.0)
        tamaño_mb = len(archivos_generados) * tamaño_por_archivo
        
        return {
            "archivos_generados": archivos_generados,
            "total_archivos": len(archivos_generados),
            "tamaño_total_mb": round(tamaño_mb, 2),
            "url_descarga": f"https://storage.ejemplo.com/download/{consulta_id}.zip",
            "fechas_procesadas": list(query_dict.get('fechas', {}).keys()),
            "bandas_utilizadas": bandas_solicitadas,
            "productos_generados": productos_solicitados,
            "timestamp_procesamiento": datetime.now().isoformat(),
            "estadisticas": {
                "fechas_exitosas": len(query_dict.get('fechas', {})),
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
    