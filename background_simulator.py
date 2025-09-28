import time
import logging
import random
from datetime import datetime, timedelta
from typing import Dict
from database import ConsultasDatabase
from collections import defaultdict


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
        nivel = query_dict.get('nivel', 'L1b')
        dominio = query_dict.get('dominio') or 'fd'
        bandas_solicitadas = query_dict.get('bandas') or []
        productos_solicitados = query_dict.get('productos') or []
        fechas_originales = query_dict.get('_original_request', {}).get('fechas', {})

        sat_code = f"G{satelite.split('-')[-1]}" if '-' in satelite else satelite

        # 1. Generar todos los "objetivos" teóricos
        objetivos = []
        for fecha_jjj, horarios_list in query_dict.get('fechas', {}).items():
            fecha_dt = datetime.strptime(fecha_jjj, "%Y%j")
            for horario_str in horarios_list:
                partes = horario_str.split('-')
                inicio_str, fin_str = partes[0], partes[1] if len(partes) > 1 else partes[0]
                inicio_dt = fecha_dt.replace(hour=int(inicio_str[:2]), minute=int(inicio_str[3:]))
                fin_dt = fecha_dt.replace(hour=int(fin_str[:2]), minute=int(fin_str[3:]))
                current_dt = inicio_dt
                while current_dt <= fin_dt:
                    if (dominio == 'conus' and current_dt.minute % 5 == 1) or \
                       (dominio == 'fd' and current_dt.minute % 10 == 0):
                        timestamp_archivo = f"s{current_dt.strftime('%Y%j%H%M')}00"
                        nombre_tgz = f"OR_{sensor.upper()}-{nivel}-RadF-M6_{sat_code}_{timestamp_archivo}.tgz"
                        fecha_ymd_str = current_dt.strftime("%Y%m%d")
                        objetivos.append({
                            "nombre_archivo": nombre_tgz,
                            "fecha_original_ymd": fecha_ymd_str,
                            "horario_original": horario_str
                        })
                    current_dt += timedelta(minutes=1)

        # 2. Simular recuperación local y S3 con fallos aleatorios
        archivos_recuperados = []
        objetivos_fallidos_final = []

        for objetivo in objetivos:
            # Simular recuperación local (80% de éxito)
            if random.random() < 0.8:
                archivos_recuperados.append(objetivo["nombre_archivo"])
            else:
                # Simular recuperación S3 (50% de éxito para los que fallaron localmente)
                if random.random() < 0.5:
                    archivos_recuperados.append(objetivo["nombre_archivo"])
                else:
                    # Fallo definitivo
                    objetivos_fallidos_final.append(objetivo)

        # 3. Construir la consulta de recuperación
        consulta_recuperacion = None
        if objetivos_fallidos_final:
            fechas_fallidas = defaultdict(list)
            for obj in objetivos_fallidos_final:
                # Encontrar la clave de fecha original (que puede ser un rango) que contiene la fecha YMD del objetivo.
                fecha_ymd_fallida = obj["fecha_original_ymd"]
                fecha_clave = fecha_ymd_fallida # Fallback
                for f_key in fechas_originales.keys():
                    if '-' in f_key and f_key.split('-')[0] <= fecha_ymd_fallida <= f_key.split('-')[1]:
                        fecha_clave = f_key
                    elif f_key == fecha_ymd_fallida:
                        fecha_clave = f_key
                if obj["horario_original"] not in fechas_fallidas[fecha_clave]: # Usar la clave original (ej. "20231027-20231028")
                    fechas_fallidas[fecha_clave].append(obj["horario_original"])
            
            # Reconstruir la consulta de recuperación
            consulta_recuperacion = query_dict.get('_original_request', {}).copy()
            # Limpiar campos que no son parte del request
            consulta_recuperacion.pop('fechas', None)
            consulta_recuperacion.pop('creado_por', None)
            consulta_recuperacion['fechas'] = dict(fechas_fallidas)
            consulta_recuperacion['descripcion'] = f"Consulta de recuperación simulada para {consulta_id}"

        # 4. Simular la extracción de archivos si es necesario
        # (Esta lógica es simplificada, solo para el nombre del archivo)
        copiar_tgz_completo = (nivel == 'L1b' and len(bandas_solicitadas) == 16) or \
                              (nivel == 'L2' and not productos_solicitados)

        if not copiar_tgz_completo:
            # Si no se copia el tgz, simulamos nombres de archivos .nc
            archivos_finales = []
            for tgz_name in archivos_recuperados:
                timestamp_part = tgz_name.split('_', 4)[-1].split('.')[0]
                if nivel == 'L1b':
                    for banda in bandas_solicitadas:
                        archivos_finales.append(f"OR_{sensor.upper()}-{nivel}-RadF-M6C{banda}_{sat_code}_{timestamp_part}_e..._c....nc")
                elif nivel == 'L2':
                    for producto in productos_solicitados:
                        archivos_finales.append(f"OR_{sensor.upper()}-{nivel}-{producto}F-M6_{sat_code}_{timestamp_part}_e..._c....nc")
            archivos_recuperados = archivos_finales

        # 5. Calcular tamaño y generar el reporte final
        tamaño_por_archivo = random.uniform(100.0, 500.0) if copiar_tgz_completo else random.uniform(20.0, 150.0)
        tamaño_mb = len(archivos_recuperados) * tamaño_por_archivo

        return {
            "archivos_recuperados": archivos_recuperados,
            "total_archivos": len(archivos_recuperados),
            "tamaño_total_mb": round(tamaño_mb, 2),
            "directorio_destino": f"/data/tmp/{consulta_id}",
            "timestamp_procesamiento": datetime.now().isoformat(),
            "consulta_recuperacion": consulta_recuperacion
        }
    
    def simular_error(self, consulta_id: str, mensaje_error: str = "Error simulado en procesamiento"):
        """Simula un error en el procesamiento (útil para testing)"""
        try:
            self.db.actualizar_estado(consulta_id, "procesando", 30, "Procesamiento normal...")
            time.sleep(2)
            raise Exception(mensaje_error)
        except Exception as e:
            self.db.actualizar_estado(consulta_id, "error", 0, f"Error simulado: {str(e)}")
    