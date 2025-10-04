import time
import logging
import os
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
        # Hacer las probabilidades de éxito configurables
        self.local_success_rate = float(os.getenv("SIM_LOCAL_SUCCESS_RATE", "0.8"))
        self.s3_success_rate = float(os.getenv("SIM_S3_SUCCESS_RATE", "0.5"))
        logging.info(f"📈 Simulador inicializado con tasa de éxito local: {self.local_success_rate*100}% y S3: {self.s3_success_rate*100}%")

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
            # Mensaje final breve y realista, consistente con el procesador real
            total_recuperados = resultados.get("total_archivos", 0)
            s3_ok = resultados.get("fuentes", {}).get("s3", {}).get("total", 0)
            lustre_ok = resultados.get("fuentes", {}).get("lustre", {}).get("total", 0)
            # En el simulador, fallidos son los objetivos que no se recuperaron por ninguna fuente
            objetivos_totales_estimados = lustre_ok + s3_ok + len(resultados.get("consulta_recuperacion", {}) or {})
            # Alternativamente calcular fallidos a partir de la consulta_recuperacion si existe
            fallidos = 0
            if resultados.get("consulta_recuperacion"):
                try:
                    fallidos = sum(len(v) for v in resultados["consulta_recuperacion"].get("fechas", {}).values())
                except Exception:
                    fallidos = 0
            mensaje_final = f"Recuperación: T={total_recuperados}, L={lustre_ok}, S={s3_ok}" + (f", F={fallidos}" if fallidos else "")
            self.db.guardar_resultados(consulta_id, resultados, mensaje=mensaje_final)
            
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
    
    def _resolver_bandas(self, nivel: str, producto: str, bandas_solicitadas):
        """
        Devuelve la lista de bandas a usar para el nombre de archivo.
        - L2 + CMI*: si no se indicó bandas -> ALL (01..16).
        - En otros casos -> normaliza bandas_solicitadas.
        """
        nivel_u = (nivel or "").upper()
        prod_u = (producto or "").upper()
        bandas_solicitadas = bandas_solicitadas or []
        if nivel_u == "L2" and prod_u.startswith("CMI"):
            if not bandas_solicitadas:
                return [f"{i:02d}" for i in range(1, 17)]
        return [f"{int(b):02d}" if str(b).isdigit() else str(b) for b in bandas_solicitadas]
    
    def _generar_resultados_simulados(self, consulta_id: str, query_dict: Dict) -> Dict:
        """Genera resultados simulados realistas, imitando la lógica de `RecoverFiles`."""
        # Extraer parámetros de la consulta
        satelite = query_dict.get('satelite', 'GOES-16')
        sensor = query_dict.get('sensor', 'abi')
        nivel = query_dict.get('nivel', 'L1b')
        dominio = query_dict.get('dominio') # Ahora es obligatorio
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

        # 2. Simular recuperación de Lustre, S3 y fallos
        lustre_recuperados = []
        s3_recuperados = []
        objetivos_fallidos_final = []

        for objetivo in objetivos:
            # Simular recuperación local (80% de éxito)
            if random.random() < self.local_success_rate:
                lustre_recuperados.append(objetivo["nombre_archivo"])
            else:
                # Simular recuperación S3 (50% de éxito para los que fallaron localmente)
                if random.random() < self.s3_success_rate:
                    s3_recuperados.append(objetivo["nombre_archivo"])
                else:
                    # Fallo definitivo
                    objetivos_fallidos_final.append(objetivo)

        # 3. Construir la consulta de recuperación
        consulta_recuperacion = None
        if objetivos_fallidos_final:
            fechas_fallidas = defaultdict(list)
            for obj in objetivos_fallidos_final:
                fecha_ymd_fallida = obj["fecha_original_ymd"]
                horario_original_fallido = obj["horario_original"]

                # Encontrar la clave de fecha original (que puede ser un rango como "20230101-20230105")
                # que contiene la fecha YMD del objetivo fallido.
                for fecha_key_original, horarios_list in fechas_originales.items():
                    start_date_str = fecha_key_original.split('-')[0]
                    end_date_str = fecha_key_original.split('-')[-1]
                    
                    if start_date_str <= fecha_ymd_fallida <= end_date_str:
                        if horario_original_fallido in horarios_list:
                            if horario_original_fallido not in fechas_fallidas[fecha_key_original]:
                                fechas_fallidas[fecha_key_original].append(horario_original_fallido)
                            break # Pasar al siguiente objetivo fallido
            
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
            # Función auxiliar para expandir nombres de .tgz a .nc
            def expandir_nombres(lista_tgz):
                archivos_nc = []
                dom_letter = 'C' if dominio == 'conus' else 'F'
                for tgz_name in lista_tgz:
                    timestamp_part = tgz_name.split('_', 4)[-1].split('.')[0]
                    if nivel == 'L1b':
                        for banda in bandas_solicitadas:
                            banda_str = f"{int(banda):02d}" if str(banda).isdigit() else str(banda)
                            archivos_nc.append(
                                f"OR_{sensor.upper()}-{nivel}-Rad{dom_letter}-M6C{banda_str}_{sat_code}_{timestamp_part}_e..._c....nc"
                            )
                    elif nivel == 'L2':
                        for producto in productos_solicitados:
                            prod_upper = str(producto).upper()
                            if prod_upper.startswith('CMI'):
                                # L2 CMI: incluir banda tras M6 con patrón Cdd (ej. M6C13)
                                product_token = f"CMIP{dom_letter}"
                                bands = self._resolver_bandas(nivel, prod_upper, bandas_solicitadas)
                                for banda in bands:
                                    banda_str = f"{int(banda):02d}" if str(banda).isdigit() else str(banda)
                                    archivos_nc.append(
                                        f"CG_{sensor.upper()}-L2-{product_token}-M6C{banda_str}_{sat_code}_{timestamp_part}_e..._c....nc"
                                    )
                            else:
                                # L2 no CMI: sin banda específica
                                product_token = f"{prod_upper}{dom_letter}"
                                archivos_nc.append(
                                    f"OR_{sensor.upper()}-L2-{product_token}-M6_{sat_code}_{timestamp_part}_e..._c....nc"
                                )
                return archivos_nc
            # Aplicar la expansión a ambas listas
            lustre_recuperados = expandir_nombres(lustre_recuperados)
            s3_recuperados = expandir_nombres(s3_recuperados)

        # 5. Calcular tamaño y generar el reporte final (imitando la nueva estructura)
        todos_los_archivos = lustre_recuperados + s3_recuperados
        tamaño_por_archivo = random.uniform(100.0, 500.0) if copiar_tgz_completo else random.uniform(20.0, 150.0)
        tamaño_mb = len(todos_los_archivos) * tamaño_por_archivo

        return {
            "fuentes": {
                "lustre": {"archivos": lustre_recuperados, "total": len(lustre_recuperados)},
                "s3": {"archivos": s3_recuperados, "total": len(s3_recuperados)}
            },
            "total_archivos": len(todos_los_archivos),
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
