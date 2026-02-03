import logging
import shutil
import re
import tarfile
from typing import List, Dict, Iterable, Optional
from datetime import datetime, timezone
from pathlib import Path
from pebble import ProcessPool, ThreadPool
from concurrent.futures import TimeoutError, as_completed
from database import ConsultasDatabase
from collections import defaultdict
import time
from s3_recover import S3RecoverFiles
from config import SatelliteConfigGOES
from settings import settings

from settings import settings

# Instanciar configuración para referenciar listas válidas (bandas/productos)
_SAT_CONFIG = SatelliteConfigGOES()


# Expresión regular para extraer el timestamp de inicio del nombre de archivo
_FILENAME_TIMESTAMP_RE = re.compile(r'_s(\d{4})(\d{3})(\d{2})\d+.*')


def filter_files_by_time(archivos_nc: list, fecha_jjj: str, horarios_list: list) -> list:
    """
    Filtra archivos NetCDF por fecha juliana y rango horario.
    Compatible con archivos S3 (string) y rutas locales NetCDF.
    """
    # Pre-compilar los rangos horarios para una búsqueda más eficiente
    rangos_validos = []
    for horario_str in horarios_list:
        partes = horario_str.split('-')
        inicio_hh = partes[0][:2]
        fin_hh = partes[1][:2] if len(partes) > 1 else inicio_hh
        rangos_validos.append((inicio_hh, fin_hh))

    archivos_filtrados = []
    for archivo in archivos_nc:
        nombre = archivo.name if hasattr(archivo, "name") else archivo
        match = _FILENAME_TIMESTAMP_RE.search(nombre)
        if match:
            anio, dia_juliano, hora = match.groups()
            if anio + dia_juliano == fecha_jjj:
                if any(inicio <= hora <= fin for inicio, fin in rangos_validos):
                    archivos_filtrados.append(archivo)
    return archivos_filtrados

# --- Clase para recuperación local (Lustre) ---
class LustreRecoverFiles:
    def __init__(self, source_data_path: str, logger):
        self.source_data_path = Path(source_data_path)
        self.logger = logger

    def build_base_path(self, query_dict: Dict) -> Path:
        base_path = self.source_data_path
        base_path /= query_dict.get('sensor', 'abi').lower()
        base_path /= query_dict.get('nivel', 'l1b').lower()
        if query_dict.get('dominio'):
            base_path /= query_dict['dominio'].lower()
        return base_path

    def find_files_for_day(self, base_path: Path, fecha_jjj: str) -> List[Path]:
        anio = fecha_jjj[:4]
        dia_del_anio_int = int(fecha_jjj[4:])
        semana = (dia_del_anio_int - 1) // 7 + 1
        directorio_semana = base_path / anio / f"{semana:02d}"
        if not directorio_semana.exists():
            self.logger.warning(f"⚠️ Directorio no encontrado en Lustre: {directorio_semana}")
            return []
        patron_dia = f"*{anio}{dia_del_anio_int:03d}*.tgz"
        archivos_candidatos = list(directorio_semana.glob(patron_dia))
        self.logger.debug(f"  Directorio: {directorio_semana}, Candidatos para el día {fecha_jjj}: {len(archivos_candidatos)}")
        return archivos_candidatos

    def filter_files_by_time(self, archivos_candidatos: List[Path], fecha_jjj: str, horarios_list: List[str]) -> List[Path]:
        archivos_filtrados_dia = []
        for horario_str in horarios_list:
            partes = horario_str.split('-')
            inicio_hhmm = partes[0].replace(':', '')
            fin_hhmm = partes[1].replace(':', '') if len(partes) > 1 else inicio_hhmm
            inicio_ts_str = f"{fecha_jjj}{inicio_hhmm[:2]}00"
            fin_ts_str = f"{fecha_jjj}{fin_hhmm[:2]}59"
            try:
                inicio_ts = int(inicio_ts_str)
                fin_ts = int(fin_ts_str)
            except ValueError:
                self.logger.warning(f"Formato de timestamp inválido para {fecha_jjj} con horario {horario_str}. Se omite.")
                continue
            self.logger.debug(f"    Filtrando por rango horario: {horario_str} ({inicio_ts} - {fin_ts})")
            for archivo in archivos_candidatos:
                try:
                    s_part_start_idx = archivo.name.find('-s')
                    if s_part_start_idx != -1:
                        file_ts_str = archivo.name[s_part_start_idx + 2 : s_part_start_idx + 13]
                        file_ts = int(file_ts_str)
                        if inicio_ts <= file_ts <= fin_ts:
                            archivos_filtrados_dia.append(archivo)
                except (ValueError, IndexError, AttributeError):
                    continue
        return archivos_filtrados_dia

    def discover_and_filter_files(self, query_dict: Dict) -> List[Path]:
        archivos_encontrados_set = set()
        base_path = self.build_base_path(query_dict)
        for fecha_jjj, horarios_list in query_dict.get('fechas', {}).items():
            archivos_candidatos_dia = self.find_files_for_day(base_path, fecha_jjj)
            if not archivos_candidatos_dia:
                continue
            archivos_filtrados = self.filter_files_by_time(archivos_candidatos_dia, fecha_jjj, horarios_list)
            archivos_encontrados_set.update(archivos_filtrados)
        return sorted(list(archivos_encontrados_set))

    def scan_existing_files(self, archivos_a_procesar: List[Path], destino: Path) -> List[Path]:
        if not destino.exists() or not any(destino.iterdir()):
            return archivos_a_procesar
        timestamps_existentes = set()
        for f in destino.iterdir():
            if f.is_file():
                s_part_start_idx = f.name.find('_s')
                if s_part_start_idx != -1:
                    timestamp_part = f.name[s_part_start_idx + 2 : s_part_start_idx + 13]
                    timestamps_existentes.add(timestamp_part)
        archivos_pendientes = []
        for archivo_fuente in archivos_a_procesar:
            s_part_start_idx = archivo_fuente.name.find('_s')
            if s_part_start_idx != -1:
                timestamp_fuente = archivo_fuente.name[s_part_start_idx + 2 : s_part_start_idx + 13]
                if timestamp_fuente not in timestamps_existentes:
                    archivos_pendientes.append(archivo_fuente)
            else:
                archivos_pendientes.append(archivo_fuente)
        num_recuperados = len(archivos_a_procesar) - len(archivos_pendientes)
        return archivos_pendientes


# --- Clase principal orquestadora ---
class RecoverFiles:
    def __init__(self, db: ConsultasDatabase, source_data_path: str, base_download_path: str, executor, s3_fallback_enabled: Optional[bool] = None, lustre_enabled: Optional[bool] = None, max_workers: Optional[int] = None, file_processing_timeout_seconds: Optional[int] = None):
        self.db = db
        self.source_data_path = Path(source_data_path)
        self.base_download_path = Path(base_download_path)
        self.logger = logging.getLogger(__name__)
        self.executor = executor
        
        self.s3_fallback_enabled = settings.s3_fallback_enabled if s3_fallback_enabled is None else s3_fallback_enabled
        self.lustre_enabled = settings.lustre_enabled if lustre_enabled is None else lustre_enabled
        self.FILE_PROCESSING_TIMEOUT_SECONDS = file_processing_timeout_seconds or settings.file_processing_timeout_seconds

        self.S3_RETRY_ATTEMPTS = settings.S3_RETRY_ATTEMPTS
        self.S3_RETRY_BACKOFF_SECONDS = settings.S3_RETRY_BACKOFF_SECONDS
        self.GOES19_OPERATIONAL_DATE = datetime(2025, 4, 1, tzinfo=timezone.utc)
        self.lustre = LustreRecoverFiles(source_data_path, self.logger)

        # Inicializa self.max_workers ANTES de usarla
        self.max_workers = max_workers or getattr(executor, "max_workers", settings.max_workers)

        self.s3 = S3RecoverFiles(self.logger, self.max_workers)
        # Limitar tamaño de listas en el reporte final para grandes volúmenes
        self.max_files_in_report = settings.max_files_per_query if settings.max_files_per_query > 0 else 1000

    def procesar_consulta(self, consulta_id: str, query_dict: Dict):
        try:
            # 1. Preparar entorno
            directorio_destino = self.base_download_path / consulta_id
            directorio_destino.mkdir(exist_ok=True, parents=True)
            self.db.actualizar_estado(consulta_id, "procesando", 10, "Preparando entorno")

            objetivos_fallidos_local = []
            archivos_pendientes_local: List[Path] = []

            # --- Optimización: Separar productos locales de los exclusivos de S3 ---
            productos_req_originales = query_dict.get('productos', []) or []
            s3_only_products = set(_SAT_CONFIG.S3_ONLY_PRODUCTS)
            
            productos_para_lustre = [p for p in productos_req_originales if p.upper() not in s3_only_products]
            query_para_lustre = query_dict.copy()
            query_para_lustre['productos'] = productos_para_lustre
            # --------------------------------------------------------------------

            if self.lustre_enabled:
                # 2. Descubrir y filtrar archivos locales.
                # Se elimina la lógica especial para 'ALL' en esta etapa.
                # La decisión de copiar el tgz completo o extraer se toma por archivo en _process_safe_recover_file.
                archivos_a_procesar_local = self.lustre.discover_and_filter_files(query_para_lustre)
                inaccessible_files_local = []

                # 3. Escanear destino
                if archivos_a_procesar_local:
                    archivos_pendientes_local = self.lustre.scan_existing_files(archivos_a_procesar_local, directorio_destino)
                    if not archivos_pendientes_local:
                        pass
                else:
                    archivos_pendientes_local = []

                total_pendientes = len(archivos_pendientes_local)
                self.db.actualizar_estado(consulta_id, "procesando", 20, f"Identificados {total_pendientes} archivos pendientes de procesar.")

                objetivos_fallidos_local.extend(inaccessible_files_local)

                # 4. Procesar archivos pendientes en paralelo
                # Extraer bandas y productos originales del request para lógica tgz
                original_req = query_dict.get('_original_request', {})
                bandas_original = original_req.get('bandas', [])
                productos_original_para_lustre = original_req.get('productos', [])
                if archivos_pendientes_local:
                    future_to_objetivo = {
                        self.executor.schedule(
                            _process_safe_recover_file, 
                            args=(
                                archivo_a_procesar, 
                                directorio_destino, 
                                query_dict.get('nivel'), 
                                productos_original_para_lustre, # Usar la lista completa original para la lógica interna
                                bandas_original # Usar la lista original para la lógica interna
                            ), 
                            timeout=self.FILE_PROCESSING_TIMEOUT_SECONDS
                        ): archivo_a_procesar
                        for i, archivo_a_procesar in enumerate(archivos_pendientes_local)
                    }
                    # Procesar tareas a medida que van completando para evitar bloquearse por una sola tarea lenta
                    for i, future in enumerate(as_completed(list(future_to_objetivo.keys()))):
                        archivo_fuente = future_to_objetivo[future]
                        try:
                            future.result()
                            mensaje = f"Recuperado archivo {i+1}/{total_pendientes} ({archivo_fuente.name})"
                        except TimeoutError:
                            self.logger.error(f"❌ Timeout en archivo {archivo_fuente.name}")
                            objetivos_fallidos_local.append(archivo_fuente)
                            mensaje = f"Falla por timeout {i+1}/{total_pendientes} ({archivo_fuente.name})"
                        except Exception as e:
                            self.logger.error(f"❌ Error procesando el archivo {archivo_fuente.name}: {e}")
                            objetivos_fallidos_local.append(archivo_fuente)
                            mensaje = f"Falla {i+1}/{total_pendientes} ({archivo_fuente.name})"
                        progreso = 20 + int(((i + 1) / total_pendientes) * 60)
                        self.db.actualizar_estado(consulta_id, "procesando", progreso, mensaje)
            else:
                # Saltar por completo la etapa local si Lustre está deshabilitado
                self.db.actualizar_estado(consulta_id, "procesando", 20, "Lustre deshabilitado; saltando recuperación local.")

            # 5. Recuperar desde S3 si está habilitado
            if self.s3_fallback_enabled:
                self.db.actualizar_estado(consulta_id, "procesando", 85, "Buscando archivos adicionales en S3.")
                s3_map = {}

                nivel = (query_dict.get("nivel") or "").upper()
                if nivel == "L2":
                    # Separar productos CMI* de no-CMI para no aplicar 'bandas' a ACHA/otros
                    productos_req = (query_dict.get("productos") or [])
                    productos_upper = [str(p).strip().upper() for p in productos_req]
                    cmi_products = [p for p in productos_upper if p.startswith("CMI")]
                    other_products = [p for p in productos_upper if not p.startswith("CMI")]

                    # Consulta para CMI*: respeta 'bandas'
                    if cmi_products:
                        q_cmi = dict(query_dict)
                        q_cmi["productos"] = cmi_products
                        q_cmi["bandas"] = query_dict.get("bandas") or []
                        s3_map.update(self.s3.discover_files(q_cmi, self.GOES19_OPERATIONAL_DATE))
                    # Consulta para no-CMI: ignorar 'bandas'
                    if other_products:
                        q_other = dict(query_dict)
                        q_other["productos"] = other_products
                        q_other["bandas"] = []  # explícito: no filtrar por banda
                        s3_map.update(self.s3.discover_files(q_other, self.GOES19_OPERATIONAL_DATE))
                elif nivel == "L1B":
                    # L1b: solo una consulta, usando bandas
                    q_l1b = dict(query_dict)
                    s3_map.update(self.s3.discover_files(q_l1b, self.GOES19_OPERATIONAL_DATE))

                archivos_s3_filtrados = []
                for fecha_jjj, horarios_list in query_dict.get('fechas', {}).items():
                    archivos_encontrados = [s3_map[k] for k in s3_map]
                    archivos_s3_filtrados += filter_files_by_time(archivos_encontrados, fecha_jjj, horarios_list)
                objetivos_finales_s3 = list(set(archivos_s3_filtrados))
                # Publicar un mensaje con conteo antes de iniciar descargas
                try:
                    total_s3 = len(objetivos_finales_s3)
                    self.db.actualizar_estado(consulta_id, "procesando", 85, f"Descargas S3 pendientes: {total_s3}")
                except Exception:
                    pass

                s3_recuperados, objetivos_fallidos_s3 = self.s3.download_files(
                    consulta_id, objetivos_finales_s3, directorio_destino, self.db
                )
                # Combinar fallas locales y de S3
                objetivos_fallidos_final = list(objetivos_fallidos_local) + list(objetivos_fallidos_s3)
            else:
                s3_recuperados = []
                objetivos_fallidos_final = objetivos_fallidos_local

            # 6. Generar reporte final
            all_files_in_destination = [f for f in directorio_destino.iterdir() if f.is_file()]
            self.db.actualizar_estado(consulta_id, "procesando", 95, "Generando reporte final")
            
            # Obtener la consulta para acceder al timestamp de creación
            consulta_db = self.db.obtener_consulta(consulta_id)
            timestamp_creacion = consulta_db.get("timestamp_creacion") if consulta_db else datetime.now().isoformat()

            resultados_finales = self._generar_reporte_final(
                consulta_id, all_files_in_destination, s3_recuperados, directorio_destino, objetivos_fallidos_final, query_dict, timestamp_creacion
            )
            # Mensaje final breve y legible
            total_recuperados = resultados_finales.get("total_archivos", 0)
            s3_ok = resultados_finales.get("fuentes", {}).get("s3", {}).get("total", 0)
            lustre_ok = resultados_finales.get("fuentes", {}).get("lustre", {}).get("total", 0)
            fallidos = len(objetivos_fallidos_final) if objetivos_fallidos_final else 0
            mensaje_final = f"Recuperación: T={total_recuperados}, L={lustre_ok}, S={s3_ok}" + (f", F={fallidos}" if fallidos else "")

            self.db.guardar_resultados(consulta_id, resultados_finales, mensaje=mensaje_final)
        except Exception as e:
            self.logger.error(f"❌ Error procesando consulta {consulta_id}: {e}")
            self.db.actualizar_estado(consulta_id, "error", 0, f"Error: {str(e)}")

    def _build_recovery_query(self, consulta_id: str, objetivos_fallidos: List[Path], query_original: Dict) -> Optional[Dict]:
        """Construye una nueva consulta a partir de los archivos que fallaron."""
        if not objetivos_fallidos:
            return None

        fechas_fallidas = defaultdict(list)
        original_fechas = query_original.get('_original_request', {}).get('fechas', {})

        for archivo_fallido in objetivos_fallidos:
            try:
                # 1. Extraer el timestamp YYYYJJJHHMM del nombre del archivo.
                ts_str = archivo_fallido.name.split('-s')[1].split('.')[0][:11]
                fecha_fallida_dt = datetime.strptime(ts_str, '%Y%j%H%M')
                fecha_fallida_ymd = fecha_fallida_dt.strftime('%Y%m%d')

                # 2. Encontrar la clave de fecha y el rango horario originales.
                for fecha_key_original, horarios_list in original_fechas.items():
                    # Comprobar si la fecha del archivo está dentro del rango de la clave (ej. "20230101-20230105")
                    start_date_str = fecha_key_original.split('-')[0]
                    end_date_str = fecha_key_original.split('-')[-1]
                    if not (start_date_str <= fecha_fallida_ymd <= end_date_str):
                        continue

                    for horario_rango in horarios_list:
                        # Comprobar si la hora del archivo está dentro del rango horario.
                        inicio_str, fin_str = (horario_rango.split('-') + [horario_rango])[:2]
                        inicio_t = datetime.strptime(inicio_str, '%H:%M').time()
                        fin_t = datetime.strptime(fin_str, '%H:%M').time()

                        if inicio_t <= fecha_fallida_dt.time() <= fin_t:
                            if horario_rango not in fechas_fallidas[fecha_key_original]:
                                fechas_fallidas[fecha_key_original].append(horario_rango)
                            break # Encontrado el rango horario, pasar al siguiente archivo.
                    else:
                        continue
                    break # Encontrada la clave de fecha, pasar al siguiente archivo.

            except (IndexError, ValueError):
                continue

        if fechas_fallidas:
            consulta_recuperacion = query_original.get('_original_request', {}).copy()
            consulta_recuperacion.pop('creado_por', None)
            consulta_recuperacion['fechas'] = dict(fechas_fallidas)
            consulta_recuperacion['descripcion'] = f"Consulta de recuperación para la solicitud original {consulta_id}"
            return consulta_recuperacion
        
        return None

    def _generar_reporte_final(self, consulta_id: str, all_files_in_destination: List[Path], s3_recuperados: List[Path], directorio_destino: Path, objetivos_fallidos: List[Path], query_original: Dict, timestamp_creacion_iso: str) -> Dict:
        """Genera el diccionario de resultados finales."""
        # --- Cálculo de la duración total del procesamiento ---
        timestamp_finalizacion = datetime.now()
        duracion_str = "N/A"
        try:
            # Convertir ambos timestamps a objetos datetime
            ts_inicio = datetime.fromisoformat(timestamp_creacion_iso)
            # Calcular la diferencia
            delta = timestamp_finalizacion - ts_inicio
            duracion_str = str(delta).split('.')[0] # Formato HH:MM:SS
        except (ValueError, TypeError):
            self.logger.warning(f"No se pudo calcular la duración para la consulta {consulta_id} debido a un timestamp inválido.")

        # Optimizar: usar comparación por nombre con set para evitar O(n^2)
        s3_names_full = [p.name for p in s3_recuperados]
        s3_names_set = set(s3_names_full)
        lustre_names_full = [p.name for p in all_files_in_destination if p.name not in s3_names_set]
        todos_los_archivos = all_files_in_destination
        # Cálculo de tamaño total (puede ser costoso con cientos de miles de archivos)
        total_bytes = sum(f.stat().st_size for f in todos_los_archivos if f.is_file())
        tamaño_mb = round(total_bytes / (1024 * 1024), 2)

        # Conteo por producto (basado en el nombre del archivo OR_/CG_ABI-L2-<PROD><DOM>-M6...)
        from collections import defaultdict as _dd
        def _extraer_producto_base(nombre: str) -> str:
            try:
                # localizar '-L2-' y '-M'
                i = nombre.index('-L2-') + 4
                j = nombre.index('-M', i)
                seg = nombre[i:j]  # puede venir como 'ACMC', 'CMIPC', 'VAAC', etc.
                # Normalizar: quitar sufijo de dominio (C/F/M1/M2)
                if seg.endswith('C') or seg.endswith('F'):
                    seg = seg[:-1]
                elif seg.endswith('M1') or seg.endswith('M2'):
                    seg = seg[:-2]
                # Mapear alias a base solicitada
                alias = {
                    'CODD': 'COD', 'CODN': 'COD', 'COD': 'COD',
                    'CPSD': 'CPS', 'CPSN': 'CPS', 'CPS': 'CPS',
                    'VAAF': 'VAA', 'VAA': 'VAA',
                }
                return alias.get(seg, seg)
            except Exception:
                return 'UNKNOWN'

        conteo_total_por_producto = _dd(int)
        conteo_s3_por_producto = _dd(int)

        for p in all_files_in_destination:
            prod = _extraer_producto_base(p.name)
            if prod != 'UNKNOWN':
                conteo_total_por_producto[prod] += 1
        for p in s3_recuperados:
            prod = _extraer_producto_base(p.name)
            if prod != 'UNKNOWN':
                conteo_s3_por_producto[prod] += 1

        # Construir la consulta de recuperación usando el método refactorizado.
        consulta_recuperacion = self._build_recovery_query(consulta_id, objetivos_fallidos, query_original)

        # Truncar listas si exceden el máximo configurado para mantener el JSON/DB manejable
        max_n = self.max_files_in_report
        s3_list = s3_names_full if len(s3_names_full) <= max_n else s3_names_full[:max_n]
        lustre_list = lustre_names_full if len(lustre_names_full) <= max_n else lustre_names_full[:max_n]

        return {
            "fuentes": {
                "lustre": {
                    "archivos": lustre_list,
                    "total": len(lustre_names_full)
                },
                "s3": {
                    "archivos": s3_list,
                    "total": len(s3_names_full)
                }
            },
            "conteo_por_producto": dict(sorted(conteo_total_por_producto.items())),
            "conteo_por_producto_s3": dict(sorted(conteo_s3_por_producto.items())),
            "total_archivos": len(todos_los_archivos),
            "total_mb": tamaño_mb,
            "ruta_destino": str(directorio_destino),
            "timestamp_procesamiento": datetime.now().isoformat(),
            "consulta_recuperacion": consulta_recuperacion,
            "duracion_procesamiento": duracion_str
        }

    def _producto_requiere_bandas(self, nivel: str, producto: str) -> bool:
        n = (nivel or "").strip().upper()
        p = (producto or "").strip().upper()
        return n == "L1B" or (n == "L2" and p.startswith("CMI"))

    def _iter_patrones_l2(
        self,
        productos: List[str],
        dominio: str,
        bandas: List[str],
        sat_code: str,
        ts_inicio: str,
        ts_fin: str,
    ) -> Iterable[str]:
        """
        Genera patrones de filename para L2.
        - CMI/CMIP/CMIPC: incluye banda como M6Cdd
        - Otros (p.ej. ACHA/ACTP): sin banda (solo M6)
        """
        dom_letter = "C" if dominio == "conus" else "F"
        productos_upper = [p.strip().upper() for p in (productos or [])]
        bandas = bandas or []

        for prod in productos_upper:
            # Mapeo especial para S3
            if prod in ("CODD", "CODN", "COD"):
                prod_s3 = "COD"
            elif prod in ("CPSD", "CPSN", "CPS"):
                prod_s3 = "CPS"
            elif prod in ("VAAF", "VAA"):
                prod_s3 = "VAA"
            else:
                prod_s3 = prod

            if prod_s3.startswith("CMI"):
                # Si no se enviaron bandas, usa ALL (16) o deja que aguas abajo expanda.
                iter_bandas = bandas or [f"{i:02d}" for i in range(1, 17)]
                for b in iter_bandas:
                    b2 = f"{int(b):02d}" if str(b).isdigit() else str(b)
                    yield f"CG_ABI-L2-{prod_s3}{dom_letter}-M6C{b2}_{sat_code}_s{ts_inicio}_e{ts_fin}_c*.nc"
            else:
                yield f"CG_ABI-L2-{prod_s3}{dom_letter}-M6_{sat_code}_s{ts_inicio}_e{ts_fin}_c*.nc"

    # En donde actualmente construyes los patrones para buscar en Lustre/S3,
    # reemplaza el bloque que usa 'bandas' para todos los productos por algo como:
    def _construir_patrones_busqueda(self, query: Dict) -> List[str]:
        """
        Construye los patrones de archivo a buscar según la query.
        """
        nivel = (query.get("nivel") or "").upper()
        dominio = query.get("dominio")
        productos = query.get("productos") or []
        sat_code = self._sat_to_code(query.get("sat"))  # asume que existe
        ts_inicio, ts_fin = self._rangos_a_timestamps(query)  # asume que existe

        patrones: List[str] = []

        if nivel == "L1B":
            # Mantén tu lógica actual para L1B con bandas
            # ...existing code...
            pass
        elif nivel == "L2":
            # USAR patrones por producto (bandas solo para CMI)
            patrones.extend(
                list(
                    self._iter_patrones_l2(
                        productos=productos,
                        dominio=dominio,
                        bandas=query.get("bandas") or [],
                        sat_code=sat_code,
                        ts_inicio=ts_inicio,
                        ts_fin=ts_fin,
                    )
                )
            )
        else:
            # ...existing code...
            pass

        logging.debug(f"Patrones L2 generados: {patrones}")
        return patrones

# --- Funciones a nivel de módulo para ProcessPoolExecutor ---
# ProcessPoolExecutor requiere que las funciones que se ejecutan en otros procesos
# estén definidas a nivel superior del módulo, no como métodos de una clase.

def _process_safe_recover_file(archivo_fuente: Path, directorio_destino: Path, nivel: str, productos_solicitados_list: List[str], bandas_solicitadas_list: List[str]) -> List[Path]:
    """
    Función segura para procesos que procesa un único archivo .tgz.
    Verifica accesibilidad, y luego lo copia o extrae su contenido según la consulta.
    """
    archivos_recuperados = []
    
    # Normalizar entradas para facilitar las comprobaciones
    # Normalizar para aceptar string o lista
    if isinstance(productos_solicitados_list, str):
        productos_solicitados_list = [productos_solicitados_list]
    if isinstance(bandas_solicitadas_list, str):
        bandas_solicitadas_list = [bandas_solicitadas_list]
    productos_solicitados = set(p.upper() for p in (productos_solicitados_list or []))
    bandas_solicitadas = set(bandas_solicitadas_list or [])
    # Detectar si la consulta originalmente pidió 'ALL' aunque la request ya haya
    # sido expandida a la lista completa de bandas/productos. Usamos la config
    # para comparar contra el conjunto completo de valores válidos.
    productos_all_set = set(p.upper() for p in _SAT_CONFIG.VALID_PRODUCTS)
    bandas_all_set = set(_SAT_CONFIG.VALID_BANDAS)
    nivel_upper = (nivel or "").upper()

    # --- Lógica de decisión: Copiar .tgz completo vs. Extracción selectiva ---
    # Basado en las reglas definidas:
    # 1. L1b y bandas="ALL" -> Copiar .tgz completo.
    # 2. L2, bandas="ALL" y productos="ALL" -> Copiar .tgz completo.
    # 3. En todos los demás casos, se debe extraer selectivamente.
    
    # Considerar que se pidió 'ALL' cuando:
    # - la lista contiene literalmente 'ALL' (caso no expandido), o
    # - la lista equivale exactamente al conjunto válido completo (caso expandido)
    bandas_indican_all = ('ALL' in bandas_solicitadas) or (bandas_solicitadas == bandas_all_set)
    productos_indican_all = ('ALL' in productos_solicitados) or (productos_solicitados == productos_all_set)

    copiar_tgz_completo = (
        (nivel_upper == 'L1B' and bandas_indican_all) or
        (nivel_upper == 'L2' and bandas_indican_all and productos_indican_all)
    )

    if copiar_tgz_completo:
        shutil.copy(archivo_fuente, directorio_destino)
        archivos_recuperados.append(directorio_destino / archivo_fuente.name)
        return archivos_recuperados

    # --- Lógica de extracción selectiva ---
    try:
        with tarfile.open(archivo_fuente, "r:gz") as tar:
            miembros_del_tar = tar.getmembers()
            miembros_a_extraer = []

            # Determinar qué bandas usar para productos CMI
            # Si se pidió 'ALL' (o la lista ya fue expandida a todas las bandas),
            # se usan todas (01-16). Si no, se usan las especificadas.
            bandas_para_cmi = set(bandas_all_set) if (('ALL' in bandas_solicitadas) or (bandas_solicitadas == bandas_all_set)) else bandas_solicitadas

            # Iterar sobre cada archivo dentro del .tgz
            for miembro in miembros_del_tar:
                if not miembro.isfile():
                    continue

                # Lógica para L1b: extraer si la banda está en la lista solicitada
                if nivel_upper == 'L1B' and any(f"C{b}_" in miembro.name for b in bandas_solicitadas):
                    miembros_a_extraer.append(miembro)
                    continue

                # Lógica para L2: más compleja
                if nivel_upper == 'L2':
                    # Extraer si el producto está en la lista (o si se pidió 'ALL' productos)
                    if 'ALL' in productos_solicitados or any(f"-L2-{p.upper()}" in miembro.name for p in productos_solicitados):
                        # Si es un producto CMI, verificar también la banda
                        if 'CMI' in miembro.name and not any(f"C{b}_" in miembro.name for b in bandas_para_cmi):
                            continue  # Es CMI pero no de la banda correcta, saltar
                        miembros_a_extraer.append(miembro)
            
            if miembros_a_extraer:
                tar.extractall(path=directorio_destino, members=miembros_a_extraer)
                for miembro in miembros_a_extraer:
                    archivos_recuperados.append(directorio_destino / miembro.name)
            
            if not miembros_a_extraer:
                raise FileNotFoundError(f"No se encontraron archivos internos que coincidieran con la solicitud en {archivo_fuente.name}")

    except (tarfile.ReadError, tarfile.ExtractError, FileNotFoundError) as e:
        logging.error(f"❌ Error al procesar el archivo tar {archivo_fuente.name} (posiblemente corrupto): {e}")
        raise

    return archivos_recuperados