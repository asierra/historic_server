import logging
import os
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
import horarios
from settings import settings

# Instanciar configuración para referenciar listas válidas (bandas/productos)
_SAT_CONFIG = SatelliteConfigGOES()


# Expresión regular para extraer el timestamp de inicio del nombre de archivo
_FILENAME_TIMESTAMP_RE = re.compile(r'_s(\d{4})(\d{3})(\d{2})(\d{2}).*')


def filter_files_by_time(archivos_nc: list, fecha_jjj: str, horarios_list: list) -> list:
    """
    Filtra archivos NetCDF por fecha juliana y rango horario.
    Compatible con archivos S3 (string) y rutas locales NetCDF.
    """
    archivos_filtrados = []
    for archivo in archivos_nc:
        nombre = archivo.name if hasattr(archivo, "name") else archivo
        match = _FILENAME_TIMESTAMP_RE.search(nombre)
        if match:
            anio, dia_juliano, hora, minuto = match.groups()
            if anio + dia_juliano == fecha_jjj:
                archivo_min = horarios.minuto_de_archivo(hora, minuto)
                if horarios.alguna_contiene(horarios_list, archivo_min):
                    archivos_filtrados.append(archivo)
    return archivos_filtrados

# Timestamp de inicio en los dos formatos de nombre que hay en Lustre:
# `...G19-s20260760746.tgz` (árbol semanal) y `..._G19_s20262930001177_...nc`
# (árbol diario, y también S3). Captura AAAA, JJJ, HH, MM.
_LUSTRE_TIMESTAMP_RE = re.compile(r'[-_]s(\d{4})(\d{3})(\d{2})(\d{2})')

# Letra de dominio con la que terminan las carpetas de producto del árbol
# diario de L2 (`ACHAC`, `CMIPF-C13`).
_LETRA_DOMINIO = {"conus": "C", "fd": "F"}


# --- Clase para recuperación local (Lustre) ---
class LustreRecoverFiles:
    """Busca archivos en los dos árboles de Lustre.

    - **Semanal** (`SOURCE_PATH`, p. ej. /depot/goes16):
      `{sensor}/{nivel}/{dominio}/AAAA/SS/*.tgz`, con SS = semana del año.
      Es el acervo histórico.
    - **Diario** (`SOURCE_PATH_DIARIO`, p. ej. /depot/goes-east):
      `{nivel}/{sensor}/{dominio}/AAAA/JJJ/`, con `.nc` sueltos. En L1b están
      todas las bandas juntas; en L2 hay una subcarpeta por producto
      (`ACHAC`, `CMIPC-C13`). Rige desde `LAYOUT_DIARIO_DESDE_ANIO`.

    Desde ese año se busca primero en el diario y, si el día no está ahí,
    en el semanal: mientras dura la migración hay días que sólo existen en
    el árbol viejo. Una vez migrado todo, ese respaldo deja de encontrar
    nada y no estorba.
    """

    def __init__(self, source_data_path: str, logger, source_path_diario: Optional[str] = None, diario_desde_anio: Optional[int] = None):
        self.source_data_path = Path(source_data_path)
        self.source_path_diario = Path(source_path_diario or settings.source_path_diario)
        self.diario_desde_anio = diario_desde_anio or settings.layout_diario_desde_anio
        self.logger = logger

    def raices_para(self, query_dict: Dict) -> List[Path]:
        """Las raíces que una consulta puede tocar, según sus fechas."""
        anios = {int(f[:4]) for f in query_dict.get('fechas', {})}
        raices = []
        if any(a >= self.diario_desde_anio for a in anios):
            raices.append(self.source_path_diario)
        # El semanal siempre puede hacer falta: para años previos, o como
        # respaldo de un día que todavía no se migra.
        raices.append(self.source_data_path)
        return raices

    def build_base_path_diario(self, query_dict: Dict) -> Path:
        base_path = self.source_path_diario
        base_path /= query_dict.get('nivel', 'l1b').lower()
        base_path /= query_dict.get('sensor', 'abi').lower()
        if query_dict.get('dominio'):
            base_path /= query_dict['dominio'].lower()
        return base_path

    def find_files_for_day_diario(self, query_dict: Dict, fecha_jjj: str) -> Optional[List[Path]]:
        """Los `.nc` del día en el árbol diario que piden bandas y productos.

        Devuelve None si el día no está en el árbol (para caer al semanal) y
        una lista, quizá vacía, si está. La selección por banda/producto se
        hace aquí, por nombre: lo que antes decidía qué miembros extraer del
        .tgz ahora decide qué archivos o carpetas leer.
        """
        directorio_dia = self.build_base_path_diario(query_dict) / fecha_jjj[:4] / fecha_jjj[4:]
        if not directorio_dia.is_dir():
            return None

        nivel = (query_dict.get('nivel') or '').upper()
        bandas = {str(b) for b in (query_dict.get('bandas') or [])}
        todas_las_bandas = not bandas or 'ALL' in bandas

        if nivel == 'L1B':
            return [
                f for f in directorio_dia.iterdir()
                if f.suffix == '.nc' and (todas_las_bandas or any(f"C{b}_" in f.name for b in bandas))
            ]

        productos = {str(p).upper() for p in (query_dict.get('productos') or [])}
        todos_los_productos = 'ALL' in productos
        letra = _LETRA_DOMINIO.get((query_dict.get('dominio') or '').lower(), '')
        archivos = []
        for carpeta in directorio_dia.iterdir():
            if not carpeta.is_dir():
                continue
            # `CMIPC-C13` -> producto `CMIP`, banda `13`; `ACHAC` -> `ACHA`.
            base, _, banda = carpeta.name.partition('-C')
            producto = base[:-1] if letra and base.endswith(letra) else base
            if not (todos_los_productos or producto in productos):
                continue
            # La banda sólo filtra en CMI*; en DMW es parte del producto y la
            # consulta no la pide (main.py rechaza bandas fuera de CMI*).
            if banda and producto.startswith('CMI') and not todas_las_bandas and banda not in bandas:
                continue
            archivos.extend(f for f in carpeta.iterdir() if f.suffix == '.nc')
        return archivos

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
        ventanas = []
        for horario_str in horarios_list:
            try:
                horarios.parsear(horario_str)
            except ValueError:
                self.logger.warning(f"Formato de horario inválido para {fecha_jjj}: {horario_str}. Se omite.")
                continue
            ventanas.append(horario_str)
        if not ventanas:
            return []
        self.logger.debug(f"    Filtrando por rangos horarios: {ventanas}")

        archivos_filtrados_dia = []
        for archivo in archivos_candidatos:
            # sAAAAJJJHHMM, con `-s` en los .tgz y `_s` en los .nc.
            m = _LUSTRE_TIMESTAMP_RE.search(archivo.name)
            if not m or m.group(1) + m.group(2) != fecha_jjj:
                continue
            try:
                archivo_min = horarios.minuto_de_archivo(m.group(3), m.group(4))
            except ValueError:
                continue
            if horarios.alguna_contiene(ventanas, archivo_min):
                archivos_filtrados_dia.append(archivo)
        return archivos_filtrados_dia

    def discover_and_filter_files(self, query_dict: Dict) -> List[Path]:
        archivos_encontrados_set = set()
        base_path = self.build_base_path(query_dict)
        for fecha_jjj, horarios_list in query_dict.get('fechas', {}).items():
            archivos_candidatos_dia = None
            if int(fecha_jjj[:4]) >= self.diario_desde_anio:
                archivos_candidatos_dia = self.find_files_for_day_diario(query_dict, fecha_jjj)
            if archivos_candidatos_dia is None:
                archivos_candidatos_dia = self.find_files_for_day(base_path, fecha_jjj)
            if not archivos_candidatos_dia:
                continue
            archivos_filtrados = self.filter_files_by_time(archivos_candidatos_dia, fecha_jjj, horarios_list)
            archivos_encontrados_set.update(archivos_filtrados)
        return sorted(list(archivos_encontrados_set))

    def scan_existing_files(self, archivos_a_procesar: List[Path], destino: Path) -> List[Path]:
        """Quita lo que ya está en destino, para no copiarlo de nuevo al retomar.

        Sólo aplica a los `.nc` sueltos, y por nombre exacto: las 16 bandas de
        un instante comparten timestamp, así que comparar por timestamp daría
        por copiadas C02-C16 en cuanto existiera C01. Un `.tgz` siempre se
        reprocesa —de él puede haberse extraído sólo una parte—, que es lo que
        ya pasaba antes: la versión anterior buscaba `_s` y los nombres de los
        .tgz llevan `-s`, así que nunca saltaba ninguno.
        """
        if not destino.exists():
            return archivos_a_procesar
        existentes = set()
        for entrada in os.scandir(destino):
            try:
                if entrada.is_file(follow_symlinks=False) and entrada.stat().st_size > 0:
                    existentes.add(entrada.name)
            except OSError:
                continue
        return [
            a for a in archivos_a_procesar
            if not (a.suffix == '.nc' and a.name in existentes)
        ]

# --- Clase principal orquestadora ---
class RecoverFiles:
    def __init__(self, db: ConsultasDatabase, source_data_path: str, base_download_path: str, executor, s3_enabled: Optional[bool] = None, lustre_enabled: Optional[bool] = None, max_workers: Optional[int] = None, file_processing_timeout_seconds: Optional[int] = None, source_path_diario: Optional[str] = None):
        self.db = db
        self.source_data_path = Path(source_data_path)
        self.base_download_path = Path(base_download_path)
        self.logger = logging.getLogger(__name__)
        self.executor = executor
        
        self.s3_enabled = settings.s3_enabled if s3_enabled is None else s3_enabled
        self.lustre_enabled = settings.lustre_enabled if lustre_enabled is None else lustre_enabled
        self.FILE_PROCESSING_TIMEOUT_SECONDS = file_processing_timeout_seconds or settings.file_processing_timeout_seconds

        self.S3_RETRY_ATTEMPTS = settings.S3_RETRY_ATTEMPTS
        self.S3_RETRY_BACKOFF_SECONDS = settings.S3_RETRY_BACKOFF_SECONDS
        self.GOES19_OPERATIONAL_DATE = datetime.fromisoformat(settings.goes19_operational_date).replace(tzinfo=timezone.utc)
        self.lustre = LustreRecoverFiles(source_data_path, self.logger, source_path_diario=source_path_diario)

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

            # ¿Está Lustre ahí? Se comprueba la raíz una vez, antes de recorrer
            # días. No es una comprobación de más: /depot se desmonta durante
            # semanas por cortes de corriente y discos dañados, y hasta ahora eso
            # era indistinguible de «no tenemos esa semana» —cada día pedido
            # encontraba su directorio ausente y avisaba lo mismo—. La consulta
            # se sirve igual desde S3, que es lo correcto cuando la avería dura
            # semanas; lo que cambia es que ahora queda escrito en el reporte,
            # para que dentro de seis meses se sepa leer por qué `lustre` da 0.
            # Con dos árboles (semanal y diario) se miran las raíces que esta
            # consulta puede tocar: basta con que una esté para buscar, pero
            # la que falte se avisa, porque sus días se irán a S3.
            if not self.lustre_enabled:
                estado_lustre = "deshabilitado"
            else:
                raices = self.lustre.raices_para(query_dict)
                ausentes = [r for r in raices if not r.exists()]
                if len(ausentes) < len(raices):
                    estado_lustre = "ok"
                    for r in ausentes:
                        self.logger.error(
                            f"❌ Raíz de Lustre ausente: {r}. Los días que vivan en ese "
                            "árbol se servirán desde S3."
                        )
                else:
                    estado_lustre = "no_disponible"
                    self.logger.error(
                        f"❌ Lustre no disponible: no existe ninguna de sus raíces "
                        f"({', '.join(str(r) for r in raices)}). Probablemente el volumen "
                        "no está montado. La consulta se servirá íntegra desde S3, más "
                        "lenta. Esto NO significa que falten datos."
                    )

            # --- Optimización: Separar productos locales de los exclusivos de S3 ---
            productos_req_originales = query_dict.get('productos', []) or []
            s3_only_products = set(_SAT_CONFIG.S3_ONLY_PRODUCTS)
            
            productos_para_lustre = [p for p in productos_req_originales if p.upper() not in s3_only_products]
            query_para_lustre = query_dict.copy()
            query_para_lustre['productos'] = productos_para_lustre
            # --------------------------------------------------------------------

            if estado_lustre == "ok":
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
            if self.s3_enabled:
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
                # Excluir de las fallas locales los archivos que S3 sí recuperó,
                # para no contarlos como fallidos en el mensaje final.
                s3_recuperados_nombres = {f.name for f in s3_recuperados}
                objetivos_fallidos_final = (
                    [f for f in objetivos_fallidos_local if f.name not in s3_recuperados_nombres]
                    + list(objetivos_fallidos_s3)
                )
            else:
                s3_recuperados = []
                objetivos_fallidos_final = objetivos_fallidos_local

            # 6. Generar reporte final
            # Usar scandir para obtener nombre + tamaño en una sola pasada del OS,
            # evitando stat() individual por cada archivo (crítico con cientos de miles).
            dest_entries = [e for e in os.scandir(directorio_destino) if e.is_file(follow_symlinks=False)]
            self.db.actualizar_estado(consulta_id, "procesando", 95, "Generando reporte final")
            
            # Obtener la consulta para acceder al timestamp de creación
            consulta_db = self.db.obtener_consulta(consulta_id)
            timestamp_creacion = consulta_db.get("timestamp_creacion") if consulta_db else datetime.now().isoformat()

            resultados_finales = self._generar_reporte_final(
                consulta_id, dest_entries, s3_recuperados, directorio_destino, objetivos_fallidos_final, query_dict, timestamp_creacion, estado_lustre
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
                # archivo_fallido puede ser Path (Lustre) o str (S3 fallidos)
                nombre_fallido = archivo_fallido.name if hasattr(archivo_fallido, 'name') else archivo_fallido
                # `-s` en los .tgz, `_s` en los .nc (Lustre diario y S3).
                m = _LUSTRE_TIMESTAMP_RE.search(nombre_fallido)
                if not m:
                    continue
                fecha_fallida_dt = datetime.strptime(''.join(m.groups()), '%Y%j%H%M')
                fecha_fallida_ymd = fecha_fallida_dt.strftime('%Y%m%d')

                # 2. Encontrar la clave de fecha y el rango horario originales.
                for fecha_key_original, horarios_list in original_fechas.items():
                    # Comprobar si la fecha del archivo está dentro del rango de la clave (ej. "20230101-20230105")
                    start_date_str = fecha_key_original.split('-')[0]
                    end_date_str = fecha_key_original.split('-')[-1]
                    if not (start_date_str <= fecha_fallida_ymd <= end_date_str):
                        continue

                    archivo_min = (fecha_fallida_dt.hour * 60) + fecha_fallida_dt.minute
                    for horario_rango in horarios_list:
                        # Comprobar si la hora del archivo está dentro del rango horario.
                        if horarios.alguna_contiene([horario_rango], archivo_min):
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

    def _generar_reporte_final(self, consulta_id: str, dest_entries: list, s3_recuperados: List[Path], directorio_destino: Path, objetivos_fallidos: List[Path], query_original: Dict, timestamp_creacion_iso: str, estado_lustre: str = "ok") -> Dict:
        """Genera el diccionario de resultados finales."""
        # --- Cálculo de la duración total del procesamiento ---
        timestamp_finalizacion = datetime.now()
        duracion_str = "N/A"
        try:
            ts_inicio = datetime.fromisoformat(timestamp_creacion_iso)
            delta = timestamp_finalizacion - ts_inicio
            duracion_str = str(delta).split('.')[0]
        except (ValueError, TypeError):
            self.logger.warning(f"No se pudo calcular la duración para la consulta {consulta_id} debido a un timestamp inválido.")

        from collections import defaultdict as _dd

        def _extraer_producto_base(nombre: str) -> str:
            try:
                i = nombre.index('-L2-') + 4
                j = nombre.index('-M', i)
                seg = nombre[i:j]
                if seg.endswith('C') or seg.endswith('F'):
                    seg = seg[:-1]
                elif seg.endswith('M1') or seg.endswith('M2'):
                    seg = seg[:-2]
                alias = {
                    'CODD': 'COD', 'CODN': 'COD', 'COD': 'COD',
                    'CPSD': 'CPS', 'CPSN': 'CPS', 'CPS': 'CPS',
                    'VAAF': 'VAA', 'VAA': 'VAA',
                }
                return alias.get(seg, seg)
            except Exception:
                return 'UNKNOWN'

        # Conjunto de nombres S3 para clasificación rápida O(1)
        s3_names_set = {p.name for p in s3_recuperados}
        s3_names_full = [p.name for p in s3_recuperados]

        # Recorrido único: tamaño total + conteos por producto en una sola pasada
        # DirEntry.stat() usa el resultado cacheado del scandir — sin syscalls extra
        total_bytes = 0
        lustre_names_full = []
        conteo_total_por_producto = _dd(int)
        conteo_s3_por_producto = _dd(int)

        for entry in dest_entries:
            nombre = entry.name
            total_bytes += entry.stat(follow_symlinks=False).st_size
            prod = _extraer_producto_base(nombre)
            if prod != 'UNKNOWN':
                conteo_total_por_producto[prod] += 1
            if nombre in s3_names_set:
                if prod != 'UNKNOWN':
                    conteo_s3_por_producto[prod] += 1
            else:
                lustre_names_full.append(nombre)

        tamaño_mb = round(total_bytes / (1024 * 1024), 2)

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
                    "total": len(lustre_names_full),
                    # 'ok' | 'deshabilitado' | 'no_disponible'. Sin esto, un total
                    # de 0 es ambiguo: puede ser que el archivo no tuviera esas
                    # fechas o que el volumen estuviera caído, y son cosas muy
                    # distintas a la hora de interpretar el resultado después.
                    "estado": estado_lustre
                },
                "s3": {
                    "archivos": s3_list,
                    "total": len(s3_names_full)
                }
            },
            "conteo_por_producto": dict(sorted(conteo_total_por_producto.items())),
            "conteo_por_producto_s3": dict(sorted(conteo_s3_por_producto.items())),
            "total_archivos": len(dest_entries),
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

# --- Funciones a nivel de módulo para ProcessPoolExecutor ---
# ProcessPoolExecutor requiere que las funciones que se ejecutan en otros procesos
# estén definidas a nivel superior del módulo, no como métodos de una clase.

def _process_safe_recover_file(archivo_fuente: Path, directorio_destino: Path, nivel: str, productos_solicitados_list: List[str], bandas_solicitadas_list: List[str]) -> List[Path]:
    """
    Función segura para procesos que procesa un único archivo de Lustre.
    Un `.nc` del árbol diario ya viene seleccionado por banda/producto y sólo
    se copia; un `.tgz` del semanal se copia entero o se extrae en parte
    según la consulta.
    """
    if archivo_fuente.suffix == '.nc':
        shutil.copy(archivo_fuente, directorio_destino)
        return [directorio_destino / archivo_fuente.name]

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