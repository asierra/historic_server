import os
import logging
import shutil
import tarfile
from typing import List, Dict, Optional, NamedTuple
from datetime import datetime, timedelta, timezone
from pathlib import Path
import concurrent.futures
import s3fs

from database import ConsultasDatabase

from collections import defaultdict
 
import time
class RecoverFiles:
    """
    Atiende solicitudes de recuperación de archivos de datos desde un almacenamiento local.
    """
    def __init__(self, db: ConsultasDatabase,
        source_data_path: str = "/depot/goes16", base_download_path: str = "/data/tmp",
        s3_fallback_enabled: bool = True, executor: concurrent.futures.ThreadPoolExecutor = None):
        self.db = db
        self.source_data_path = Path(source_data_path)
        self.base_download_path = Path(base_download_path)
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"📂 Inicializando RecoverFiles.")
        self.executor = executor
        self.logger.info(f"   - Usando executor compartido con max_workers={executor._max_workers}")
        self.logger.info(f"   - Origen de datos (Lustre): {self.source_data_path}")
        self.logger.info(f"   - Directorio de descargas: {self.base_download_path}")
        self.logger.info(f"   - Fallback a S3: {'Activado' if s3_fallback_enabled else 'Desactivado'}")
        # --- Configuración para reintentos ---
        self.S3_RETRY_ATTEMPTS = 3
        self.S3_RETRY_BACKOFF_SECONDS = 2
        self.logger.info(f"   - Reintentos S3: {self.S3_RETRY_ATTEMPTS} intentos con backoff inicial de {self.S3_RETRY_BACKOFF_SECONDS}s")

        self.s3_fallback_enabled = s3_fallback_enabled

    # --- Constantes de Configuración de Satélites ---
    # TODO: Actualizar esta fecha con la fecha oficial en que GOES-19 se vuelve operacional como GOES-EAST.
    # Se asume que la fecha está en UTC.
    GOES19_OPERATIONAL_DATE = datetime(2025, 4, 1, tzinfo=timezone.utc)

    # Definir la clase anidada aquí, al principio de la clase, para que esté
    # disponible para todas las anotaciones de tipo de los métodos.
    class ObjetivoBusqueda(NamedTuple):
        """Estructura para un archivo potencial que se debe encontrar."""
        directorio_semana: Path
        patron_busqueda: str
        fecha_original: str  # YYYYMMDD o YYYYMMDD-YYYYMMDD
        horario_original: str # HH:MM o HH:MM-HH:MM

    def _scan_existing_files(self, archivos_a_procesar: List[Path], destino: Path) -> List[Path]:
        """
        Escanea el directorio de destino en busca de archivos ya recuperados
        y devuelve solo la lista de archivos que faltan por procesar.
        """
        if not destino.exists() or not any(destino.iterdir()):
            return archivos_a_procesar

        self.logger.info(f"🔍 Escaneando {destino} en busca de archivos ya recuperados...")
        
        # Crear un set de los nombres de archivo ya existentes para una búsqueda rápida.
        # Esto funciona tanto para .tgz como para .nc extraídos.
        archivos_existentes = {f.name for f in destino.iterdir()}
        
        archivos_pendientes = []
        for archivo_fuente in archivos_a_procesar:
            if archivo_fuente.name not in archivos_existentes:
                archivos_pendientes.append(archivo_fuente)

        num_recuperados = len(archivos_a_procesar) - len(archivos_pendientes)
        self.logger.info(f"📊 Escaneo completo. {num_recuperados} archivos ya recuperados, {len(archivos_pendientes)} pendientes.")
        return archivos_pendientes

    def procesar_consulta(self, consulta_id: str, query_dict: Dict):
        try:
            self.logger.info(f" Atendiendo solicitud {consulta_id}")

            # 1. Preparar entorno
            directorio_destino = self.base_download_path / consulta_id
            directorio_destino.mkdir(exist_ok=True, parents=True)
            self.db.actualizar_estado(consulta_id, "procesando", 10, "Preparando entorno")

            # 2. Descubrir y filtrar archivos que coinciden con la consulta
            archivos_a_procesar_local = self._discover_and_filter_files(query_dict)
            self.logger.info(f"🔎 Se encontraron {len(archivos_a_procesar_local)} archivos potenciales en el almacenamiento local.")

            # 3. Escanear el directorio de destino para no reprocesar archivos
            archivos_pendientes_local = self._scan_existing_files(archivos_a_procesar_local, directorio_destino)
            total_pendientes = len(archivos_pendientes_local)
            
            if not archivos_pendientes_local:
                self.logger.info("👍 No hay objetivos pendientes, todos los archivos ya fueron recuperados.")
            
            self.db.actualizar_estado(consulta_id, "procesando", 20, f"Identificados {total_pendientes} archivos pendientes de procesar.")

            objetivos_fallidos_local = []
            
            # 4. Procesar cada objetivo PENDIENTE en paralelo
            if archivos_pendientes_local:
                # Ya no usamos 'with', usamos el executor global
                future_to_objetivo = {
                    self.executor.submit(self._recuperar_archivo, consulta_id, 20 + int(((i + 1) / total_pendientes) * 60), archivo_a_procesar, directorio_destino, query_dict): archivo_a_procesar
                    for i, archivo_a_procesar in enumerate(archivos_pendientes_local)
                }

                for i, future in enumerate(concurrent.futures.as_completed(future_to_objetivo)):
                    archivo_fuente = future_to_objetivo[future]
                    self.db.actualizar_estado(consulta_id, "procesando", None, f"Procesando archivo {i+1}/{total_pendientes}")

                    try:
                        future.result() # Esperar a que termine, el resultado es una lista de archivos que no necesitamos aquí
                    except Exception as e: # Catch exceptions from _process_single_objective
                        self.logger.error(f"❌ Error procesando el archivo {archivo_fuente.name}: {e}")
                        objetivos_fallidos_local.append(archivo_fuente)

            # 5. (Opcional) Intentar recuperar los fallidos desde S3
            if self.s3_fallback_enabled: # Siempre intentar S3 si está habilitado
                self.db.actualizar_estado(consulta_id, "procesando", 85, "Buscando archivos adicionales en S3.")
                s3_recuperados, objetivos_fallidos_final = self._recuperar_fallidos_desde_s3(
                    consulta_id, query_dict, directorio_destino
                )
            else:
                s3_recuperados = []
                objetivos_fallidos_final = objetivos_fallidos_local

            # 6. Generar reporte final
            # Scan the destination directory for all files (newly recovered + already existing)
            all_files_in_destination = [f for f in directorio_destino.iterdir() if f.is_file()]
            self.db.actualizar_estado(consulta_id, "procesando", 95, "Generando reporte final")
            # Pass all_files_in_destination to the report generator, it will classify them.
            resultados_finales = self._generar_reporte_final(consulta_id, all_files_in_destination, s3_recuperados, directorio_destino, objetivos_fallidos_final, query_dict)
            self.db.guardar_resultados(consulta_id, resultados_finales)

            self.logger.info(f"✅ Procesamiento completado para {consulta_id}")

        except Exception as e:
            self.logger.error(f"❌ Error procesando consulta {consulta_id}: {e}")
            self.db.actualizar_estado(consulta_id, "error", 0, f"Error: {str(e)}")
    
    def _discover_and_filter_files(self, query_dict: Dict) -> List[Path]:
        """
        Descubre todos los archivos en los directorios relevantes y los filtra
        según los rangos de tiempo de la consulta.
        """
        archivos_encontrados = []
        
        base_path = self.source_data_path
        for key in ['sensor', 'nivel', 'dominio']:
            if query_dict.get(key):
                base_path /= query_dict[key].lower()

        for fecha_jjj, horarios_list in query_dict.get('fechas', {}).items():
            año = fecha_jjj[:4]
            dia_del_año_int = int(fecha_jjj[4:])
            semana = (dia_del_año_int - 1) // 7 + 1
            directorio_semana = base_path / año / f"{semana:02d}"

            if not directorio_semana.exists():
                self.logger.warning(f"⚠️ Directorio no encontrado en Lustre: {directorio_semana}")
                continue

            # Listar todos los archivos .tgz en el directorio de la hora
            # Esto es más eficiente que iterar minuto a minuto
            archivos_candidatos = list(directorio_semana.glob(f"*-s{año}{dia_del_año_int:03d}*.tgz"))

            for horario_str in horarios_list:
                partes = horario_str.split('-')
                inicio_str, fin_str = partes[0], partes[1] if len(partes) > 1 else partes[0]
                
                # Convertir a timestamps para comparación numérica
                inicio_ts = int(f"{año}{dia_del_año_int:03d}{inicio_str.replace(':', '')}")
                fin_ts = int(f"{año}{dia_del_año_int:03d}{fin_str.replace(':', '')}")

                for archivo in archivos_candidatos:
                    try:
                        # Extraer el timestamp del nombre del archivo, ej: ..._s20200011901...
                        s_part_start_idx = archivo.name.find('_s')
                        if s_part_start_idx != -1:
                            # Tomar solo YYYYJJJHHMM (11 dígitos después de 's')
                            file_ts_str = archivo.name[s_part_start_idx + 2 : s_part_start_idx + 13]
                            file_ts = int(file_ts_str)

                            # Comprobar si el timestamp del archivo está en el rango
                            if inicio_ts <= file_ts <= fin_ts:
                                if archivo not in archivos_encontrados:
                                    archivos_encontrados.append(archivo)
                    except (ValueError, IndexError):
                        # Ignorar archivos con nombres mal formados
                        continue
        
        return archivos_encontrados

    def _get_sat_code_for_date(self, satellite_name: str, request_date: datetime) -> str:
        """
        Determina el código de satélite (G16, G19, etc.) basado en el nombre operacional
        y la fecha de la solicitud.
        """
        # Asegurarse de que la fecha de la solicitud tenga zona horaria para una comparación correcta.
        if request_date.tzinfo is None:
            request_date = request_date.replace(tzinfo=timezone.utc)

        if satellite_name == "GOES-EAST":
            # Si la fecha es posterior a la fecha de operación de GOES-19, usa G19. Si no, G16.
            return "G19" if request_date >= self.GOES19_OPERATIONAL_DATE else "G16"
        
        if satellite_name == "GOES-WEST":
            # Lógica similar podría aplicarse aquí si GOES-WEST cambia de satélite físico.
            # Por ahora, asumimos que es G18.
            return "G18"

        # Para nombres de satélite específicos como "GOES-16", "GOES-18", etc.
        if '-' in satellite_name:
            return f"G{satellite_name.split('-')[-1]}"
        
        return satellite_name # Fallback

    def _recuperar_archivo(self, consulta_id: str, progreso: int, archivo_fuente: Path, directorio_destino: Path, query_dict: Dict) -> List[Path]:
        """
        Procesa un único archivo .tgz: lo copia o extrae su contenido según la consulta.
        Devuelve una lista de rutas de los archivos finales en el destino.
        """
        archivos_recuperados = []
        
        # Determinar si se debe copiar el .tgz completo o extraer su contenido
        nivel = query_dict.get('nivel')
        bandas_solicitadas = query_dict.get('bandas', [])
        productos_solicitados = query_dict.get('productos')

        copiar_tgz_completo = False
        if nivel == 'L1b':
            # Si se piden todas las bandas de ABI, se copia el tgz.
            copiar_tgz_completo = len(bandas_solicitadas) == 16
        elif nivel == 'L2':
            # Si no se especifica una lista de productos, se asume que se quieren todos.
            # En ese caso, se copia el tgz completo.
            copiar_tgz_completo = not productos_solicitados

        # Si se pidieron todos los datos (bandas o productos), solo copiamos el .tgz
        if copiar_tgz_completo:
            self.db.actualizar_estado(consulta_id, "procesando", progreso, f"Copiando desde Lustre: {archivo_fuente.name}")
            self.logger.debug(f"📦 Copiando archivo completo: {archivo_fuente.name}")
            shutil.copy(archivo_fuente, directorio_destino)
            archivos_recuperados.append(directorio_destino / archivo_fuente.name)
            return archivos_recuperados
        
        # Si se pidió un subconjunto, abrir el .tgz para extraer selectivamente
        try:
            with tarfile.open(archivo_fuente, "r:gz") as tar: # Puede lanzar ReadError, etc.
                miembros_a_extraer = []
                for miembro in tar.getmembers():
                    if not miembro.isfile():
                        continue

                    extraer = False
                    if nivel == 'L1b':
                        # El formato correcto del nombre de archivo interno es M[Modo]C[Banda]_, ej. M6C13_
                        if any(f"C{banda}_" in miembro.name for banda in bandas_solicitadas):
                            extraer = True
                    elif nivel == 'L2':
                        if any(f"-L2-{producto}" in miembro.name for producto in productos_solicitados):
                            extraer = True
                    
                    if extraer:
                        miembros_a_extraer.append(miembro)
                
                if miembros_a_extraer:
                    self.db.actualizar_estado(consulta_id, "procesando", progreso, f"Extrayendo de: {archivo_fuente.name}")
                    self.logger.debug(f"🔎 Extrayendo {len(miembros_a_extraer)} archivos de {archivo_fuente.name}")
                    tar.extractall(path=directorio_destino, members=miembros_a_extraer) # También puede lanzar errores
                    for miembro in miembros_a_extraer:
                        archivos_recuperados.append(directorio_destino / miembro.name)
                
                # Si se pidió extracción pero no se encontró ningún miembro, es un fallo.
                if not miembros_a_extraer:
                    raise FileNotFoundError(f"No se encontraron archivos internos que coincidieran con la solicitud en {archivo_fuente.name}")

        except (tarfile.ReadError, tarfile.ExtractError, FileNotFoundError) as e:
            # Capturamos errores específicos de lectura/extracción (archivos corruptos/incompletos)
            # y relanzamos la excepción para que el objetivo se marque como fallido.
            self.logger.error(f"❌ Error al procesar el archivo tar {archivo_fuente.name} (posiblemente corrupto): {e}")
            raise # Relanzar la excepción es CRÍTICO para que el objetivo se marque como fallido.

        return archivos_recuperados

    def _generar_reporte_final(self, consulta_id: str, all_files_in_destination: List[Path], s3_recuperados: List[Path], directorio_destino: Path, objetivos_fallidos: List[Path], query_original: Dict) -> Dict:
        """Genera el diccionario de resultados finales."""
        # Separate files from Lustre and S3 based on their origin (S3 recovered files are explicitly tracked)
        lustre_files_for_report = [f for f in all_files_in_destination if f not in s3_recuperados]
        todos_los_archivos = all_files_in_destination # Total files are all files found in the destination
        total_bytes = sum(f.stat().st_size for f in todos_los_archivos if f.is_file())
        tamaño_mb = round(total_bytes / (1024 * 1024), 2)

        # Construir la consulta de recuperación con los objetivos que fallaron
        fechas_fallidas = defaultdict(list)
        # Esta lógica necesita ser repensada ya que ya no tenemos 'ObjetivoBusqueda'
        # Por ahora, la dejaremos vacía, ya que S3 debería encontrar lo que falta.
        # for obj in objetivos_fallidos:
        #     pass

        consulta_recuperacion = None
        if fechas_fallidas:
            # Opcional: añadir una nota sobre el origen de esta consulta
            # Reconstruir la consulta de recuperación a partir de la original,
            # pero convirtiendo las fechas julianas de los fallos de nuevo a YYYYMMDD.
            fechas_recuperacion_ymd = defaultdict(list)
            for fecha_jjj, horarios in fechas_fallidas.items():
                fecha_dt = datetime.strptime(fecha_jjj, "%Y%j")
                fecha_ymd = fecha_dt.strftime("%Y%m%d")
                fechas_recuperacion_ymd[fecha_ymd].extend(horarios)

            # Usar la solicitud original como base
            consulta_recuperacion = query_original.get('_original_request', {}).copy() or {}
            # Limpiar campos que no son parte de una solicitud
            consulta_recuperacion.pop('creado_por', None)
            # Reemplazar con las fechas fallidas
            consulta_recuperacion['fechas'] = dict(fechas_recuperacion_ymd)
            consulta_recuperacion['descripcion'] = f"Consulta de recuperación para la solicitud original {consulta_id}"

        return {
            "fuentes": {
                "lustre": {
                    "archivos": [f.name for f in lustre_files_for_report],
                    "total": len(lustre_files_for_report)
                },
                "s3": {
                    "archivos": [f.name for f in s3_recuperados],
                    "total": len(s3_recuperados)
                }
            },
            "total_archivos": len(todos_los_archivos),
            "tamaño_total_mb": tamaño_mb,
            "directorio_destino": str(directorio_destino),
            "timestamp_procesamiento": datetime.now().isoformat(),
            "consulta_recuperacion": consulta_recuperacion
        }

    def _recuperar_fallidos_desde_s3(self, consulta_id: str, query_dict: Dict, directorio_destino: Path) -> (List[Path], List[Path]):
        """ 
        Intenta descargar desde S3 los archivos que no se encontraron localmente.
        """
        from botocore.config import Config
        # Configurar timeouts para el cliente S3 para evitar que se quede colgado indefinidamente.
        # connect_timeout: tiempo para establecer la conexión.
        # read_timeout: tiempo de espera para recibir datos una vez conectado.
        s3 = s3fs.S3FileSystem(
            anon=True, 
            config_kwargs={'connect_timeout': 10, 'read_timeout': 30}
        )
        archivos_s3_recuperados = []
        objetivos_aun_fallidos = []

        # Construir el nombre del producto para la ruta S3
        sensor = query_dict.get('sensor', 'abi').upper()
        nivel = query_dict.get('nivel', 'L1b')
        productos_solicitados = query_dict.get('productos')

        # La lógica ahora es descubrir y filtrar en S3, similar a la local.
        # Esta es una implementación simplificada. Una versión completa replicaría
        # la lógica de _discover_and_filter_files para S3.
        # Por ahora, solo buscaremos los archivos que fallaron localmente.
        
        # TODO: Implementar una lógica de descubrimiento en S3.
        # Por ahora, devolvemos listas vacías para que el flujo no se rompa.

        return archivos_s3_recuperados, objetivos_aun_fallidos

    def _download_single_s3_objective(self, consulta_id: str, archivo_remoto_s3: str, directorio_destino: Path, s3_client: s3fs.S3FileSystem) -> Optional[Path]:
        """Helper function to download a single S3 objective, to be run in a thread pool."""
        last_exception = None
        for attempt in range(self.S3_RETRY_ATTEMPTS):
            try:
                nombre_archivo_local = Path(archivo_remoto_s3).name
                ruta_local_destino = directorio_destino / nombre_archivo_local
                
                self.db.actualizar_estado(consulta_id, "procesando", None, f"Descargando de S3 (Intento {attempt + 1}/{self.S3_RETRY_ATTEMPTS}): {nombre_archivo_local}")
                self.logger.info(f"⬇️ Descargando desde S3: {archivo_remoto_s3} -> {ruta_local_destino} (Intento {attempt + 1}/{self.S3_RETRY_ATTEMPTS})")
                s3_client.get(archivo_remoto_s3, str(ruta_local_destino))
                return ruta_local_destino # Éxito, salir de la función

            except Exception as e:
                last_exception = e
                self.logger.warning(f"⚠️ Falló el intento {attempt + 1}/{self.S3_RETRY_ATTEMPTS} para descargar {archivo_remoto_s3}: {e}")
                if attempt < self.S3_RETRY_ATTEMPTS - 1:
                    wait_time = self.S3_RETRY_BACKOFF_SECONDS * (2 ** attempt) # Backoff exponencial
                    self.logger.info(f"   Reintentando en {wait_time} segundos...")
                    time.sleep(wait_time)

        # Si todos los intentos fallaron, lanzar la última excepción capturada
        self.logger.error(f"❌ Fallaron todos los {self.S3_RETRY_ATTEMPTS} intentos para descargar desde S3 el archivo {archivo_remoto_s3}.")
        if last_exception:
            raise last_exception
        
        return None # Fallback en caso de que no haya habido excepción