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

    def procesar_consulta(self, consulta_id: str, query_dict: Dict):
        """
        Procesa una consulta de recuperación de archivos.
        Esta función es idempotente: si se vuelve a ejecutar para una consulta
        interrumpida, escaneará los archivos ya recuperados y solo procesará
        los que falten.
        """
        def _scan_existing_files(objetivos: List[self.ObjetivoBusqueda], destino: Path) -> (List[Path], List[self.ObjetivoBusqueda]):
            """
            Escanea el directorio de destino en busca de archivos ya recuperados.
            Devuelve una tupla: (lista_de_archivos_ya_recuperados, lista_de_objetivos_pendientes).
            """
            if not destino.exists():
                return [], objetivos

            self.logger.info(f"🔍 Escaneando {destino} en busca de archivos existentes...")
            archivos_existentes = {f.name for f in destino.iterdir()}
            objetivos_pendientes = []
            archivos_recuperados = []

            for obj in objetivos:
                # Construimos el nombre de archivo esperado a partir del patrón
                nombre_archivo_esperado = f"{obj.patron_busqueda}*.tgz" # Asumimos que el patrón es suficiente
                # Esta lógica es simplificada. Asume que si un .tgz con el timestamp existe, el objetivo está completo.
                # Una mejora sería verificar el contenido si se extrajeron archivos .nc.
                if any(Path(f).match(nombre_archivo_esperado) for f in archivos_existentes):
                    self.logger.debug(f"✅ Objetivo ya completado (archivo encontrado): {obj.patron_busqueda}")
                    archivos_recuperados.append(next(destino.glob(nombre_archivo_esperado)))
                else:
                    objetivos_pendientes.append(obj)
            
            self.logger.info(f"📊 Escaneo completo. {len(archivos_recuperados)} objetivos ya recuperados, {len(objetivos_pendientes)} pendientes.")
            return archivos_recuperados, objetivos_pendientes

        try:
            self.logger.info(f" Atendiendo solicitud {consulta_id}")

            # 1. Preparar entorno
            directorio_destino = self.base_download_path / consulta_id
            directorio_destino.mkdir(exist_ok=True, parents=True)
            self.db.actualizar_estado(consulta_id, "procesando", 10, "Preparando entorno")

            # 2. Determinar todos los archivos potenciales (objetivos)
            objetivos = self._generar_objetivos_de_busqueda(query_dict)
            self.logger.info(f"🔎 Se identificaron {len(objetivos)} archivos potenciales en total.")

            # 3. Escanear archivos existentes para reanudar el trabajo
            archivos_recuperados, objetivos_pendientes = _scan_existing_files(objetivos, directorio_destino)
            total_objetivos_pendientes = len(objetivos_pendientes)
            
            if not objetivos_pendientes: # If no pending objectives, all are recovered
                self.logger.info("👍 No hay objetivos pendientes, todos los archivos ya fueron recuperados.")
            
            self.db.actualizar_estado(consulta_id, "procesando", 20, f"Identificados {total_objetivos_pendientes} archivos pendientes de procesar.")

            objetivos_fallidos = []

            # 4. Procesar cada objetivo PENDIENTE en paralelo
            newly_recovered_from_lustre = [] # Collect files recovered in this run
            if objetivos_pendientes:
                # Ya no usamos 'with', usamos el executor global
                future_to_objetivo = {
                    self.executor.submit(self._process_single_objective, consulta_id, objetivo, directorio_destino, query_dict, idx, total_objetivos_pendientes): (objetivo, idx)
                    for idx, objetivo in enumerate(objetivos_pendientes) # Usar enumerate para obtener el índice 'idx'
                }

                for i, future in enumerate(concurrent.futures.as_completed(future_to_objetivo)):
                    objetivo, _ = future_to_objetivo[future] # Desempaquetar la tupla para obtener el objeto objetivo
                    # El progreso se calcula sobre los objetivos pendientes
                    progreso = 20 + int(((i + 1) / total_objetivos_pendientes) * 60)
                    self.db.actualizar_estado(consulta_id, "procesando", progreso, f"Buscando y recuperando archivo {i+1}/{total_objetivos_pendientes}")

                    try:
                        result = future.result() # (found_file_path, list_of_recovered_files)
                        if result and result[0]: # If file was found and processed
                            newly_recovered_from_lustre.extend(result[1])
                        else: # File not found or error during processing
                            objetivos_fallidos.append(objetivo)
                    except Exception as e: # Catch exceptions from _process_single_objective
                        self.logger.error(f"❌ Error procesando objetivo {objetivo.patron_busqueda}: {e}")
                        objetivos_fallidos.append(objetivo)

            # 5. (Opcional) Intentar recuperar los fallidos desde S3
            if self.s3_fallback_enabled and objetivos_fallidos:
                self.db.actualizar_estado(consulta_id, "procesando", 85, f"Intentando recuperar {len(objetivos_fallidos)} archivos faltantes desde S3.")
                s3_recuperados, objetivos_fallidos_final = self._recuperar_fallidos_desde_s3(
                    consulta_id, objetivos_fallidos, directorio_destino, query_dict
                )
                objetivos_fallidos = objetivos_fallidos_final # Actualizar la lista de fallidos
            else:
                s3_recuperados = []

            # 6. Generar reporte final
            # Scan the destination directory for all files (newly recovered + already existing)
            all_files_in_destination = [f for f in directorio_destino.iterdir() if f.is_file()]
            self.db.actualizar_estado(consulta_id, "procesando", 95, "Generando reporte final")
            # Pass all_files_in_destination to the report generator, it will classify them.
            resultados_finales = self._generar_reporte_final(consulta_id, all_files_in_destination, s3_recuperados, directorio_destino, objetivos_fallidos, query_dict)
            self.db.guardar_resultados(consulta_id, resultados_finales)

            self.logger.info(f"✅ Procesamiento completado para {consulta_id}")

        except Exception as e:
            self.logger.error(f"❌ Error procesando consulta {consulta_id}: {e}")
            self.db.actualizar_estado(consulta_id, "error", 0, f"Error: {str(e)}")
    
    def _process_single_objective(self, consulta_id: str, objetivo: ObjetivoBusqueda, directorio_destino: Path, query_dict: Dict, idx: int, total: int) -> Optional[tuple[Path, List[Path]]]:
        """
        Helper function to process a single objective, to be run in a thread pool.
        Returns (found_file_path, list_of_recovered_files) or None if not found/processed.
        """
        archivo_encontrado = self._buscar_archivo_para_objetivo(objetivo)
        if archivo_encontrado:
            try:
                # Pasamos el ID y el progreso para poder actualizar el estado desde dentro
                progreso_actual = 20 + int(((idx + 1) / total) * 60)
                nuevos_archivos = self._recuperar_archivo(consulta_id, progreso_actual, archivo_encontrado, directorio_destino, query_dict)
                return archivo_encontrado, nuevos_archivos
            except Exception as e:
                self.logger.error(f"❌ No se pudo procesar {archivo_encontrado}: {e}")
                return archivo_encontrado, [] # Indicate it was found but failed to process
        self.logger.warning(f"⚠️ No se encontró archivo local para '{objetivo.patron_busqueda}' en '{objetivo.directorio_semana}'")
        return None

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

    def _generar_objetivos_de_busqueda(self, query_dict: Dict) -> List['RecoverFiles.ObjetivoBusqueda']:
        """
        Genera una lista de todos los archivos que *deberían* existir según la consulta.
        """
        objetivos = []
        satelite = query_dict.get('satelite', 'GOES-16')
        sensor = query_dict.get('sensor', 'abi')
        nivel = query_dict.get('nivel', 'unknown')
        dominio = query_dict.get('dominio', 'fd')

        # Construir la ruta base de la consulta, incluyendo sensor, nivel y dominio.
        base_path = self.source_data_path
        for key in ['sensor', 'nivel', 'dominio']:
            if query_dict.get(key):
                # Forzar a minúsculas para coincidir con la estructura de directorios en Linux
                base_path /= query_dict[key].lower()

        for fecha_jjj, horarios_list in query_dict.get('fechas', {}).items():
            año = fecha_jjj[:4]
            dia_del_año_int = int(fecha_jjj[4:])
            semana = dia_del_año_int // 7 + 1

            # La ruta base ya incluye sensor/nivel/dominio, ahora añadimos año/semana
            directorio_semana = base_path / año / f"{semana:02d}"

            fecha_dt = datetime.strptime(fecha_jjj, "%Y%j")

            for horario_str in horarios_list:
                partes = horario_str.split('-') # Se re-añade este bloque
                inicio_str, fin_str = partes[0], partes[1] if len(partes) > 1 else partes[0]
                inicio_dt = fecha_dt.replace(hour=int(inicio_str[:2]), minute=int(inicio_str[3:])) # Se re-añade este bloque
                fin_dt = fecha_dt.replace(hour=int(fin_str[:2]), minute=int(fin_str[3:])) # Se re-añade este bloque
                current_dt = inicio_dt # Se re-añade este bloque
                while current_dt <= fin_dt:
                    # Re-introducir el filtro de minutos basado en el dominio para generar
                    # solo los objetivos que tienen probabilidad de existir.
                    # FD (Full Disk) genera cada 10 minutos (00, 10, 20...).
                    # CONUS genera cada 5 minutos (en los minutos que terminan en 1 o 6).
                    # Como fallback, si el dominio no es ninguno de estos, se asume cada 10 min.
                    
                    should_generate = False
                    # Para productos L2, el intervalo de minutos en el nombre del archivo puede diferir del intervalo de escaneo nominal.
                    # Basado en los logs de S3 para ACTPF, parece que son intervalos de 10 minutos para el timestamp 's'.
                    # Esto debería ser configurable o derivado de forma más inteligente.
                    productos_l2_10min = ['ACTP', 'CMIP']
                    if nivel == 'L2' and query_dict.get('productos') and any(p in query_dict['productos'] for p in productos_l2_10min):
                        # Regla específica para ciertos productos L2: asumir intervalos de 10 minutos en el nombre del archivo
                        should_generate = (current_dt.minute % 10 == 0)
                    else: # Lógica por defecto basada en el dominio para otros niveles/productos
                        if dominio == 'fd':
                            should_generate = (current_dt.minute % 10 == 0)
                        elif dominio == 'conus':
                            should_generate = (current_dt.minute % 5 == 1)
                        else: # Fallback para dominios no especificados o diferentes
                            should_generate = (current_dt.minute % 10 == 0)

                    if should_generate:
                        # Determinar el código de satélite correcto para esta fecha específica
                        sat_code = self._get_sat_code_for_date(satelite, current_dt)

                        # Construir el timestamp sin segundos (YYYYJJJHHMM)
                        timestamp_archivo = f"s{current_dt.strftime('%Y%j%H%M')}"
                        # Construir el patrón de búsqueda
                        if nivel == 'L1b' and sensor == 'abi':
                            # Formato: OR_ABI-L1b-RadF-M6_G16_s2024123120000.tgz
                            patron_busqueda = f"OR_{sensor.upper()}-{nivel}-RadF-M6_{sat_code}_{timestamp_archivo}"
                        elif nivel == 'L2' and sensor == 'abi':
                            # Para L2, el producto no está en el nombre del .tgz, pero sí el modo (M6)
                            # Formato: OR_ABI-L2-ACMF-M6_G16_s2024123120000.tgz
                            # Usamos un comodín para el producto: OR_ABI-L2-*-M6_G16_...
                            patron_busqueda = f"OR_{sensor.upper()}-{nivel}-*-M6_{sat_code}_{timestamp_archivo}"
                        else:
                            # Fallback genérico, puede ser menos preciso
                            patron_busqueda = f"OR_{sensor.upper()}-{nivel}-*_{sat_code}_{timestamp_archivo}"

                        # La fecha original para la reconstrucción de fallos es la clave YYYYJJJ del bucle actual.
                        # El horario original es el rango o valor que estamos iterando.
                        objetivos.append(self.ObjetivoBusqueda(
                            directorio_semana=directorio_semana,
                            patron_busqueda=patron_busqueda,
                            fecha_original=fecha_jjj, # Usamos la fecha juliana
                            horario_original=horario_str
                        ))
                    current_dt += timedelta(minutes=1)
        return objetivos
    def _buscar_archivo_para_objetivo(self, objetivo: ObjetivoBusqueda) -> Optional[Path]:
        """Busca en disco un archivo que coincida con el patrón del objetivo."""
        if not objetivo.directorio_semana.exists():
            return None
        
        # Extraer el timestamp del patrón de búsqueda (ej. 's20252462220')
        try:
            # Construir un patrón glob que sea específico pero flexible
            # Ejemplo: OR_ABI-L1b-RadF-M6_G16_s20241231200 -> *-s20241231200*.tgz
            # El patrón de búsqueda ya contiene el timestamp, solo añadimos comodines y extensión.
            glob_pattern = f"{objetivo.patron_busqueda}*.tgz"
            
            # find() es un generador, next() obtiene el primer elemento o None
            return next(objetivo.directorio_semana.glob(glob_pattern), None)
        except IndexError:
            self.logger.error(f"Patrón de búsqueda inválido, no se pudo extraer el timestamp: {objetivo.patron_busqueda}")
            return None

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

    def _generar_reporte_final(self, consulta_id: str, all_files_in_destination: List[Path], s3_recuperados: List[Path], directorio_destino: Path, objetivos_fallidos: List[ObjetivoBusqueda], query_original: Dict) -> Dict:
        """Genera el diccionario de resultados finales."""
        # Separate files from Lustre and S3 based on their origin (S3 recovered files are explicitly tracked)
        lustre_files_for_report = [f for f in all_files_in_destination if f not in s3_recuperados]
        todos_los_archivos = all_files_in_destination # Total files are all files found in the destination
        total_bytes = sum(f.stat().st_size for f in todos_los_archivos if f.is_file())
        tamaño_mb = round(total_bytes / (1024 * 1024), 2)

        # Construir la consulta de recuperación con los objetivos que fallaron
        fechas_fallidas = defaultdict(list)
        for obj in objetivos_fallidos:
            # Evitar duplicados
            if obj.horario_original not in fechas_fallidas[obj.fecha_original]:
                fechas_fallidas[obj.fecha_original].append(obj.horario_original)

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

    def _recuperar_fallidos_desde_s3(self, consulta_id: str, objetivos_fallidos: List[ObjetivoBusqueda], directorio_destino: Path, query_dict: Dict) -> (List[Path], List[ObjetivoBusqueda]):
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

        if objetivos_fallidos:
            # Para L2, cada producto puede estar en un directorio S3 diferente.
            # Para L1b, todos los objetivos usan el mismo producto S3.
            if nivel == 'L1b':
                producto_s3 = f"{sensor}-{nivel}-RadF"
                future_to_objetivo_s3 = {
                    self.executor.submit(self._download_single_s3_objective, consulta_id, objetivo, directorio_destino, s3, producto_s3): objetivo
                    for objetivo in objetivos_fallidos
                }
            elif nivel == 'L2' and productos_solicitados:
                future_to_objetivo_s3 = {}
                for producto in productos_solicitados:
                    producto_s3 = f"{sensor}-{nivel}-{producto}F"
                    for objetivo in objetivos_fallidos:
                        # Evita reenviar el mismo objetivo si ya está en la cola para otro producto
                        if objetivo not in future_to_objetivo_s3.values():
                             future_to_objetivo_s3[self.executor.submit(self._download_single_s3_objective, consulta_id, objetivo, directorio_destino, s3, producto_s3)] = objetivo
            else:
                self.logger.error("No se puede determinar el producto S3 para la consulta L2 sin productos especificados.")
                return [], objetivos_fallidos

            for future in concurrent.futures.as_completed(future_to_objetivo_s3):
                objetivo = future_to_objetivo_s3[future]
                try:
                    ruta_local_destino = future.result()
                    if ruta_local_destino:
                        archivos_s3_recuperados.append(ruta_local_destino)
                    else:
                        objetivos_aun_fallidos.append(objetivo)
                except Exception as e:
                    self.logger.error(f"❌ Error durante la recuperación desde S3 para el objetivo {objetivo.patron_busqueda}: {e}")
                    objetivos_aun_fallidos.append(objetivo)

        return archivos_s3_recuperados, objetivos_aun_fallidos

    def _download_single_s3_objective(self, consulta_id: str, objetivo: ObjetivoBusqueda, directorio_destino: Path, s3_client: s3fs.S3FileSystem, producto_s3: str) -> Optional[Path]:
        """Helper function to download a single S3 objective, to be run in a thread pool."""
        last_exception = None
        for attempt in range(self.S3_RETRY_ATTEMPTS):
            try:
                # Extraer año, día juliano y hora del patrón de búsqueda
                # El patrón es como OR_ABI-L1b-RadF-M6_G16_s202412312000
                timestamp_str = objetivo.patron_busqueda.split('_s')[1].split('.')[0]
                dt_obj = datetime.strptime(timestamp_str, "%Y%j%H%M")
                
                anio = dt_obj.strftime("%Y")
                dia_juliano = dt_obj.strftime("%j")
                hora = dt_obj.strftime("%H")

                s3_path_dir = f"s3://noaa-goes16/{producto_s3}/{anio}/{dia_juliano}/{hora}/"
                
                # Listar archivos en el directorio S3
                archivos_en_s3 = s3_client.ls(s3_path_dir)
                
                # Buscar el archivo que coincida con nuestro timestamp
                archivo_s3_a_descargar = None
                for s3_file in archivos_en_s3:
                    # Extraer la parte del timestamp 's' del nombre del archivo S3
                    # Ejemplo s3_file: OR_ABI-L2-ACTPF-M6_G16_s20202800930188_e...
                    s_part_start_idx = s3_file.find('_s')
                    if s_part_start_idx != -1:
                        # La parte del timestamp termina antes del siguiente guion bajo o la extensión del archivo
                        s_part_end_idx = s3_file.find('_e', s_part_start_idx)
                        if s_part_end_idx == -1: # Fallback si _e no se encuentra
                            s_part_end_idx = s3_file.find('.nc', s_part_start_idx) # Buscar .nc
                        
                        if s_part_end_idx != -1:
                            s3_timestamp_full = s3_file[s_part_start_idx + 2 : s_part_end_idx] # ej., "20202800930188"
                            # Verificar si nuestro timestamp generado (YYYYJJJHHMM) es un prefijo del timestamp de S3
                            if s3_timestamp_full.startswith(timestamp_str):
                                archivo_s3_a_descargar = s3_file
                                break
                
                if not archivo_s3_a_descargar:
                    self.logger.warning(f"❌ No se encontró el archivo en S3 para el objetivo: {objetivo.patron_busqueda}")
                    return None # No reintentar si el archivo no existe

                nombre_archivo_local = Path(archivo_s3_a_descargar).name
                ruta_local_destino = directorio_destino / nombre_archivo_local
                
                self.db.actualizar_estado(consulta_id, "procesando", None, f"Descargando de S3 (Intento {attempt + 1}/{self.S3_RETRY_ATTEMPTS}): {nombre_archivo_local}")
                self.logger.info(f"⬇️ Descargando desde S3: {archivo_s3_a_descargar} -> {ruta_local_destino} (Intento {attempt + 1}/{self.S3_RETRY_ATTEMPTS})")
                s3_client.get(archivo_s3_a_descargar, str(ruta_local_destino))
                return ruta_local_destino # Éxito, salir de la función

            except Exception as e:
                last_exception = e
                self.logger.warning(f"⚠️ Falló el intento {attempt + 1}/{self.S3_RETRY_ATTEMPTS} para descargar {objetivo.patron_busqueda}: {e}")
                if attempt < self.S3_RETRY_ATTEMPTS - 1:
                    wait_time = self.S3_RETRY_BACKOFF_SECONDS * (2 ** attempt) # Backoff exponencial
                    self.logger.info(f"   Reintentando en {wait_time} segundos...")
                    time.sleep(wait_time)

        # Si todos los intentos fallaron, lanzar la última excepción capturada
        self.logger.error(f"❌ Fallaron todos los {self.S3_RETRY_ATTEMPTS} intentos para descargar desde S3 el objetivo {objetivo.patron_busqueda}.")
        if last_exception:
            raise last_exception
        
        return None # Fallback en caso de que no haya habido excepción