import os
import logging
import shutil
import tarfile
from typing import List, Dict, Optional, NamedTuple
from datetime import datetime, timedelta
from pathlib import Path
import s3fs

from database import ConsultasDatabase

from collections import defaultdict
 
class RecoverFiles:
    """
    Atiende solicitudes de recuperación de archivos de datos desde un almacenamiento local.
    """
    def __init__(self, db: ConsultasDatabase, 
        source_data_path: str = "/depot/goes16", base_download_path: str = "/data/tmp",
        s3_fallback_enabled: bool = True):
        self.db = db
        self.source_data_path = Path(source_data_path)
        self.base_download_path = Path(base_download_path)
        self.logger = logging.getLogger(__name__)
        self.s3_fallback_enabled = s3_fallback_enabled

    def procesar_consulta(self, consulta_id: str, query_dict: Dict):
        try:
            self.logger.info(f"🚀 Atendiendo solicitud {consulta_id}")

            # 1. Preparar entorno
            directorio_destino = self.base_download_path / consulta_id
            directorio_destino.mkdir(exist_ok=True, parents=True)
            self.db.actualizar_estado(consulta_id, "procesando", 10, "Preparando entorno")

            # 2. Determinar todos los archivos potenciales (objetivos)
            objetivos = self._generar_objetivos_de_busqueda(query_dict)
            total_objetivos = len(objetivos)
            self.logger.info(f"🔎 Se buscarán {total_objetivos} archivos potenciales.")
            self.db.actualizar_estado(consulta_id, "procesando", 20, f"Identificados {total_objetivos} archivos potenciales a buscar.")

            # 3. Procesar cada objetivo y registrar éxitos y fracasos
            archivos_recuperados = []
            objetivos_fallidos = []

            for i, objetivo in enumerate(objetivos):
                # El progreso para la recuperación local va del 20% al 80%
                progreso = 20 + int(((i + 1) / total_objetivos) * 60)
                self.db.actualizar_estado(consulta_id, "procesando", progreso, f"Buscando archivo {i+1}/{total_objetivos}")

                archivo_encontrado = self._buscar_archivo_para_objetivo(objetivo)

                if archivo_encontrado:
                    try:
                        # La función _recuperar_archivo ahora maneja un solo archivo
                        nuevos_archivos = self._recuperar_archivo(archivo_encontrado, directorio_destino, query_dict)
                        archivos_recuperados.extend(nuevos_archivos)
                    except Exception as e:
                        self.logger.error(f"❌ No se pudo procesar {archivo_encontrado}: {e}")
                        objetivos_fallidos.append(objetivo)
                else:
                    self.logger.warning(f"⚠️ No se encontró archivo local para el objetivo: {objetivo.patron_busqueda}")
                    objetivos_fallidos.append(objetivo)

            # 3.5 (Opcional) Intentar recuperar los fallidos desde S3
            if self.s3_fallback_enabled and objetivos_fallidos:
                self.db.actualizar_estado(consulta_id, "procesando", 85, f"Intentando recuperar {len(objetivos_fallidos)} archivos faltantes desde S3.")
                s3_recuperados, objetivos_fallidos_final = self._recuperar_fallidos_desde_s3(
                    objetivos_fallidos, directorio_destino, query_dict
                )
                archivos_recuperados.extend(s3_recuperados)
                objetivos_fallidos = objetivos_fallidos_final # Actualizar la lista de fallidos


            # 4. Generar reporte final
            self.db.actualizar_estado(consulta_id, "procesando", 95, "Generando reporte final")
            resultados_finales = self._generar_reporte_final(
                archivos_recuperados, directorio_destino, objetivos_fallidos, query_dict
            )
            self.db.guardar_resultados(consulta_id, resultados_finales)

            self.logger.info(f"✅ Procesamiento completado para {consulta_id}")

        except Exception as e:
            self.logger.error(f"❌ Error procesando consulta {consulta_id}: {e}")
            self.db.actualizar_estado(consulta_id, "error", 0, f"Error: {str(e)}")
    
    class ObjetivoBusqueda(NamedTuple):
        """Estructura para un archivo potencial que se debe encontrar."""
        directorio_semana: Path
        patron_busqueda: str
        fecha_original: str  # YYYYMMDD o YYYYMMDD-YYYYMMDD
        horario_original: str # HH:MM o HH:MM-HH:MM

    def _generar_objetivos_de_busqueda(self, query_dict: Dict) -> List[ObjetivoBusqueda]:
        """
        Genera una lista de todos los archivos que *deberían* existir según la consulta.
        """
        objetivos = []
        satelite = query_dict.get('satelite', 'GOES-16')
        sensor = query_dict.get('sensor', 'abi')
        nivel = query_dict.get('nivel', 'unknown')
        dominio = query_dict.get('dominio', 'fd')
        sat_code = f"G{satelite.split('-')[-1]}" if '-' in satelite else satelite

        # Construir la ruta base de la consulta, incluyendo sensor, nivel y dominio.
        base_path = self.source_data_path
        for key in ['sensor', 'nivel', 'dominio']:
            if query_dict.get(key):
                base_path /= query_dict[key]

        # Usamos las fechas originales del request para reconstruir la consulta de recuperación
        fechas_originales = query_dict.get('_original_request', {}).get('fechas', {})

        for fecha_jjj, horarios_list in query_dict.get('fechas', {}).items():
            año = fecha_jjj[:4]
            dia_del_año_int = int(fecha_jjj[4:])
            semana = dia_del_año_int // 7 + 1

            directorio_semana = base_path / año / f"{semana:02d}"

            fecha_dt = datetime.strptime(fecha_jjj, "%Y%j")

            for horario_str in horarios_list:
                partes = horario_str.split('-') # Se re-añade este bloque
                inicio_str, fin_str = partes[0], partes[1] if len(partes) > 1 else partes[0] # Se re-añade este bloque
                inicio_dt = fecha_dt.replace(hour=int(inicio_str[:2]), minute=int(inicio_str[3:])) # Se re-añade este bloque
                fin_dt = fecha_dt.replace(hour=int(fin_str[:2]), minute=int(fin_str[3:])) # Se re-añade este bloque
                current_dt = inicio_dt # Se re-añade este bloque
                while current_dt <= fin_dt:
                    if (dominio == 'conus' and current_dt.minute % 5 == 1) or \
                       (dominio == 'fd' and current_dt.minute % 10 == 0):
                        
                        timestamp_archivo = f"s{current_dt.strftime('%Y%j%H%M')}00"
                        patron_busqueda = f"OR_{sensor.upper()}-{nivel}-*-{sat_code}_{timestamp_archivo}.tgz"

                        # Encontrar la fecha y horario original que generó este timestamp
                        fecha_ymd_str = current_dt.strftime("%Y%m%d")
                        horario_original_encontrado = self._find_original_request_slot(fecha_ymd_str, horario_str, fechas_originales)

                        objetivos.append(self.ObjetivoBusqueda(
                            directorio_semana=directorio_semana,
                            patron_busqueda=patron_busqueda,
                            fecha_original=horario_original_encontrado[0],
                            horario_original=horario_original_encontrado[1]
                        ))
                    current_dt += timedelta(minutes=1)
        return objetivos
    def _buscar_archivo_para_objetivo(self, objetivo: ObjetivoBusqueda) -> Optional[Path]:
        """Busca en disco un archivo que coincida con el patrón del objetivo."""
        if not objetivo.directorio_semana.exists():
            return None
        
        # Usamos glob para encontrar el archivo exacto, permitiendo variaciones menores (ej. RadF-M6)
        archivos_encontrados = list(objetivo.directorio_semana.glob(objetivo.patron_busqueda))
        if archivos_encontrados:
            return archivos_encontrados[0] # Devolver solo el primero, asumiendo que es único
        return None

    def _recuperar_archivo(self, archivo_fuente: Path, directorio_destino: Path, query_dict: Dict) -> List[Path]:
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
            self.logger.debug(f"📦 Copiando archivo completo: {archivo_fuente.name}")
            shutil.copy(archivo_fuente, directorio_destino)
            archivos_recuperados.append(directorio_destino / archivo_fuente.name)
            return archivos_recuperados
        
        # Si se pidió un subconjunto, abrir el .tgz para extraer selectivamente
        try:
            with tarfile.open(archivo_fuente, "r:gz") as tar:
                miembros_a_extraer = []
                for miembro in tar.getmembers():
                    if not miembro.isfile():
                        continue

                    extraer = False
                    if nivel == 'L1b':
                        if any(f"_C{banda}_" in miembro.name for banda in bandas_solicitadas):
                            extraer = True
                    elif nivel == 'L2':
                        if any(f"-L2-{producto}" in miembro.name for producto in productos_solicitados):
                            extraer = True
                    
                    if extraer:
                        miembros_a_extraer.append(miembro)
                
                if miembros_a_extraer:
                    self.logger.debug(f"🔎 Extrayendo {len(miembros_a_extraer)} archivos de {archivo_fuente.name}")
                    tar.extractall(path=directorio_destino, members=miembros_a_extraer)
                    for miembro in miembros_a_extraer:
                        archivos_recuperados.append(directorio_destino / miembro.name)
        except (tarfile.TarError, FileNotFoundError) as e:
            self.logger.error(f"❌ No se pudo procesar el archivo tar {archivo_fuente}: {e}")

        return archivos_recuperados

    def _generar_reporte_final(self, archivos_recuperados: List[Path], directorio_destino: Path, objetivos_fallidos: List[ObjetivoBusqueda], query_original: Dict) -> Dict:
        """Genera el diccionario de resultados finales."""
        total_bytes = sum(f.stat().st_size for f in archivos_recuperados if f.is_file())
        tamaño_mb = round(total_bytes / (1024 * 1024), 2)

        # Construir la consulta de recuperación con los objetivos que fallaron
        fechas_fallidas = defaultdict(list)
        for obj in objetivos_fallidos:
            # Evitar duplicados
            if obj.horario_original not in fechas_fallidas[obj.fecha_original]:
                fechas_fallidas[obj.fecha_original].append(obj.horario_original)

        consulta_recuperacion = None
        if fechas_fallidas:
            # Copiamos la solicitud original y reemplazamos las fechas
            consulta_recuperacion = query_original.get('_original_request', {}).copy()
            consulta_recuperacion['fechas'] = dict(fechas_fallidas)
            # Opcional: añadir una nota sobre el origen de esta consulta
            consulta_recuperacion['descripcion'] = f"Consulta de recuperación para la solicitud original {query_original.get('id')}"

        return {
            "archivos_recuperados": [f.name for f in archivos_recuperados],
            "total_archivos": len(archivos_recuperados),
            "tamaño_total_mb": tamaño_mb,
            "directorio_destino": str(directorio_destino),
            "timestamp_procesamiento": datetime.now().isoformat(),
            "consulta_recuperacion": consulta_recuperacion
        }

    def _find_original_request_slot(self, fecha_ymd: str, horario_ymd: str, fechas_originales: Dict) -> (str, str):
        """
        Encuentra la clave de fecha y valor de horario originales que corresponden
        a una fecha/hora expandida. Es un helper para reconstruir la consulta de fallos.
        """
        # La lógica se simplificó en un paso anterior, pero la mantenemos por si acaso.
        # Ahora `fechas_originales` tiene claves YYYYMMDD.
        horarios_list = fechas_originales.get(fecha_ymd, [])
        for horario_key in horarios_list:
            if horario_key == horario_ymd:
                return fecha_ymd, horario_key
        
        # Fallback por si algo no coincide (no debería pasar)
        return fecha_ymd, horario_ymd

    def _recuperar_fallidos_desde_s3(self, objetivos_fallidos: List[ObjetivoBusqueda], directorio_destino: Path, query_dict: Dict) -> (List[Path], List[ObjetivoBusqueda]):
        """
        Intenta descargar desde S3 los archivos que no se encontraron localmente.
        """
        s3 = s3fs.S3FileSystem(anon=True)
        archivos_s3_recuperados = []
        objetivos_aun_fallidos = []

        # Construir el nombre del producto para la ruta S3
        sensor = query_dict.get('sensor', 'abi').upper()
        nivel = query_dict.get('nivel', 'L1b')
        producto_principal = query_dict.get('productos', [None])[0]
        
        if nivel == 'L1b':
            producto_s3 = f"{sensor}-{nivel}-RadF"
        elif nivel == 'L2' and producto_principal:
            producto_s3 = f"{sensor}-{nivel}-{producto_principal}F" # Asume dominio 'F' por defecto para S3
        else:
            self.logger.error("No se puede determinar el producto S3 para la consulta.")
            return [], objetivos_fallidos

        for objetivo in objetivos_fallidos:
            try:
                # Extraer año, día juliano y hora del patrón de búsqueda
                timestamp_str = objetivo.patron_busqueda.split('_s')[1].split('.')[0]
                dt_obj = datetime.strptime(timestamp_str, "%Y%j%H%M%S")
                
                anio = dt_obj.strftime("%Y")
                dia_juliano = dt_obj.strftime("%j")
                hora = dt_obj.strftime("%H")

                s3_path_dir = f"s3://noaa-goes16/{producto_s3}/{anio}/{dia_juliano}/{hora}/"
                
                # Listar archivos en el directorio S3
                archivos_en_s3 = s3.ls(s3_path_dir)
                
                # Buscar el archivo que coincida con nuestro timestamp
                archivo_s3_a_descargar = None
                for s3_file in archivos_en_s3:
                    if f"_s{timestamp_str}" in s3_file:
                        archivo_s3_a_descargar = s3_file
                        break
                
                if archivo_s3_a_descargar:
                    nombre_archivo_local = Path(archivo_s3_a_descargar).name
                    ruta_local_destino = directorio_destino / nombre_archivo_local
                    
                    self.logger.info(f"⬇️ Descargando desde S3: {archivo_s3_a_descargar} -> {ruta_local_destino}")
                    s3.get(archivo_s3_a_descargar, str(ruta_local_destino))
                    archivos_s3_recuperados.append(ruta_local_destino)
                else:
                    self.logger.warning(f"❌ No se encontró el archivo en S3 para el objetivo: {objetivo.patron_busqueda}")
                    objetivos_aun_fallidos.append(objetivo)

            except Exception as e:
                self.logger.error(f"❌ Error durante la recuperación desde S3 para el objetivo {objetivo.patron_busqueda}: {e}")
                objetivos_aun_fallidos.append(objetivo)

        return archivos_s3_recuperados, objetivos_aun_fallidos