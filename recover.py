import os
import logging
import shutil
import tarfile
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from pathlib import Path
from database import ConsultasDatabase

class RecoverFiles:
    """
    Atiende solicitudes de recuperación de archivos de datos desde un almacenamiento local.
    """
    def __init__(self, db: ConsultasDatabase, 
        source_data_path: str = "/depot/goes16", base_download_path: str = "/data/tmp"):
        self.db = db
        self.source_data_path = Path(source_data_path)
        self.base_download_path = Path(base_download_path)
        self.logger = logging.getLogger(__name__)

    def procesar_consulta(self, consulta_id: str, query_dict: Dict):
        """
        Método principal que orquesta la recuperación de archivos.
        """
        try:
            self.logger.info(f"🚀 Atendiendo solicitud {consulta_id}")

            # 1. Preparar directorio de destino para la consulta
            directorio_destino = self.base_download_path / consulta_id
            directorio_destino.mkdir(exist_ok=True, parents=True)
            self.db.actualizar_estado(consulta_id, "procesando", 10, "Preparando entorno")

            # 2. Generar la lista de archivos a recuperar
            self.db.actualizar_estado(consulta_id, "procesando", 20, "Generando lista de archivos")
            archivos_a_recuperar = self._generar_lista_de_archivos(query_dict)
            self.logger.info(f"📁 Se recuperarán {len(archivos_a_recuperar)} archivos.")

            # 3. Copiar los archivos y actualizar el progreso
            # 3. Extraer los archivos de los .tgz y actualizar el progreso
            archivos_recuperados = self._recuperar_archivos(consulta_id, archivos_a_recuperar, directorio_destino, query_dict)

            # 4. Generar reporte final y guardar en la base de datos
            self.db.actualizar_estado(consulta_id, "procesando", 90, "Finalizando")
            resultados_finales = self._generar_reporte_final(archivos_recuperados, directorio_destino)
            self.db.guardar_resultados(consulta_id, resultados_finales)

            self.logger.info(f"✅ Procesamiento completado para {consulta_id}")

        except Exception as e:
            self.logger.error(f"❌ Error procesando consulta {consulta_id}: {e}")
            self.db.actualizar_estado(consulta_id, "error", 0, f"Error: {str(e)}")

    def _generar_lista_de_archivos(self, query_dict: Dict) -> List[Path]:
        """
        Genera la lista de rutas de archivos fuente a recuperar.
        **ESTA ES LA LÓGICA PRINCIPAL QUE DEBES ADAPTAR A TU SISTEMA.**
        """
        lista_archivos = []
        
        # Extraer parámetros de la consulta
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

        for fecha_jjj, horarios_list in query_dict.get('fechas', {}).items():
            año = fecha_jjj[:4]
            dia_del_año_int = int(fecha_jjj[4:])
            semana = dia_del_año_int // 7 + 1

            directorio_semana = base_path / año / f"{semana:02d}"

            if not directorio_semana.exists():
                self.logger.warning(f"⚠️ Directorio de la semana no encontrado: {directorio_semana}")
                continue

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
                        patron_busqueda = f"OR_{sensor.upper()}-{nivel}-*-{sat_code}_{timestamp_archivo}.tgz"
                        
                        # Usamos glob para encontrar el archivo exacto, permitiendo variaciones menores (ej. RadF-M6)
                        archivos_encontrados = list(directorio_semana.glob(patron_busqueda))
                        lista_archivos.extend(archivos_encontrados)
                    
                    current_dt += timedelta(minutes=1)

        return lista_archivos

    def _recuperar_archivos(self, consulta_id: str, archivos_a_recuperar: List[Path], directorio_destino: Path, query_dict: Dict) -> List[Path]:
        """
        Copia los archivos de la lista fuente al directorio de destino.
        Abre los archivos .tgz de la lista fuente, extrae solo las bandas solicitadas
        al directorio de destino y actualiza el progreso en la base de datos.
        Actualiza el progreso en la base de datos.
        """
        archivos_recuperados = []
        total_archivos = len(archivos_a_recuperar)
        
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

        if total_archivos == 0:
            return []

        for i, archivo_fuente in enumerate(archivos_a_recuperar):
            progreso = 20 + int((i / total_archivos) * 70)  # Progreso de 20% a 90%
            self.db.actualizar_estado(consulta_id, "procesando", progreso, f"Procesando archivo {i+1}/{total_archivos}")

            try:
                if not archivo_fuente.exists():
                    self.logger.warning(f"⚠️ Archivo fuente no existe: {archivo_fuente}")
                    continue

                # Si se pidieron todos los datos (bandas o productos), solo copiamos el .tgz
                if copiar_tgz_completo:
                    self.logger.info(f"📦 Copiando archivo completo (solicitud general): {archivo_fuente.name}")
                    shutil.copy(archivo_fuente, directorio_destino)
                    archivos_recuperados.append(directorio_destino / archivo_fuente.name)
                    continue

                # Si se pidió un subconjunto, abrir el .tgz para extraer selectivamente
                with tarfile.open(archivo_fuente, "r:gz") as tar:
                    miembros_a_extraer = []
                    for miembro in tar.getmembers():
                        if miembro.isfile():
                            extraer = False
                            if nivel == 'L1b':
                                # Lógica de extracción para bandas L1b
                                if any(f"_C{banda}_" in miembro.name for banda in bandas_solicitadas):
                                    extraer = True
                            elif nivel == 'L2':
                                # Lógica de extracción para productos L2 (ej. ...-L2-CMIPF-...)
                                if any(f"-L2-{producto}" in miembro.name for producto in productos_solicitados):
                                    extraer = True
                            
                            if extraer:
                                miembros_a_extraer.append(miembro)
                    
                    if miembros_a_extraer:
                        self.logger.info(f"🔎 Extrayendo {len(miembros_a_extraer)} archivos de {archivo_fuente.name}")
                        tar.extractall(path=directorio_destino, members=miembros_a_extraer)
                        for miembro in miembros_a_extraer:
                            archivos_recuperados.append(directorio_destino / miembro.name)

            except Exception as e:
                self.logger.error(f"❌ No se pudo procesar {archivo_fuente}: {e}")

        return archivos_recuperados

    def _generar_reporte_final(self, archivos_recuperados: List[Path], directorio_destino: Path) -> Dict:
        """Genera el diccionario de resultados finales."""
        total_bytes = sum(f.stat().st_size for f in archivos_recuperados if f.is_file())
        tamaño_mb = round(total_bytes / (1024 * 1024), 2)

        return {
            "archivos_recuperados": [f.name for f in archivos_recuperados],
            "total_archivos": len(archivos_recuperados),
            "tamaño_total_mb": tamaño_mb,
            "directorio_destino": str(directorio_destino),
            "timestamp_procesamiento": datetime.now().isoformat()
        }