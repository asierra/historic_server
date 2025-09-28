import os
import logging
import shutil
from typing import List, Dict, Optional
from datetime import datetime
from pathlib import Path
from database import ConsultasDatabase

class RecoverFiles:
    """
    Atiende solicitudes de recuperación de archivos de datos desde un almacenamiento local.
    """
    def __init__(self, db: ConsultasDatabase, source_data_path: str = "/data/goes", base_download_path: str = "/data/tmp"):
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
            archivos_recuperados = self._recuperar_archivos(consulta_id, archivos_a_recuperar, directorio_destino)

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
        fechas = query_dict.get('fechas', {})
        sensor = query_dict.get('sensor', 'abi') # Default a 'abi' si no viene
        bandas = query_dict.get('bandas', [])

        for fecha_str in fechas.keys():
            año = fecha_str[:4]
            dia_del_año = datetime.strptime(fecha_str, "%Y%m%d").strftime('%j')

            # Ejemplo: /data/goes/GOES16/L1b/2024/001/
            directorio_fuente = self.source_data_path / query_dict['satelite'] / query_dict['nivel'] / año / dia_del_año

            if not directorio_fuente.exists():
                self.logger.warning(f"⚠️ Directorio fuente no encontrado: {directorio_fuente}")
                continue

            # Lógica de búsqueda de archivos para el sensor ABI.
            # Por ahora, ignoramos el valor de 'sensor' y asumimos que es 'abi'.
            # Ejemplo de nombre de archivo: OR_ABI-L1b-RadF-M6C02_G16_...
            for banda in bandas:
                # El patrón busca archivos que contengan la banda específica (ej. _C02_).
                # Los asteriscos aseguran que coincida sin importar el resto del nombre.
                patron_busqueda = f"*_C{banda}_*.nc"
                lista_archivos.extend(directorio_fuente.glob(patron_busqueda))

        return lista_archivos

    def _recuperar_archivos(self, consulta_id: str, archivos_a_recuperar: List[Path], directorio_destino: Path) -> List[Path]:
        """
        Copia los archivos de la lista fuente al directorio de destino.
        Actualiza el progreso en la base de datos.
        """
        archivos_recuperados = []
        total_archivos = len(archivos_a_recuperar)
        if total_archivos == 0:
            return []

        for i, archivo_fuente in enumerate(archivos_a_recuperar):
            progreso = 20 + int((i / total_archivos) * 70)  # Progreso de 20% a 90%
            self.db.actualizar_estado(consulta_id, "procesando", progreso, f"Recuperando {i+1}/{total_archivos}")

            try:
                if archivo_fuente.exists():
                    shutil.copy(archivo_fuente, directorio_destino)
                    archivos_recuperados.append(directorio_destino / archivo_fuente.name)
                else:
                    self.logger.warning(f"⚠️ Archivo fuente no existe: {archivo_fuente}")
            except Exception as e:
                self.logger.error(f"❌ No se pudo copiar {archivo_fuente}: {e}")

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