import s3fs
import time
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime, timezone
from pebble import ProcessPool, ThreadPool


class S3RecoverFiles:
    def __init__(self, logger, max_workers, retry_attempts, retry_backoff):
        self.logger = logger
        self.max_workers = max_workers
        self.retry_attempts = retry_attempts
        self.retry_backoff = retry_backoff

    def get_sat_code_for_date(self, satellite_name: str, request_date: datetime, goes19_operational_date: datetime) -> str:
        if request_date.tzinfo is None:
            request_date = request_date.replace(tzinfo=timezone.utc)
        if satellite_name == "GOES-EAST":
            return "G19" if request_date >= goes19_operational_date else "G16"
        if satellite_name == "GOES-WEST":
            return "G18"
        if '-' in satellite_name:
            return f"G{satellite_name.split('-')[-1]}"
        return satellite_name

    def get_s3_product_names(self, query_dict: Dict) -> List[str]:
        sensor = query_dict.get('sensor', 'abi').upper()
        nivel = query_dict.get('nivel', 'L1b')
        domain_map = {'fd': 'F', 'conus': 'C', 'm1': 'M1', 'm2': 'M2'}
        api_domain = query_dict.get('dominio', 'fd')
        s3_domain_code = domain_map.get(api_domain.lower(), api_domain.upper())
        if nivel == 'L1b':
            return [f"{sensor}-{nivel}-Rad{s3_domain_code}"]
        if nivel == 'L2' and query_dict.get('productos'):
            # Para cada producto, agrega el sufijo de dominio
            return [f"{sensor}-{nivel}-{prod}{s3_domain_code}" for prod in query_dict['productos']]
        return [f"{sensor}-{nivel}-RadF"]

    def discover_files(self, query_dict: Dict, goes19_operational_date: datetime) -> Dict[str, str]:
        sat_name = query_dict.get('satelite', 'GOES-16')
        first_day_jjj = next(iter(query_dict.get('fechas', {})), None)
        request_date = datetime.strptime(first_day_jjj, '%Y%j') if first_day_jjj else datetime.now()
        sat_code = self.get_sat_code_for_date(sat_name, request_date, goes19_operational_date)
        s3_bucket = f"noaa-goes{sat_code.replace('G', '')}"
        s3_product_names = self.get_s3_product_names(query_dict)
        s3 = s3fs.S3FileSystem(anon=True, config_kwargs={'connect_timeout': 10, 'read_timeout': 30})  # <-- AGREGA ESTA LÍNEA
        objetivos_s3_a_descargar = set()
        bandas_solicitadas = query_dict.get('bandas')
        # ...resto del método...
        for fecha_jjj, horarios_list in query_dict.get('fechas', {}).items():
            # Permitir tanto YYYYMMDD como YYYYJJJ
            if len(fecha_jjj) == 8:  # YYYYMMDD
                dt = datetime.strptime(fecha_jjj, "%Y%m%d")
                anio = dt.strftime("%Y")
                dia_juliano = dt.strftime("%j")  # Siempre 3 dígitos
            elif len(fecha_jjj) == 7:  # YYYYJJJ
                anio = fecha_jjj[:4]
                dia_juliano = fecha_jjj[4:].zfill(3)
            else:
                raise ValueError(f"Formato de fecha no soportado: {fecha_jjj}")
            
            for horario_str in horarios_list:
                inicio_hh = int(horario_str.split(':')[0])
                fin_hh = int(horario_str.split('-')[1].split(':')[0]) if '-' in horario_str else inicio_hh
                for hora in range(inicio_hh, fin_hh + 1):
                    for s3_product_name in s3_product_names:
                        s3_path_hora = f"{s3_bucket}/{s3_product_name}/{anio}/{dia_juliano}/{hora:02d}/"
                        try:
                            archivos_en_hora = s3.ls(s3_path_hora)
                            archivos_nc = [f for f in archivos_en_hora if f.endswith('.nc')]
                            bandas_solicitadas = query_dict.get('bandas')
                            if bandas_solicitadas:
                                bandas_solicitadas_str = [str(b) for b in bandas_solicitadas]
                                archivos_nc = [
                                    f for f in archivos_nc
                                    if any(f"C{b}" in f for b in bandas_solicitadas_str)
                                ]

                            archivos_filtrados = self.filter_files_by_time(archivos_nc, f"{anio}{dia_juliano}", horarios_list)

                            objetivos_s3_a_descargar.update(archivos_filtrados)
                        except FileNotFoundError:
                            continue

        return {Path(f).name: f for f in objetivos_s3_a_descargar}


    def download_files(self, consulta_id: str, archivos_s3: List[str], directorio_destino: Path, db) -> (List[Path], List[str]):
        s3 = s3fs.S3FileSystem(anon=True, config_kwargs={'connect_timeout': 10, 'read_timeout': 30})
        archivos_s3_recuperados = []
        objetivos_aun_fallidos = []
        total_obj = len(set(archivos_s3)) or 1
        completados = 0
        update_every = 100  # Fijo: actualizar progreso cada 100 archivos
        ok_count = 0
        fail_count = 0
        with ThreadPool(max_workers=self.max_workers) as pool:
            future_to_s3_path = {
                pool.schedule(self._download_single_s3_objective, args=(consulta_id, s3_path, directorio_destino, s3, db)): s3_path
                for s3_path in set(archivos_s3)
            }
            for future in future_to_s3_path:
                s3_path = future_to_s3_path[future]
                try:
                    resultado = future.result()
                    if resultado:
                        archivos_s3_recuperados.append(resultado)
                        ok_count += 1
                except Exception as e:
                    objetivos_aun_fallidos.append(Path(s3_path).name)
                    fail_count += 1
                finally:
                    completados += 1
                    if db and (completados % update_every == 0 or completados == total_obj):
                        # Mapear progreso de 85 a 95 proporcional a descargas S3
                        progreso = 85 + int((completados / total_obj) * 10)
                        db.actualizar_estado(
                            consulta_id,
                            "procesando",
                            progreso,
                            f"S3 progreso: {completados}/{total_obj}"
                        )
                        # Log resumen cada corte
                        self.logger.info(
                            f"S3 progreso: {completados}/{total_obj} (ok: {ok_count}, fail: {fail_count})"
                        )
        # Log resumen final
        self.logger.info(
            f"S3 finalizado: {ok_count} ok, {fail_count} fallos de {total_obj} objetivos"
        )
        return archivos_s3_recuperados, objetivos_aun_fallidos

    def _download_single_s3_objective(self, consulta_id: str, archivo_remoto_s3: str, directorio_destino: Path, s3_client: s3fs.S3FileSystem, db) -> Optional[Path]:
        last_exception = None
        for attempt in range(self.retry_attempts):
            try:
                nombre_archivo_local = Path(archivo_remoto_s3).name
                ruta_local_destino = directorio_destino / nombre_archivo_local
                # Idempotencia: si el archivo ya existe, omitir descarga
                try:
                    if ruta_local_destino.exists() and ruta_local_destino.stat().st_size > 0:
                        return ruta_local_destino
                except OSError:
                    pass
                # Reducir ruido: no actualizar DB por cada intento/archivo; el progreso se reporta en bloque.
                s3_client.get(archivo_remoto_s3, str(ruta_local_destino))
                return ruta_local_destino
            except Exception as e:
                last_exception = e
                # Reducir ruido: registrar intentos a nivel debug
                self.logger.debug(f"Intento {attempt + 1}/{self.retry_attempts} falló para {archivo_remoto_s3}: {e}")
                if attempt < self.retry_attempts - 1:
                    wait_time = self.retry_backoff * (2 ** attempt)
                    time.sleep(wait_time)
        self.logger.error(f"❌ Fallaron todos los {self.retry_attempts} intentos para descargar desde S3 el archivo {archivo_remoto_s3}.")
        if last_exception:
            raise last_exception
        return None

    def filter_files_by_time(self, archivos_nc: list, fecha_jjj: str, horarios_list: list) -> list:
        archivos_filtrados = []
        for archivo in archivos_nc:
            nombre = archivo.name if hasattr(archivo, "name") else archivo
            s_idx = nombre.find('_s')
            e_idx = nombre.find('_e')
            if s_idx == -1 or e_idx == -1:
                continue
            ts_str = nombre[s_idx+2:e_idx]
            if len(ts_str) < 11:
                continue
            anio = ts_str[:4]
            dia_juliano = ts_str[4:7]
            hora = ts_str[7:9]
            minuto = ts_str[9:11]
            if anio + dia_juliano != fecha_jjj:
                continue
            archivo_hm = int(hora) * 60 + int(minuto)
            for horario_str in horarios_list:
                partes = horario_str.split('-')
                inicio = partes[0]
                fin = partes[1] if len(partes) > 1 else inicio
                inicio_hm = int(inicio[:2]) * 60 + int(inicio[3:5])
                fin_hm = int(fin[:2]) * 60 + int(fin[3:5])
                if inicio_hm <= archivo_hm <= fin_hm:
                    archivos_filtrados.append(archivo)
                    break
        return archivos_filtrados
