from typing import List, Dict, Any
from config_base import SatelliteConfigBase
from datetime import datetime, timedelta, time

class SatelliteConfigGOES(SatelliteConfigBase):
    """Configuration specific to GOES satellites."""
    
    # --- GOES Data Periodicity Configuration ---
    # Basado en goes_typical.csv - periodicidad en minutos para cada producto/banda por dominio
    GOES_PERIODICITY = {
        # L1b - Bandas (periodicidad en minutos)
        "L1b": {
            "fd": {
                "01": 10, "02": 10, "03": 10, "04": 10, "05": 10, "06": 10,
                "07": 10, "08": 10, "09": 10, "10": 10, "11": 10, "12": 10,
                "13": 10, "14": 10, "15": 10, "16": 10
            },
            "conus": {
                "01": 5, "02": 5, "03": 5, "04": 5, "05": 5, "06": 5,
                "07": 5, "08": 5, "09": 5, "10": 5, "11": 5, "12": 5,
                "13": 5, "14": 5, "15": 5, "16": 5
            }
        },
        # L2 - Productos (periodicidad en minutos)
        "L2": {
            "fd": {
                "ACHA": 10, "ACM": 10, "ACTP": 10, "AOD": 10, "CMIP": 10, "DMW": 60,
                "LST": 10, "Rainfall": 10, "SST": 60, "TPW": 10,
                # Agregamos mapeos adicionales por compatibilidad
                "CODD": 10, "CODN": 10, "CPSD": 10, "CPSN": 10, "CTP": 10, 
                "ACHT": 10, "DMWV": 60, "AVIATION_FOG": 10, "ADP": 10
            },
            "conus": {
                "ACHA": 5, "ACM": 5, "ACTP": 5, "AOD": 5, "CMIP": 5, "DMW": 15,
                "LST": 5, "Rainfall": 5, "SST": 60, "TPW": 5,
                # Agregamos mapeos adicionales por compatibilidad  
                "CODD": 5, "CODN": 5, "CPSD": 5, "CPSN": 5, "CTP": 5,
                "ACHT": 5, "DMWV": 15, "AVIATION_FOG": 5, "ADP": 5
            }
        }
    }

    # --- GOES Data Size Configuration ---
    # Basado en goes_typical.csv - tamaño en MB para cada producto/banda por dominio
    GOES_WEIGHTS = {
        # L1b - Bandas (tamaño en MB)
        "L1b": {
            "fd": {
                "01": 21, "02": 65, "03": 21, "04": 14, "05": 21, "06": 14,
                "07": 14, "08": 14, "09": 14, "10": 14, "11": 14, "12": 14,
                "13": 14, "14": 14, "15": 14, "16": 14
            },
            "conus": {
                "01": 7.29, "02": 53.94, "03": 9.25, "04": 2.02, "05": 9.49, "06": 2.09,
                "07": 4.05, "08": 3.22, "09": 3.06, "10": 3.72, "11": 4.62, "12": 3.87,
                "13": 4.68, "14": 4.68, "15": 4.62, "16": 3.33
            }
        },
        # L2 - Productos (tamaño en MB)
        "L2": {
            "fd": {
                "ACHA": 35, "ACM": 35, "ACTP": 35, "AOD": 10, "CMIP": 600, "DMW": 1,
                "LST": 10, "Rainfall": 15, "SST": 25, "TPW": 15,
                # Mapeos adicionales basados en productos similares
                "CODD": 35, "CODN": 35, "CPSD": 35, "CPSN": 35, "CTP": 35,
                "ACHT": 35, "DMWV": 1, "AVIATION_FOG": 15, "ADP": 35
            },
            "conus": {
                "ACHA": 20, "ACM": 20, "ACTP": 20, "AOD": 6.5, "CMIP": 180, "DMW": 1,
                "LST": 5, "Rainfall": 5, "SST": 15, "TPW": 5,
                # Mapeos adicionales basados en productos similares  
                "CODD": 20, "CODN": 20, "CPSD": 20, "CPSN": 20, "CTP": 20,
                "ACHT": 20, "DMWV": 1, "AVIATION_FOG": 5, "ADP": 20
            }
        }
    }
    
    # --- Implementation of Abstract Properties ---

    @property
    def VALID_SATELLITES(self) -> List[str]:
        return ["GOES-EAST", "GOES-WEST", "GOES-16", "GOES-18", "GOES-19"]

    @property
    def DEFAULT_SATELLITE(self) -> str:
        return "GOES-EAST"

    @property
    def VALID_SENSORS(self) -> List[str]:
        return ["abi", "suvi", "glm"]

    @property
    def DEFAULT_SENSOR(self) -> str:
        return "abi"

    @property
    def VALID_LEVELS(self) -> List[str]:
        return ["L1b", "L2"]

    @property
    def DEFAULT_LEVEL(self) -> str:
        return "L1b"

    @property
    def VALID_DOMAINS(self) -> List[str]:
        return ["fd", "conus"]

    @property
    def VALID_PRODUCTS(self) -> List[str]:
        return [
            "ADP", "AOD", "ACM", "CMIP", "CODD", "CODN", "CPSD", "CPSN",
            "ACHA", "ACTP", "CTP", "ACHT", "Rainfall", "SST", "TPW", 
            "DMW", "DMWV", "LST", "AVIATION_FOG", "VAA"
        ]

    @property
    def VALID_BANDAS(self) -> List[str]:
        return [f"{i:02d}" for i in range(1, 17)]

    @property
    def DEFAULT_BANDAS(self) -> List[str]:
        return ["ALL"]

    # --- Helper Methods ---
    
    def is_valid_satellite(self, satellite: str) -> bool:
        return satellite in self.VALID_SATELLITES

    def is_valid_sensor(self, sensor: str) -> bool:
        return sensor in self.VALID_SENSORS

    def is_valid_level(self, level: str) -> bool:
        return level in self.VALID_LEVELS

    def is_valid_domain(self, domain: str) -> bool:
        return domain in self.VALID_DOMAINS

    def validate_bandas(self, bandas: List[str]) -> List[str]:
        """Valida bandas. Acepta lista vacía cuando no se requieren bandas."""
        bandas = bandas or []
        if not bandas:
            # No exigir en este nivel; la lógica de 'requeridas' vive en main.py
            return bandas

        if "ALL" in bandas:
            return ["ALL"]  # expand_bandas las convertirá luego

        bandas_invalidas = [b for b in bandas if b not in self.VALID_BANDAS]
        if bandas_invalidas:
            raise ValueError(f"Bandas inválidas: {bandas_invalidas}. Bandas válidas: {self.VALID_BANDAS}")

        return bandas

    def expand_bandas(self, bandas: List[str]) -> List[str]:
        """Expande 'ALL' o tolera lista vacía/None sin error."""
        bandas = bandas or []
        if "ALL" in bandas:
            return self.VALID_BANDAS
        return bandas

    def get_periodicity(self, nivel: str, dominio: str, item: str) -> int:
        """
        Obtiene la periodicidad en minutos para un producto/banda específico.
        
        Args:
            nivel: "L1b" o "L2"
            dominio: "fd" o "conus"  
            item: nombre del producto (L2) o banda (L1b)
        
        Returns:
            Periodicidad en minutos, o periodicidad por defecto si no se encuentra
        """
        try:
            return self.GOES_PERIODICITY[nivel][dominio][item]
        except KeyError:
            # Si no encontramos el item específico, usar periodicidad por defecto
            if dominio == 'fd':
                return 10  # Default para Full Disk
            else:  # conus
                return 5   # Default para CONUS

    def get_file_weight(self, nivel: str, dominio: str, item: str) -> float:
        """
        Obtiene el tamaño en MB para un archivo de producto/banda específico.
        
        Args:
            nivel: "L1b" o "L2"
            dominio: "fd" o "conus"
            item: nombre del producto (L2) o banda (L1b)
        
        Returns:
            Tamaño en MB, o tamaño por defecto si no se encuentra
        """
        base_weight = None
        
        try:
            base_weight = self.GOES_WEIGHTS[nivel][dominio][item]
        except KeyError:
            # Si no encontramos el item específico, usar tamaño por defecto
            if nivel == "L1b":
                base_weight = 14.0 if dominio == 'fd' else 2.5  # Tamaño promedio para bandas
            else:  # L2
                base_weight = 20.0 if dominio == 'fd' else 10.0  # Tamaño promedio para productos
        
        return base_weight

    def estimate_file_count(self, request_data: Dict[str, Any]) -> int:
        """
        Estima la cantidad de archivos que se recuperarán basándose en la periodicidad específica
        de cada producto/banda según goes_typical.csv.
        """
        nivel = request_data.get("nivel")
        if nivel not in ["L1b", "L2"]:
            return 0

        dominio = request_data.get("dominio")
        fechas_dict = request_data.get("fechas", {})
        total_files = 0

        # Obtener items a procesar (productos para L2, bandas para L1b)
        items_to_process = []
        if nivel == "L1b":
            bandas = request_data.get("bandas", [])
            if "ALL" in bandas:
                items_to_process = self.VALID_BANDAS
            else:
                items_to_process = bandas if bandas else ["C02"]  # Default a C02 si no se especifica
        elif nivel == "L2":
            productos = request_data.get("productos", [])
            if not productos:
                return 0
            
            # Para CMIP, generar items por cada banda
            if "CMIP" in productos:
                bandas = request_data.get("bandas", [])
                if "ALL" in bandas or not bandas:
                    # CMIP genera un archivo por cada banda
                    items_to_process.extend([f"CMIP_C{i:02d}" for i in range(1, 17)])
                else:
                    items_to_process.extend([f"CMIP_{banda}" for banda in bandas])
                
                # Remover CMIP de la lista de productos para evitar doble conteo
                productos = [p for p in productos if p != "CMIP"]
            
            # Agregar otros productos (no CMIP)
            items_to_process.extend(productos)

        # Procesar cada fecha y horario
        for fecha_key, horarios_list in fechas_dict.items():
            # Expandir rangos de fechas
            try:
                if '-' in fecha_key:
                    start_date_str, end_date_str = fecha_key.split('-')
                    start_date = datetime.strptime(start_date_str, "%Y%m%d")
                    end_date = datetime.strptime(end_date_str, "%Y%m%d")
                    
                    current_date = start_date
                    date_range = []
                    while current_date <= end_date:
                        date_range.append(current_date)
                        current_date += timedelta(days=1)
                else:
                    date_range = [datetime.strptime(fecha_key, "%Y%m%d")]
            except ValueError:
                continue

            # Procesar cada día y horario
            for date_obj in date_range:
                for horario_str in horarios_list:
                    parts = horario_str.split('-')
                    start_t = datetime.strptime(parts[0], "%H:%M").time()
                    end_t = datetime.strptime(parts[1], "%H:%M").time() if len(parts) > 1 else start_t

                    start_minute = start_t.hour * 60 + start_t.minute
                    end_minute = end_t.hour * 60 + end_t.minute

                    # Contar archivos para cada item (producto/banda)
                    for item in items_to_process:
                        # Para items CMIP_Cxx, usar periodicidad de CMIP
                        lookup_item = "CMIP" if item.startswith("CMIP_") else item
                        
                        periodicity = self.get_periodicity(nivel, dominio, lookup_item)
                        
                        # Contar cuántos archivos se generan en el rango horario
                        # Manejar casos donde el rango puede cruzar medianoche
                        if end_minute >= start_minute:
                            # Rango normal dentro del mismo día
                            minute_range = end_minute - start_minute + 1
                        else:
                            # Rango que cruza medianoche (ej: 23:00-01:00)
                            minute_range = (1440 - start_minute) + end_minute + 1
                        
                        # Calcular archivos basándose en la periodicidad y dominio
                        files_in_range = 0
                        current_minute = start_minute
                        
                        for _ in range(minute_range):
                            actual_minute = current_minute % 1440  # Manejar wrap-around de 24h
                            
                            # Aplicar lógica específica según dominio
                            should_include = False
                            if dominio == 'fd':
                                # Full Disk: archivos en minutos múltiplos de la periodicidad (0, 10, 20, etc.)
                                should_include = (actual_minute % periodicity == 0)
                            elif dominio == 'conus':
                                # CONUS: archivos cada 5 mins, en minutos terminados en 1 o 6
                                minute_mod_10 = actual_minute % 10
                                should_include = (minute_mod_10 == 1 or minute_mod_10 == 6)
                            
                            if should_include:
                                files_in_range += 1
                            current_minute += 1
                        
                        total_files += files_in_range

        return total_files

    def estimate_files_size(self, request_data: Dict[str, Any]) -> float:
        """
        Estima el tamaño total en MB de los archivos que se recuperarán basándose en:
        - La cantidad de archivos (usando estimate_file_count)
        - El peso individual de cada tipo de archivo según goes_typical.csv
        
        Returns:
            Tamaño estimado total en MB
        """
        nivel = request_data.get("nivel")
        if nivel not in ["L1b", "L2"]:
            return 0.0

        dominio = request_data.get("dominio")
        fechas_dict = request_data.get("fechas", {})
        total_size_mb = 0.0

        # Obtener items a procesar (productos para L2, bandas para L1b)
        items_to_process = []
        if nivel == "L1b":
            bandas = request_data.get("bandas", [])
            if "ALL" in bandas:
                items_to_process = self.VALID_BANDAS
            else:
                items_to_process = bandas if bandas else ["C02"]
        elif nivel == "L2":
            productos = request_data.get("productos", [])
            if not productos:
                return 0.0
            
            # Para CMIP, generar items por cada banda
            if "CMIP" in productos:
                bandas = request_data.get("bandas", [])
                if "ALL" in bandas or not bandas:
                    items_to_process.extend([f"CMIP_C{i:02d}" for i in range(1, 17)])
                else:
                    items_to_process.extend([f"CMIP_{banda}" for banda in bandas])
                
                productos = [p for p in productos if p != "CMIP"]
            
            items_to_process.extend(productos)

        # Procesar cada fecha y horario
        for fecha_key, horarios_list in fechas_dict.items():
            # Expandir rangos de fechas
            try:
                if '-' in fecha_key:
                    start_date_str, end_date_str = fecha_key.split('-')
                    start_date = datetime.strptime(start_date_str, "%Y%m%d")
                    end_date = datetime.strptime(end_date_str, "%Y%m%d")
                    
                    current_date = start_date
                    date_range = []
                    while current_date <= end_date:
                        date_range.append(current_date)
                        current_date += timedelta(days=1)
                else:
                    date_range = [datetime.strptime(fecha_key, "%Y%m%d")]
            except ValueError:
                continue

            # Procesar cada día y horario
            for date_obj in date_range:
                for horario_str in horarios_list:
                    parts = horario_str.split('-')
                    start_t = datetime.strptime(parts[0], "%H:%M").time()
                    end_t = datetime.strptime(parts[1], "%H:%M").time() if len(parts) > 1 else start_t

                    start_minute = start_t.hour * 60 + start_t.minute
                    end_minute = end_t.hour * 60 + end_t.minute

                    # Calcular tamaño para cada item (producto/banda)
                    for item in items_to_process:
                        # Para items CMIP_Cxx, usar periodicidad y peso de CMIP
                        lookup_item = "CMIP" if item.startswith("CMIP_") else item
                        
                        periodicity = self.get_periodicity(nivel, dominio, lookup_item)
                        file_weight = self.get_file_weight(nivel, dominio, lookup_item)
                        
                        # Contar archivos en este rango horario
                        if end_minute >= start_minute:
                            minute_range = end_minute - start_minute + 1
                        else:
                            minute_range = (1440 - start_minute) + end_minute + 1
                        
                        files_in_range = 0
                        current_minute = start_minute
                        
                        for _ in range(minute_range):
                            actual_minute = current_minute % 1440
                            
                            # Aplicar lógica específica según dominio
                            should_include = False
                            if dominio == 'fd':
                                # Full Disk: archivos en minutos múltiplos de la periodicidad
                                should_include = (actual_minute % periodicity == 0)
                            elif dominio == 'conus':
                                # CONUS: archivos cada 5 mins, en minutos terminados en 1 o 6
                                minute_mod_10 = actual_minute % 10
                                should_include = (minute_mod_10 == 1 or minute_mod_10 == 6)
                            
                            if should_include:
                                files_in_range += 1
                            current_minute += 1
                        
                        # Sumar el tamaño total para este item
                        total_size_mb += files_in_range * file_weight

        return total_size_mb

    def estimate_files_summary(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Devuelve un resumen completo de la estimación de archivos.
        
        Returns:
            Dict con 'file_count' (int), 'total_size_mb' (float) y 'average_file_size_mb' (float)
        """
        file_count = self.estimate_file_count(request_data)
        total_size_mb = self.estimate_files_size(request_data)
        
        average_size = total_size_mb / file_count if file_count > 0 else 0.0
        
        return {
            "file_count": file_count,
            "total_size_mb": round(total_size_mb, 2),
            "average_file_size_mb": round(average_size, 2),
            "total_size_gb": round(total_size_mb / 1024, 3)
        }
