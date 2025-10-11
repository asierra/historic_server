from typing import List, Dict, Any
from config_base import SatelliteConfigBase
from datetime import datetime, timedelta, time

class SatelliteConfigGOES(SatelliteConfigBase):
    """Configuration specific to GOES satellites."""
    
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

    def estimate_file_count(self, request_data: Dict[str, Any]) -> int:
        """
        Estima la cantidad de archivos .tgz que se recuperarán basándose en la cadencia del satélite.
        """
        nivel = request_data.get("nivel")
        if nivel not in ["L1b", "L2"]:
            # La lógica de cadencia es la misma para L1b y L2.
            # Si en el futuro se añade un nivel con otra cadencia, se deberá modificar.
            return 0 # O lanzar un error si se prefiere.

        dominio = request_data.get("dominio")
        fechas_dict = request_data.get("fechas", {})
        total_files = 0

        # --- Lógica de Multiplicador por Productos y Bandas ---
        # Un solo instante de tiempo puede generar múltiples archivos.
        file_multiplier = 0
        if nivel == "L1b":
            bandas = request_data.get("bandas", [])
            # Si se pide "ALL", el fallback a S3 recupera un .nc por cada banda.
            # Si se piden bandas específicas, se recupera un .nc por cada una.
            # Si se copia el .tgz de Lustre, se cuenta como 1.
            if "ALL" in bandas:
                file_multiplier = len(self.VALID_BANDAS) # 16
            else:
                file_multiplier = len(bandas) if bandas else 1
        elif nivel == "L2":
            productos = request_data.get("productos", [])
            bandas = request_data.get("bandas", [])

            # Si no se especifican productos para L2, no se puede estimar.
            if not productos:
                return 0

            if "CMIP" in productos:
                # CMIP genera un archivo .nc por cada banda.
                num_bandas = len(self.VALID_BANDAS) if not bandas or "ALL" in bandas else len(bandas)
                file_multiplier += num_bandas
            
            # Otros productos como ACTP generan 1 archivo .nc por instante, sin importar las bandas.
            file_multiplier += sum(1 for p in productos if p != "CMIP")

        for fecha_key, horarios_list in fechas_dict.items():
            # 1. Expandir rangos de fechas (ej. "20230101-20230103")
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
                continue # Ignorar claves de fecha con formato incorrecto

            # 2. Iterar sobre cada día y cada rango horario
            for date_obj in date_range:
                for horario_str in horarios_list:
                    parts = horario_str.split('-')
                    start_t = datetime.strptime(parts[0], "%H:%M").time()
                    end_t = datetime.strptime(parts[1], "%H:%M").time() if len(parts) > 1 else start_t

                    current_minute = start_t.hour * 60 + start_t.minute
                    end_minute = end_t.hour * 60 + end_t.minute

                    # 3. Contar archivos según la cadencia del dominio
                    while current_minute <= end_minute:
                        if dominio == 'fd':
                            # 1 archivo cada 10 minutos (00, 10, 20...)
                            if current_minute % 10 == 0:
                                total_files += file_multiplier
                        elif dominio == 'conus':
                            # 1 archivo cada 5 mins, en minutos terminados en 1 o 6
                            if current_minute % 10 == 1 or current_minute % 10 == 6:
                                total_files += file_multiplier
                        current_minute += 1
        return total_files
