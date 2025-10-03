from typing import List, Dict, Any
from config_base import SatelliteConfigBase

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
            "DMW", "DMWV", "LST", "AVIATION_FOG"
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
