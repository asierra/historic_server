from typing import List, Dict, Any

class SatelliteConfigGOES:
    """Configuración para descarga histórica de satélites GOES"""
    
    # Satélites válidos
    VALID_SATELLITES = ["GOES-EAST", "GOES-WEST", "GOES-16", "GOES-18", "GOES-19"]
    DEFAULT_SATELLITE = "GOES-EAST"
    
    # Niveles válidos
    VALID_LEVELS = ["L1b", "L2"]
    DEFAULT_LEVEL = "L1b"
    
    # Dominios válidos
    VALID_DOMAINS = ["fd", "conus"]
    
    # Productos válidos
    VALID_PRODUCTS = [
        "ADP", "AOD", "ACM", "CMIP", "CODD", "CODN", "CPSD", "CPSN",
        "ACHA", "ACTP", "CTP", "ACHT", "Rainfall", "SST", "TPW", 
        "DMW", "DMWV", "LST", "AVIATION_FOG"
    ]
    
    # Bandas válidas (formato "01" al "16")
    VALID_BANDAS = [f"{i:02d}" for i in range(1, 17)]
    ALL_BANDAS = "ALL"
    VALID_BANDAS_INCLUDING_ALL = VALID_BANDAS + [ALL_BANDAS]
    DEFAULT_BANDAS = [ALL_BANDAS]
    
    @classmethod
    def is_valid_satellite(cls, satellite: str) -> bool:
        return satellite in cls.VALID_SATELLITES
    
    @classmethod
    def is_valid_level(cls, level: str) -> bool:
        return level in cls.VALID_LEVELS
    
    @classmethod
    def is_valid_domain(cls, domain: str) -> bool:
        return domain in cls.VALID_DOMAINS
    
    @classmethod
    def is_valid_banda(cls, banda: str) -> bool:
        return banda in cls.VALID_BANDAS_INCLUDING_ALL
    
    @classmethod
    def validate_bandas(cls, bandas: List[str]) -> List[str]:
        """Valida bandas y DEVUELVE ERROR si hay bandas inválidas"""
        if not bandas:
            raise ValueError("La lista de bandas no puede estar vacía")
        
        # Si hay "ALL", devolver todas las bandas (ignorando las demás)
        if cls.ALL_BANDAS in bandas:
            return cls.VALID_BANDAS.copy()
        
        # Verificar que todas las bandas sean válidas
        bandas_invalidas = [banda for banda in bandas if not cls.is_valid_banda(banda)]
        if bandas_invalidas:
            raise ValueError(f"Bandas inválidas: {bandas_invalidas}. Bandas válidas: {cls.VALID_BANDAS_INCLUDING_ALL}")
        
        return bandas
    
    @classmethod
    def expand_bandas(cls, bandas: List[str]) -> List[str]:
        """Expande "ALL" a la lista completa de bandas"""
        if cls.ALL_BANDAS in bandas:
            return cls.VALID_BANDAS.copy()
        return bandas
