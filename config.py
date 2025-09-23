from typing import List, Dict, Any

class SatelliteConfigGOES:
    """Configuración simplificada para satélites GOES"""
    
    # Satélites válidos
    VALID_SATELLITES = ["GOES-EAST", "GOES-WEST", "GOES-16", "GOES-18", "GOES-19"]
    DEFAULT_SATELLITE = "GOES-EAST"
    
    # Niveles válidos
    VALID_LEVELS = ["L1b", "L2"]
    DEFAULT_LEVEL = "L1b"
    
    # Dominios válidos
    VALID_DOMAINS = ["fd", "conus"]
    
    # Productos válidos
    VALID_PRODUCTS = ["CMIP", "ACTP", "CMI", "RGBT", "Rainfall", "Cloud", "Aerosol"]
    
    # Bandas válidas (formato "01" al "16")
    VALID_BANDAS = [f"{i:02d}" for i in range(1, 17)]
    ALL_BANDAS = "ALL"
    VALID_BANDAS_INCLUDING_ALL = VALID_BANDAS + [ALL_BANDAS]
    DEFAULT_BANDAS = [ALL_BANDAS]  # Todas las bandas por defecto
    
    @classmethod
    def is_valid_satellite(cls, satellite: str) -> bool:
        """Verifica si un satélite es válido"""
        return satellite in cls.VALID_SATELLITES
    
    @classmethod
    def is_valid_level(cls, level: str) -> bool:
        """Verifica si un nivel es válido"""
        return level in cls.VALID_LEVELS
    
    @classmethod
    def is_valid_domain(cls, domain: str) -> bool:
        """Verifica si un dominio es válido"""
        return domain in cls.VALID_DOMAINS
    
    @classmethod
    def is_valid_banda(cls, banda: str) -> bool:
        """Verifica si una banda es válida (formato "01"-"16" o "ALL")"""
        return banda in cls.VALID_BANDAS_INCLUDING_ALL
    
    @classmethod
    def validate_bandas(cls, bandas: List[str]) -> List[str]:
        """Valida y procesa la lista de bandas"""
        if not bandas:
            return cls.DEFAULT_BANDAS
        
        # Si hay "ALL", devolver todas las bandas
        if cls.ALL_BANDAS in bandas:
            return cls.VALID_BANDAS.copy()
        
        # Filtrar bandas válidas
        bandas_validadas = [banda for banda in bandas if cls.is_valid_banda(banda)]
        return bandas_validadas if bandas_validadas else cls.DEFAULT_BANDAS
    
    @classmethod
    def expand_bandas(cls, bandas: List[str]) -> List[str]:
        """Expande "ALL" a la lista completa de bandas"""
        if cls.ALL_BANDAS in bandas:
            return cls.VALID_BANDAS.copy()
        return bandas
    
    @classmethod
    def is_all_bandas(cls, bandas: List[str]) -> bool:
        """Verifica si la lista de bandas equivale a 'ALL'"""
        return (cls.ALL_BANDAS in bandas or 
                set(bandas) == set(cls.VALID_BANDAS))
