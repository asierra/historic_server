from typing import List, Dict, Any

# Configuración centralizada
class SatelliteConfig:
    """Configuración centralizada de satélites"""
    
    # Lista de satélites válidos
    VALID_SATELLITES = ["GOES-EAST", "GOES-WEST", "GOES-16", "GOES-18", "GOES-19"]
    
    # Satélite por defecto
    DEFAULT_SATELLITE = "GOES-EAST"
    
    # Metadatos adicionales de satélites (opcional)
    SATELLITE_METADATA = {
        "GOES-16": {"region": "East", "launch_year": 2016},
        "GOES-17": {"region": "West", "launch_year": 2018},
        "GOES-18": {"region": "West", "launch_year": 2022},
        "GOES-19": {"region": "East", "launch_year": 2024},
        "GOES-EAST": {"region": "East", "alias": "GOES-16"},
        "GOES-WEST": {"region": "West", "alias": "GOES-18"}
    }
    
    # Niveles válidos
    VALID_LEVELS = ["L1b", "L2"]
    DEFAULT_LEVEL = "L1b"
    
    # Productos válidos
    VALID_PRODUCTS = ["CMIP", "ACTP", "CMI", "RGBT", "Rainfall"]
    
    @classmethod
    def get_valid_satellites(cls) -> List[str]:
        return cls.VALID_SATELLITES.copy()
    
    @classmethod
    def is_valid_satellite(cls, satellite: str) -> bool:
        return satellite in cls.VALID_SATELLITES
    
    @classmethod
    def get_satellite_metadata(cls, satellite: str) -> Dict[str, Any]:
        return cls.SATELLITE_METADATA.get(satellite, {})
