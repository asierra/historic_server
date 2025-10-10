import pytest
from config import SatelliteConfigGOES


def test_estimate_july_full_month_l1b_conus():
    """Verifica la estimación para L1b CONUS en julio completo.

    Basado en ejecuciones previas, esperamos:
      - archivos: 142,848
      - tamaño total MB: ~1,106,447.04
    """
    config = SatelliteConfigGOES()

    request_data = {
        "nivel": "L1b",
        "dominio": "conus",
        "bandas": [f"{i:02d}" for i in range(1, 17)],
        "fechas": {
            "20230701-20230731": ["00:00-23:59"]
        }
    }

    summary = config.estimate_files_summary(request_data)

    # Comprobaciones básicas
    assert 'file_count' in summary and 'total_size_mb' in summary

    # Valores esperados (exactos para file_count, aproximado para tamaño)
    assert summary['file_count'] == 142_848
    assert pytest.approx(summary['total_size_mb'], rel=1e-3) == 1_106_447.04
