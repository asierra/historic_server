"""Que un Lustre caído no se confunda con un archivo incompleto.

`/depot` se desmonta durante semanas por cortes de corriente y discos dañados.
La consulta se sirve igual desde S3 —lo correcto cuando la avería dura tanto—
pero antes eso era indistinguible de «el archivo no tiene esas fechas»: las dos
cosas eran un directorio ausente, el mismo aviso repetido una vez por día
pedido, y un `fuentes.lustre.total` de 0 sin explicación.

Es lo que dejó ilegibles los resultados de mayo de 2026.
"""

import logging

import pytest

from database import ConsultasDatabase
from recover import RecoverFiles, LustreRecoverFiles


QUERY = {
    "sensor": "abi",
    "nivel": "L1b",
    "dominio": "fd",
    "fechas": {"2020029": ["00:00-23:59"]},
    "_original_request": {"bandas": ["13"]},
}


@pytest.fixture
def db(tmp_path):
    return ConsultasDatabase(str(tmp_path / "c.db"))


def _recuperador(db, tmp_path, source, lustre=True):
    """RecoverFiles sin S3 ni pool: aquí no se procesa ningún archivo."""
    return RecoverFiles(
        db=db,
        source_data_path=str(source),
        base_download_path=str(tmp_path / "descargas"),
        executor=None,
        s3_enabled=False,
        lustre_enabled=lustre,
        max_workers=1,
    )


def _reporte(db, consulta_id):
    return db.obtener_consulta(consulta_id)["resultados"]["fuentes"]["lustre"]


def test_lustre_montado_pero_sin_esas_fechas_es_ok(db, tmp_path):
    """Una laguna real del archivo: el volumen está, esas fechas no."""
    raiz = tmp_path / "depot"
    (raiz / "abi" / "l1b" / "fd" / "2020").mkdir(parents=True)  # el año sí, la semana no
    assert db.crear_consulta("LAGUNA01", QUERY)

    _recuperador(db, tmp_path, raiz).procesar_consulta("LAGUNA01", QUERY)

    lustre = _reporte(db, "LAGUNA01")
    assert lustre["estado"] == "ok"
    assert lustre["total"] == 0


def test_lustre_desmontado_se_distingue_de_una_laguna(db, tmp_path):
    """Mismo total de 0, causa distinta, y ahora el reporte lo dice."""
    assert db.crear_consulta("CAIDA001", QUERY)

    _recuperador(db, tmp_path, tmp_path / "depot-que-no-existe").procesar_consulta(
        "CAIDA001", QUERY
    )

    lustre = _reporte(db, "CAIDA001")
    assert lustre["estado"] == "no_disponible"
    assert lustre["total"] == 0


def test_lustre_apagado_a_proposito_tambien_se_distingue(db, tmp_path):
    """Ni avería ni laguna: alguien puso LUSTRE_ENABLED=False."""
    assert db.crear_consulta("APAGADO1", QUERY)

    _recuperador(db, tmp_path, tmp_path / "da-igual", lustre=False).procesar_consulta(
        "APAGADO1", QUERY
    )

    assert _reporte(db, "APAGADO1")["estado"] == "deshabilitado"


def test_con_la_raiz_caida_no_se_recorre_ningun_dia(db, tmp_path, caplog):
    """Y de paso deja de repetir el mismo aviso una vez por día pedido.

    Una consulta de 60 días producía 60 líneas idénticas a las de una laguna.
    Ahora sale un solo error, que además dice lo que pasa.
    """
    query = dict(QUERY, fechas={f"20200{d:02d}": ["00:00-23:59"] for d in range(29, 40)})
    assert db.crear_consulta("SILENCIO", query)

    with caplog.at_level(logging.WARNING):
        _recuperador(db, tmp_path, tmp_path / "no-existe").procesar_consulta(
            "SILENCIO", query
        )

    por_dia = [r for r in caplog.records if "Directorio no encontrado" in r.getMessage()]
    de_raiz = [r for r in caplog.records if "Lustre no disponible" in r.getMessage()]
    assert por_dia == [], "no debería recorrer días con el volumen caído"
    assert len(de_raiz) == 1
    assert de_raiz[0].levelno == logging.ERROR
