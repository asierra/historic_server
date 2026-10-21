"""El árbol diario de Lustre (`/depot/goes-east`, desde 2026).

Desde 2026 el acervo vive en `{nivel}/{sensor}/{dominio}/AAAA/JJJ/` con `.nc`
sueltos, en vez de `{sensor}/{nivel}/{dominio}/AAAA/SS/*.tgz`. En L2 cada
producto tiene su carpeta (`ACHAC`, `CMIPC-C13`). Los días de 2026 que aún no
se migran siguen en el árbol semanal, así que de ahí se toman mientras tanto.

Los nombres de archivo y carpeta son los que hay en tahan (21-oct-2026).
"""

import logging
import tarfile

import pytest

from database import ConsultasDatabase
from recover import LustreRecoverFiles, RecoverFiles


def _nc_l1b(dominio_letra, banda, jjj, hhmm):
    return (f"OR_ABI-L1b-Rad{dominio_letra}-M6C{banda}_G19_s{jjj}{hhmm}177_"
            f"e{jjj}{hhmm}551_c{jjj}{hhmm}577.nc")


def _nc_l2(carpeta, jjj, hhmm):
    producto = carpeta.split("-")[0]
    banda = f"C{carpeta.split('-C')[1]}" if "-C" in carpeta else ""
    return (f"CG_ABI-L2-{producto}-M6{banda}_G19_s{jjj}{hhmm}177_"
            f"e{jjj}{hhmm}563_c{jjj}{hhmm}320.nc")


@pytest.fixture
def arboles(tmp_path):
    """Un /depot falso con los dos árboles."""
    semanal = tmp_path / "goes16"
    diario = tmp_path / "goes-east"
    semanal.mkdir()
    diario.mkdir()
    return semanal, diario


def _lustre(arboles):
    semanal, diario = arboles
    return LustreRecoverFiles(str(semanal), logging.getLogger("test"),
                              source_path_diario=str(diario), diario_desde_anio=2026)


def _dia_l1b(diario, jjj, bandas=("01", "13"), horas=("0000", "1200")):
    d = diario / "l1b" / "abi" / "conus" / jjj[:4] / jjj[4:]
    d.mkdir(parents=True)
    for b in bandas:
        for h in horas:
            (d / _nc_l1b("C", b, jjj, h)).write_bytes(b"x")
    return d


def _tgz_semanal(semanal, jjj, hhmm, miembros, nivel="l1b", dominio="conus"):
    anio, doy = jjj[:4], int(jjj[4:])
    d = semanal / "abi" / nivel / dominio / anio / f"{(doy - 1) // 7 + 1:02d}"
    d.mkdir(parents=True, exist_ok=True)
    ruta = d / f"ABI-L1b-RadC-M6_G19-s{jjj}{hhmm}.tgz"
    fuente = d / "_contenido"
    fuente.mkdir(exist_ok=True)
    with tarfile.open(ruta, "w:gz") as tar:
        for nombre in miembros:
            (fuente / nombre).write_bytes(b"x")
            tar.add(fuente / nombre, arcname=nombre)
    return ruta


QUERY_L1B = {
    "sensor": "abi", "nivel": "L1b", "dominio": "conus",
    "bandas": ["13"], "productos": [],
}


def test_l1b_2026_sale_del_arbol_diario_filtrado_por_banda_y_hora(arboles):
    _, diario = arboles
    _dia_l1b(diario, "2026293")
    q = dict(QUERY_L1B, fechas={"2026293": ["11:30-12:30"]})

    archivos = _lustre(arboles).discover_and_filter_files(q)

    assert [a.name for a in archivos] == [_nc_l1b("C", "13", "2026293", "1200")]


def test_l1b_todas_las_bandas_trae_las_dieciseis_sueltas(arboles):
    _, diario = arboles
    bandas = [f"{i:02d}" for i in range(1, 17)]
    _dia_l1b(diario, "2026100", bandas=bandas, horas=("0600",))
    q = dict(QUERY_L1B, bandas=bandas, fechas={"2026100": ["06:00-06:10"]})

    archivos = _lustre(arboles).discover_and_filter_files(q)

    assert len(archivos) == 16
    assert all(a.suffix == ".nc" for a in archivos)


def test_l2_entra_solo_a_las_carpetas_del_producto_y_banda_pedidos(arboles):
    _, diario = arboles
    d = diario / "l2" / "abi" / "conus" / "2026" / "293"
    for carpeta in ("ACHAC", "ACMC", "CMIPC-C01", "CMIPC-C13", "DMWC-C07", "DMWC-C14"):
        (d / carpeta).mkdir(parents=True)
        (d / carpeta / _nc_l2(carpeta, "2026293", "1200")).write_bytes(b"x")
    q = {
        "sensor": "abi", "nivel": "L2", "dominio": "conus",
        "productos": ["ACHA", "CMIP", "DMW"], "bandas": ["13"],
        "fechas": {"2026293": ["12:00-12:05"]},
    }

    nombres = sorted(a.parent.name for a in _lustre(arboles).discover_and_filter_files(q))

    # ACM no se pidió; CMIP sólo en la banda 13; DMW en todas, porque ahí la
    # banda es parte del producto y la consulta no la elige.
    assert nombres == ["ACHAC", "CMIPC-C13", "DMWC-C07", "DMWC-C14"]


def test_l2_full_disk_usa_la_letra_f(arboles):
    _, diario = arboles
    d = diario / "l2" / "abi" / "fd" / "2026" / "200" / "ACHAF"
    d.mkdir(parents=True)
    (d / _nc_l2("ACHAF", "2026200", "0000")).write_bytes(b"x")
    q = {"sensor": "abi", "nivel": "L2", "dominio": "fd", "productos": ["ACHA"],
         "bandas": [], "fechas": {"2026200": ["00:00-00:05"]}}

    assert len(_lustre(arboles).discover_and_filter_files(q)) == 1


def test_dia_de_2026_sin_migrar_cae_al_arbol_semanal(arboles):
    semanal, diario = arboles
    _dia_l1b(diario, "2026293")  # el árbol diario existe, pero no ese día
    tgz = _tgz_semanal(semanal, "2026050", "1200", [_nc_l1b("C", "13", "2026050", "1200")])
    q = dict(QUERY_L1B, fechas={"2026050": ["12:00-12:05"]})

    assert _lustre(arboles).discover_and_filter_files(q) == [tgz]


def test_anio_previo_no_mira_el_arbol_diario(arboles):
    """Aunque alguien deje un 2025 en el árbol nuevo, antes del corte manda el semanal."""
    semanal, diario = arboles
    _dia_l1b(diario, "2025300")
    tgz = _tgz_semanal(semanal, "2025300", "1200", [_nc_l1b("C", "13", "2025300", "1200")])
    q = dict(QUERY_L1B, fechas={"2025300": ["11:00-13:00"]})

    assert _lustre(arboles).discover_and_filter_files(q) == [tgz]


def test_una_consulta_que_cruza_el_anio_usa_un_arbol_por_dia(arboles):
    semanal, diario = arboles
    tgz = _tgz_semanal(semanal, "2025365", "2300", [_nc_l1b("C", "13", "2025365", "2300")])
    _dia_l1b(diario, "2026001", horas=("0000",))
    q = dict(QUERY_L1B, fechas={"2025365": ["23:00-23:59"], "2026001": ["00:00-00:30"]})

    archivos = _lustre(arboles).discover_and_filter_files(q)

    assert set(archivos) == {tgz, diario / "l1b/abi/conus/2026/001" / _nc_l1b("C", "13", "2026001", "0000")}


def test_retomar_no_salta_bandas_que_comparten_timestamp(arboles, tmp_path):
    """Las 16 bandas de un instante comparten timestamp: comparar por él daría
    por copiadas C02-C16 en cuanto existiera C01."""
    _, diario = arboles
    d = _dia_l1b(diario, "2026293", bandas=("01", "02"), horas=("1200",))
    destino = tmp_path / "destino"
    destino.mkdir()
    (destino / _nc_l1b("C", "01", "2026293", "1200")).write_bytes(b"x")

    pendientes = _lustre(arboles).scan_existing_files(sorted(d.iterdir()), destino)

    assert [p.name for p in pendientes] == [_nc_l1b("C", "02", "2026293", "1200")]


class _PoolEnLinea:
    """Ejecuta en el mismo proceso lo que el ProcessPool mandaría a otro."""

    class _Futuro:
        def __init__(self, fn, args):
            self._fn, self._args = fn, args
            self._done = True

        def result(self, timeout=None):
            return self._fn(*self._args)

    def schedule(self, fn, args=(), timeout=None):
        from concurrent.futures import Future
        f = Future()
        try:
            f.set_result(fn(*args))
        except Exception as e:  # pragma: no cover - se reporta igual que en el pool
            f.set_exception(e)
        return f


def test_de_punta_a_punta_los_nc_se_copian_sueltos(arboles, tmp_path):
    semanal, diario = arboles
    _dia_l1b(diario, "2026293")
    db = ConsultasDatabase(str(tmp_path / "c.db"))
    q = dict(QUERY_L1B, fechas={"2026293": ["11:30-12:30"]},
             _original_request={"bandas": ["13"], "productos": []})
    assert db.crear_consulta("DIARIO01", q)

    RecoverFiles(
        db=db, source_data_path=str(semanal), source_path_diario=str(diario),
        base_download_path=str(tmp_path / "descargas"), executor=_PoolEnLinea(),
        s3_enabled=False, lustre_enabled=True, max_workers=1,
    ).procesar_consulta("DIARIO01", q)

    lustre = db.obtener_consulta("DIARIO01")["resultados"]["fuentes"]["lustre"]
    assert lustre["estado"] == "ok"
    assert lustre["archivos"] == [_nc_l1b("C", "13", "2026293", "1200")]


def test_sin_ninguna_raiz_lustre_esta_no_disponible(tmp_path):
    db = ConsultasDatabase(str(tmp_path / "c.db"))
    q = dict(QUERY_L1B, fechas={"2026293": ["12:00-12:05"]}, _original_request={})
    assert db.crear_consulta("NIGUNA01", q)

    RecoverFiles(
        db=db, source_data_path=str(tmp_path / "no1"), source_path_diario=str(tmp_path / "no2"),
        base_download_path=str(tmp_path / "descargas"), executor=None,
        s3_enabled=False, lustre_enabled=True, max_workers=1,
    ).procesar_consulta("NIGUNA01", q)

    assert db.obtener_consulta("NIGUNA01")["resultados"]["fuentes"]["lustre"]["estado"] == "no_disponible"
