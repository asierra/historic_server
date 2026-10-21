"""tools/migrar_2026.py: pasar 2026 al árbol diario sin perder nada.

Los nombres son los que hay en cirrus (21-oct-2026), incluidos los .tgz en
modo M4 y los L2 con prefijo CG_.
"""

import importlib.util
import json
import logging
import os
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

from recover import LustreRecoverFiles

_spec = importlib.util.spec_from_file_location(
    "migrar_2026", str(Path(__file__).resolve().parents[1] / "tools" / "migrar_2026.py"))
mig = importlib.util.module_from_spec(_spec)
sys.modules["migrar_2026"] = mig  # para que multiprocessing pueda importarlo
_spec.loader.exec_module(mig)


def _l1b(banda, jjj, hhmm, modo="M6"):
    return f"OR_ABI-L1b-RadC-{modo}C{banda}_G19_s{jjj}{hhmm}171_e{jjj}{hhmm}544_c{jjj}{hhmm}584.nc"


def _l2(producto, jjj, hhmm, banda=None, modo="M6"):
    b = f"C{banda}" if banda else ""
    return f"CG_ABI-L2-{producto}-{modo}{b}_G19_s{jjj}{hhmm}171_e{jjj}{hhmm}555_c{jjj}{hhmm}310.nc"


@pytest.fixture
def depot(tmp_path):
    vieja, nueva = tmp_path / "goes16", tmp_path / "goes-east"
    vieja.mkdir()
    nueva.mkdir()
    return vieja, nueva


def _tgz(vieja, nivel, jjj, hhmm, miembros, dominio="conus", contenido=b"x"):
    doy = int(jjj[4:])
    d = vieja / "abi" / nivel / dominio / jjj[:4] / f"{(doy - 1) // 7 + 1:02d}"
    d.mkdir(parents=True, exist_ok=True)
    tipo = "L1b-RadC" if nivel == "l1b" else "L2C"
    ruta = d / f"ABI-{tipo}-M6_G19-s{jjj}{hhmm}.tgz"
    fuente = d / f"_src_{hhmm}"
    fuente.mkdir(exist_ok=True)
    with tarfile.open(ruta, "w:gz") as tar:
        for nombre in miembros:
            (fuente / nombre).write_bytes(contenido)
            tar.add(fuente / nombre, arcname=nombre)
    for f in fuente.iterdir():
        f.unlink()
    fuente.rmdir()
    return ruta


def _temporal(nueva, nivel, jjj, nombres, dominio="conus", contenido=b"x"):
    d = nueva / "abi" / nivel / dominio / jjj[:4] / jjj[4:]
    d.mkdir(parents=True, exist_ok=True)
    for n in nombres:
        (d / n).write_bytes(contenido)
    return d


# Compresor de mentira para L2: antepone "Z" y así se ve qué pasó por él.
# Lo de verdad (nccopy) se prueba aparte, con un netCDF real.
_COMPRESOR_FALSO = Path(__file__).with_name("_compresor_falso.py")


@pytest.fixture(autouse=True)
def _compresor_falso():
    _COMPRESOR_FALSO.write_text(
        "import sys\n"
        "src, dst = sys.argv[-2:]\n"
        "if open(src, 'rb').read().startswith(b'ROTO'): sys.exit('nccopy: NetCDF: Unknown file format')\n"
        "open(dst, 'wb').write(b'Z' + open(src, 'rb').read())\n")
    yield
    _COMPRESOR_FALSO.unlink()


def _correr(depot, *extra, compresor=None):
    vieja, nueva = depot
    compresor = compresor or f"{sys.executable} {_COMPRESOR_FALSO}"
    return mig.main(["--raiz-vieja", str(vieja), "--raiz-nueva", str(nueva), "--procesos", "1",
                     "--compresor", compresor, *extra])


def _final(nueva, nivel, jjj, dominio="conus"):
    return nueva / nivel / "abi" / dominio / jjj[:4] / jjj[4:]


def _todo(raiz):
    return sorted(str(p.relative_to(raiz)) for p in raiz.rglob("*") if p.is_file())


def test_reglas_de_subcarpeta_coinciden_con_las_de_tahan():
    assert mig.subcarpeta("l1b", _l1b("13", "2026050", "1200")) == ""
    assert mig.subcarpeta("l2", _l2("ACHAC", "2026050", "1200")) == "ACHAC"
    assert mig.subcarpeta("l2", _l2("CMIPC", "2026050", "1200", "13")) == "CMIPC-C13"
    assert mig.subcarpeta("l2", _l2("DMWVC", "2026050", "1200", "08")) == "DMWVC-C08"
    assert mig.subcarpeta("l2", _l2("CMIPF", "2026050", "1200", "01", modo="M4")) == "CMIPF-C01"
    assert mig.subcarpeta("l2", "algo-raro.nc") is None


def test_simulacro_no_toca_nada(depot):
    vieja, nueva = depot
    _tgz(vieja, "l1b", "2026050", "1200", [_l1b("13", "2026050", "1200")])
    antes = _todo(vieja.parent)

    assert _correr(depot) == 0

    assert _todo(vieja.parent) == antes
    assert not (nueva / ".migracion").exists()


def test_l1b_semanal_al_diario_y_se_borra_el_tgz_verificado(depot):
    vieja, nueva = depot
    tgz = _tgz(vieja, "l1b", "2026050", "1200", [_l1b(b, "2026050", "1200") for b in ("01", "13")])

    assert _correr(depot, "--ejecutar") == 0

    assert _todo(_final(nueva, "l1b", "2026050")) == sorted(
        [_l1b("01", "2026050", "1200"), _l1b("13", "2026050", "1200")])
    assert not tgz.exists()
    man = json.loads((nueva / ".migracion/manifiestos/l1b/conus/2026" / (tgz.name + ".json")).read_text())
    assert man["completo"] and "borrado" in man


def test_l2_reparte_por_producto_y_banda(depot):
    vieja, nueva = depot
    _tgz(vieja, "l2", "2026033", "1110", [
        _l2("ACHAC", "2026033", "1110", modo="M4"),
        _l2("CMIPC", "2026033", "1110", "13", modo="M4"),
        _l2("DMWC", "2026033", "1110", "07"),
    ])

    assert _correr(depot, "--ejecutar") == 0

    final = _final(nueva, "l2", "2026033")
    assert sorted(p.name for p in final.iterdir()) == ["ACHAC", "CMIPC-C13", "DMWC-C07"]
    # Todo L2 pasó por el compresor.
    assert {p.read_bytes() for p in final.rglob("*.nc")} == {b"Zx"}


def test_lo_migrado_lo_encuentra_el_recuperador(depot):
    """La prueba que importa: las carpetas que arma el script son las que
    busca recover.py, y el día ya no depende del árbol semanal."""
    vieja, nueva = depot
    _tgz(vieja, "l2", "2026033", "1201", [
        _l2("CMIPC", "2026033", "1201", "13"), _l2("CMIPC", "2026033", "1201", "01"),
        _l2("ACHAC", "2026033", "1201")])
    assert _correr(depot, "--ejecutar") == 0

    lustre = LustreRecoverFiles(str(vieja), logging.getLogger("t"), source_path_diario=str(nueva))
    q = {"sensor": "abi", "nivel": "L2", "dominio": "conus", "productos": ["CMIP", "ACHA"],
         "bandas": ["13"], "fechas": {"2026033": ["12:00-12:05"]}}
    arbol, archivos = lustre.buscar_dia(q, "2026033", ["12:00-12:05"])

    assert arbol == "diario"
    assert sorted(a.parent.name for a in archivos) == ["ACHAC", "CMIPC-C13"]


def test_temporal_se_mueve_y_su_carpeta_desaparece(depot):
    vieja, nueva = depot
    tmp = _temporal(nueva, "l2", "2026080", [_l2("ACHAC", "2026080", "0001"), _l2("CMIPC", "2026080", "0001", "02")])

    assert _correr(depot, "--ejecutar") == 0

    assert not tmp.exists()
    assert {p.read_bytes() for p in _final(nueva, "l2", "2026080").rglob("*.nc")} == {b"Zx"}
    assert _todo(_final(nueva, "l2", "2026080")) == sorted([
        f"ACHAC/{_l2('ACHAC', '2026080', '0001')}", f"CMIPC-C02/{_l2('CMIPC', '2026080', '0001', '02')}"])


def test_dia_en_los_dos_origenes_no_duplica_ni_choca(depot):
    """El 076 está a medias en el semanal y entero en el temporal."""
    vieja, nueva = depot
    comun = _l1b("13", "2026076", "0001")
    tgz = _tgz(vieja, "l1b", "2026076", "0001", [comun])
    _temporal(nueva, "l1b", "2026076", [comun, _l1b("13", "2026076", "0006")])

    assert _correr(depot, "--ejecutar") == 0

    assert _todo(_final(nueva, "l1b", "2026076")) == sorted([comun, _l1b("13", "2026076", "0006")])
    assert not tgz.exists()


def test_conflicto_de_tamano_retiene_el_dia_y_no_borra_nada(depot):
    vieja, nueva = depot
    nombre = _l1b("13", "2026076", "0001")
    tgz = _tgz(vieja, "l1b", "2026076", "0001", [nombre], contenido=b"version-larga")
    _temporal(nueva, "l1b", "2026076", [nombre], contenido=b"corta")

    assert _correr(depot, "--ejecutar") == 1

    assert not _final(nueva, "l1b", "2026076").exists()
    assert tgz.exists()


def test_tgz_corrupto_retiene_el_dia(depot):
    vieja, nueva = depot
    tgz = _tgz(vieja, "l1b", "2026050", "1200", [_l1b("13", "2026050", "1200")])
    tgz.write_bytes(b"no es gzip")

    assert _correr(depot, "--ejecutar") == 1

    assert tgz.exists()
    assert not _final(nueva, "l1b", "2026050").exists()


def test_se_retoma_tras_un_corte(depot):
    """Un proceso muerto a media extracción deja un .part en staging y ningún
    manifiesto; la siguiente corrida limpia y termina."""
    vieja, nueva = depot
    nombre = _l1b("13", "2026050", "1200")
    tgz = _tgz(vieja, "l1b", "2026050", "1200", [nombre])
    staging = nueva / ".migracion/staging/l1b/conus/2026/050"
    staging.mkdir(parents=True)
    (staging / (nombre + ".part")).write_bytes(b"mit")

    assert _correr(depot, "--ejecutar") == 0

    assert _todo(_final(nueva, "l1b", "2026050")) == [nombre]
    assert not tgz.exists()


def test_dia_que_ya_existe_en_destino_se_fusiona(depot):
    vieja, nueva = depot
    final = _final(nueva, "l1b", "2026050")
    final.mkdir(parents=True)
    ya = _l1b("01", "2026050", "1200")
    (final / ya).write_bytes(b"x")
    _tgz(vieja, "l1b", "2026050", "1200", [ya, _l1b("13", "2026050", "1200")])

    assert _correr(depot, "--ejecutar") == 0

    assert _todo(final) == sorted([ya, _l1b("13", "2026050", "1200")])


def test_conservar_origen(depot):
    vieja, nueva = depot
    tgz = _tgz(vieja, "l1b", "2026050", "1200", [_l1b("13", "2026050", "1200")])
    tmp = _temporal(nueva, "l1b", "2026080", [_l1b("13", "2026080", "0001")])

    assert _correr(depot, "--ejecutar", "--conservar-origen") == 0

    assert tgz.exists()
    assert _todo(_final(nueva, "l1b", "2026050")) == [_l1b("13", "2026050", "1200")]
    # El temporal se mueve igual: es un rename, no hay copia que conservar.
    assert _todo(_final(nueva, "l1b", "2026080")) == [_l1b("13", "2026080", "0001")]


def test_solo_y_dias_acotan(depot):
    vieja, nueva = depot
    _tgz(vieja, "l1b", "2026050", "1200", [_l1b("13", "2026050", "1200")])
    _tgz(vieja, "l1b", "2026051", "1200", [_l1b("13", "2026051", "1200")])
    _tgz(vieja, "l2", "2026050", "1200", [_l2("ACHAC", "2026050", "1200")])

    assert _correr(depot, "--ejecutar", "--solo", "l1b/conus", "--dias", "050") == 0

    assert _final(nueva, "l1b", "2026050").exists()
    assert not _final(nueva, "l1b", "2026051").exists()
    assert not _final(nueva, "l2", "2026050").exists()


def test_no_corre_dos_veces_a_la_vez(depot):
    import fcntl
    vieja, nueva = depot
    (nueva / ".migracion").mkdir()
    with open(nueva / ".migracion/migrar_2026.lock", "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert _correr(depot, "--ejecutar") == 2


def test_con_varios_procesos(depot):
    vieja, nueva = depot
    for jjj in ("2026050", "2026051", "2026052"):
        _tgz(vieja, "l1b", jjj, "1200", [_l1b("13", jjj, "1200")])

    assert _correr(depot, "--ejecutar", "--procesos", "2") == 0

    for jjj in ("2026050", "2026051", "2026052"):
        assert _todo(_final(nueva, "l1b", jjj)) == [_l1b("13", jjj, "1200")]


def test_l1b_no_se_recomprime(depot):
    vieja, nueva = depot
    _tgz(vieja, "l1b", "2026050", "1200", [_l1b("13", "2026050", "1200")])
    _temporal(nueva, "l1b", "2026080", [_l1b("13", "2026080", "0001")])

    assert _correr(depot, "--ejecutar") == 0

    assert {p.read_bytes() for p in (nueva / "l1b").rglob("*.nc")} == {b"x"}


def test_l2_retomado_no_recomprime_y_verifica_con_el_tamano_comprimido(depot):
    """Corte a mitad de un .tgz de L2: en staging hay un miembro ya comprimido
    y ningún manifiesto. No se puede comparar con el tamaño del miembro (al
    comprimir cambia): basta con que exista, porque sólo llega a su nombre
    final completo. El .tgz se borra igual, verificado contra lo comprimido."""
    vieja, nueva = depot
    hecho, falta = _l2("ACHAC", "2026050", "1200"), _l2("CTPC", "2026050", "1200")
    tgz = _tgz(vieja, "l2", "2026050", "1200", [hecho, falta])
    previo = nueva / ".migracion/staging/l2/conus/2026/050/ACHAC" / hecho
    previo.parent.mkdir(parents=True)
    previo.write_bytes(b"Zx-de-la-corrida-anterior")

    assert _correr(depot, "--ejecutar") == 0

    final = _final(nueva, "l2", "2026050")
    assert (final / "ACHAC" / hecho).read_bytes() == b"Zx-de-la-corrida-anterior"
    assert (final / "CTPC" / falta).read_bytes() == b"Zx"
    assert not tgz.exists()


def test_si_el_compresor_falla_el_dia_se_retiene(depot):
    vieja, nueva = depot
    tgz = _tgz(vieja, "l2", "2026050", "1200", [_l2("ACHAC", "2026050", "1200")], contenido=b"ROTO")

    assert _correr(depot, "--ejecutar") == 1

    assert tgz.exists()
    assert not _final(nueva, "l2", "2026050").exists()
    assert not list((nueva / ".migracion/staging").rglob("*.part"))


def test_sin_compresor_no_arranca_con_l2(depot):
    vieja, _ = depot
    _tgz(vieja, "l2", "2026050", "1200", [_l2("ACHAC", "2026050", "1200")])
    assert _correr(depot, "--ejecutar", compresor="no-existe-este-programa") == 2


@pytest.mark.skipif(not shutil.which("ncgen") or not shutil.which("nccopy"), reason="sin netcdf-bin")
def test_nccopy_de_verdad_comprime_como_el_arbol_diario(depot, tmp_path):
    """Con un netCDF real: sale con deflate 4 y shuffle, que es lo que trae
    /depot/goes-east/l2 (medido en cirrus el 21-oct-2026)."""
    vieja, nueva = depot
    cdl = tmp_path / "x.cdl"
    nc = tmp_path / "x.nc"
    cdl.write_text("netcdf x { dimensions: y = 300 ; x = 500 ; variables: short HT(y, x) ; data: HT = "
                   + ", ".join(["0"] * 150000) + " ; }")
    subprocess.run(["ncgen", "-k", "nc4", "-o", str(nc), str(cdl)], check=True)
    nombre = _l2("ACHAC", "2026050", "1200")
    d = vieja / "abi/l2/conus/2026/08"
    d.mkdir(parents=True)
    import shutil as _sh
    _sh.copy(nc, tmp_path / nombre)
    with tarfile.open(d / "ABI-L2C-M6_G19-s20260501200.tgz", "w:gz") as tar:
        tar.add(tmp_path / nombre, arcname=nombre)

    assert _correr(depot, "--ejecutar", compresor="nccopy -d 4 -s") == 0

    salida = _final(nueva, "l2", "2026050") / "ACHAC" / nombre
    cabecera = subprocess.run(["ncdump", "-hs", str(salida)], stdout=subprocess.PIPE, check=True).stdout.decode()
    assert "HT:_DeflateLevel = 4" in cabecera
    assert 'HT:_Shuffle = "true"' in cabecera
    assert salida.stat().st_size < nc.stat().st_size
