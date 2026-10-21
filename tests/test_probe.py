"""POST /probe: qué hay en Lustre para una solicitud, sin copiar ni encolar."""

import pytest
from fastapi.testclient import TestClient

import main
from database import ConsultasDatabase
from processors import HistoricQueryProcessor
from settings import settings

client = TestClient(main.app)


def _nc(banda, jjj, hhmm):
    return f"OR_ABI-L1b-RadC-M6C{banda}_G19_s{jjj}{hhmm}177_e{jjj}{hhmm}551_c{jjj}{hhmm}577.nc"


@pytest.fixture
def depot(tmp_path, monkeypatch):
    semanal, diario = tmp_path / "goes16", tmp_path / "goes-east"
    semanal.mkdir()
    diario.mkdir()
    monkeypatch.setattr(main, "SOURCE_DATA_PATH", str(semanal))
    monkeypatch.setattr(main, "SOURCE_PATH_DIARIO", str(diario))
    monkeypatch.setattr(main, "processor", HistoricQueryProcessor())
    monkeypatch.setattr(main, "db", ConsultasDatabase(str(tmp_path / "c.db")))
    monkeypatch.setattr(settings, "lustre_enabled", True)
    # Si /probe no se saltara las cuotas, esto lo rechazaría con 413.
    monkeypatch.setattr(main, "MAX_SIZE_MB_PER_QUERY", 1)
    return semanal, diario


def _dia(diario, jjj, horas, bandas=("10",)):
    d = diario / "l1b" / "abi" / "conus" / jjj[:4] / jjj[4:]
    d.mkdir(parents=True)
    for b in bandas:
        for h in horas:
            (d / _nc(b, jjj, h)).write_bytes(b"x")


def _tgz(semanal, jjj, hhmm):
    doy = int(jjj[4:])
    d = semanal / "abi" / "l1b" / "conus" / jjj[:4] / f"{(doy - 1) // 7 + 1:02d}"
    d.mkdir(parents=True, exist_ok=True)
    (d / f"ABI-L1b-RadC-M6_G19-s{jjj}{hhmm}.tgz").write_bytes(b"x")


def _solicitud(fechas, bandas=("10",)):
    return {"sat": "GOES-EAST", "sensor": "abi", "nivel": "L1b", "dominio": "conus",
            "bandas": list(bandas), "fechas": fechas}


def test_probe_cuenta_por_dia_y_arbol_sin_copiar_nada(depot):
    semanal, diario = depot
    _dia(diario, "2026291", ["0001", "0006", "0011"])        # 18-oct, árbol diario
    (diario / "l1b/abi/conus/2026/200").mkdir(parents=True)   # 19-jul, carpeta vacía
    _tgz(semanal, "2026050", "0001")                          # 19-feb, sin migrar

    r = client.post("/probe", json=_solicitud({
        "20261018": ["00:00-00:10"],
        "20260719": ["00:00-00:10"],
        "20260219": ["00:00-00:10"],
    }))

    assert r.status_code == 200, r.text
    cuerpo = r.json()
    assert cuerpo["lustre"]["estado"] == "ok"
    assert cuerpo["por_dia"] == {
        "20260219": {"arbol": "semanal", "nc": 0, "tgz": 1},
        "20260719": {"arbol": "diario", "nc": 0, "tgz": 0},
        "20261018": {"arbol": "diario", "nc": 2, "tgz": 0},
    }
    assert cuerpo["nc"] == 2 and cuerpo["tgz"] == 1
    assert cuerpo["dias_sin_datos"] == ["20260719"]


def test_probe_no_crea_consulta(depot):
    _, diario = depot
    _dia(diario, "2026291", ["0001"])
    antes = len(main.db.listar_consultas(limite=1000))

    assert client.post("/probe", json=_solicitud({"20261018": ["00:00-00:10"]})).status_code == 200

    assert len(main.db.listar_consultas(limite=1000)) == antes


def test_probe_sin_raices_dice_no_disponible_y_no_recorre(depot, monkeypatch):
    monkeypatch.setattr(main, "SOURCE_DATA_PATH", "/no/existe/1")
    monkeypatch.setattr(main, "SOURCE_PATH_DIARIO", "/no/existe/2")

    cuerpo = client.post("/probe", json=_solicitud({"20261018": ["00:00-00:10"]})).json()

    assert cuerpo["lustre"]["estado"] == "no_disponible"
    assert "por_dia" not in cuerpo


def test_probe_con_lustre_deshabilitado(depot, monkeypatch):
    monkeypatch.setattr(settings, "lustre_enabled", False)

    cuerpo = client.post("/probe", json=_solicitud({"20261018": ["00:00-00:10"]})).json()

    assert cuerpo["lustre"]["estado"] == "deshabilitado"


def test_probe_rechaza_rangos_mas_largos_que_su_limite(depot, monkeypatch):
    monkeypatch.setattr(main, "PROBE_MAX_DIAS", 5)

    r = client.post("/probe", json=_solicitud({"20261001-20261010": ["00:00-00:10"]}))

    assert r.status_code == 413
    assert "10 días" in r.json()["detail"]


def test_probe_valida_la_forma_como_query(depot):
    r = client.post("/probe", json=_solicitud({"20261018": ["00:00-00:10"]}, bandas=("99",)))
    assert 400 <= r.status_code < 500
