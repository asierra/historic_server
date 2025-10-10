import os
import tarfile
from pathlib import Path
import time
import pytest
from fastapi.testclient import TestClient
import main
from recover import RecoverFiles
from database import ConsultasDatabase

TEST_DB_PATH = "test_consultas_recover.db"

@pytest.fixture(autouse=True)
def override_db_and_recover(monkeypatch, tmp_path):
    # DB temporal
    test_db = ConsultasDatabase(db_path=TEST_DB_PATH)
    monkeypatch.setattr(main, "db", test_db)

    # Directorios temporales
    source_dir = tmp_path / "lustre_src"
    dest_dir = tmp_path / "downloads"
    source_dir.mkdir(parents=True, exist_ok=True)
    dest_dir.mkdir(parents=True, exist_ok=True)

    # Recover real con rutas temporales
    real_recover = RecoverFiles(
        db=test_db,
        source_data_path=str(source_dir),
        base_download_path=str(dest_dir),
        executor=main.executor,
        s3_fallback_enabled=True,
        lustre_enabled=True,
    )
    monkeypatch.setattr(main, "recover", real_recover)

    try:
        yield {
            "db": test_db,
            "source_dir": source_dir,
            "dest_dir": dest_dir,
            "recover": real_recover,
        }
    finally:
        if os.path.exists(TEST_DB_PATH):
            os.remove(TEST_DB_PATH)

client = TestClient(main.app)


def _wait_until_completed(consulta_id: str, timeout_s: int = 20):
    start = time.time()
    while time.time() - start < timeout_s:
        data = client.get(f"/query/{consulta_id}").json()
        if data.get("estado") == "completado":
            return True
        time.sleep(1)
    return False


def _create_dummy_tgz(path: Path, inner_names):
    with tarfile.open(path, "w:gz") as tar:
        for name in inner_names:
            p = Path(name)
            # create a temporary file in memory using TarInfo
            info = tarfile.TarInfo(name=name)
            info.size = 0
            tar.addfile(info)


def test_recover_lustre_copies_tgz_for_all_cases(monkeypatch, override_db_and_recover):
    """
    Verifica que el recover real copie .tgz sin expandir desde Lustre cuando:
    - L1b + bandas=ALL
    - L2 + productos=ALL + bandas=ALL
    """
    src = override_db_and_recover["source_dir"]

    # Crear un .tgz con nombre compatible para 2023-10-26 12:00 (YYYY=2023, JJJ=299, HHMM=1200)
    anio = 2023
    dia_jjj = 299
    semana = (dia_jjj - 1) // 7 + 1
    base = src / "abi" / "l1b" / "fd" / f"{anio}" / f"{semana:02d}"
    base.mkdir(parents=True, exist_ok=True)

    tgz_name_l1b = f"ABI-L1B-RadF-M6_GEAST-s{anio}{dia_jjj:03d}1200.tgz"
    tgz_path_l1b = base / tgz_name_l1b
    _create_dummy_tgz(tgz_path_l1b, [
        f"OR_ABI-L1B-RadF-M6C01_GEAST_s{anio}{dia_jjj:03d}1200_e..._c....nc"
    ])

    # L1b + ALL
    monkeypatch.setattr("main.generar_id_consulta", lambda: "TEST_RECOV_LUSTRE_L1B_ALL")
    req_l1b = {
        "nivel": "L1b",
        "dominio": "fd",
        "bandas": "ALL",
        "fechas": {"20231026": ["12:00"]}
    }
    r = client.post("/query", json=req_l1b)
    assert r.status_code == 200
    assert _wait_until_completed("TEST_RECOV_LUSTRE_L1B_ALL")
    res = client.get("/query/TEST_RECOV_LUSTRE_L1B_ALL?resultados=true").json()["resultados"]
    lustre_files = res["fuentes"]["lustre"]["archivos"]
    assert any(f.endswith(".tgz") for f in lustre_files), f"Lustre no devolvió .tgz: {lustre_files}"

    # Preparar también un tgz L2 en la misma hora/fecha para que lo encuentre al pedir L2
    base_l2 = src / "abi" / "l2" / "fd" / f"{anio}" / f"{semana:02d}"
    base_l2.mkdir(parents=True, exist_ok=True)
    tgz_name_l2 = f"ABI-L2F-M6_GEAST-s{anio}{dia_jjj:03d}1200.tgz"
    tgz_path_l2 = base_l2 / tgz_name_l2
    _create_dummy_tgz(tgz_path_l2, [
        f"CG_ABI-L2-CMIPF-M6C01_GEAST_s{anio}{dia_jjj:03d}1200_e..._c....nc",
        f"OR_ABI-L2-ACHAF-M6_GEAST_s{anio}{dia_jjj:03d}1200_e..._c....nc"
    ])

    # L2 + ALL/ALL -> debe copiar tgz
    monkeypatch.setattr("main.generar_id_consulta", lambda: "TEST_RECOV_LUSTRE_L2_ALL_ALL")
    req_l2 = {
        "nivel": "L2",
        "productos": "ALL",
        "bandas": "ALL",
        "dominio": "fd",
        "fechas": {"20231026": ["12:00"]}
    }
    r = client.post("/query", json=req_l2)
    assert r.status_code == 200
    assert _wait_until_completed("TEST_RECOV_LUSTRE_L2_ALL_ALL")
    res = client.get("/query/TEST_RECOV_LUSTRE_L2_ALL_ALL?resultados=true").json()["resultados"]
    lustre_files = res["fuentes"]["lustre"]["archivos"]
    assert any(f.endswith(".tgz") for f in lustre_files), f"Lustre no devolvió .tgz: {lustre_files}"


def test_recover_s3_never_returns_tgz(monkeypatch, override_db_and_recover):
    """
    Verifica que el recover real NUNCA genere .tgz desde S3,
    incluso cuando L1b ALL o L2 ALL/ALL.
    Mockeamos S3 para evitar red externa.
    """
    # Mock de discover_files para devolver rutas S3 simuladas (.nc)
    def fake_discover_files(query, goes19_date):
        # Simular hallazgo de un par de archivos .nc
        return {
            f"OR_ABI-L2-ACHAF-M6_GEAST_s20230011200_e..._c....nc": "s3://noaa-goes16/ABI-L2-ACHAF/2023/001/12/OR_ABI-L2-ACHAF-M6_GEAST_s20230011200_e..._c....nc",
            f"CG_ABI-L2-CMIPF-M6C13_GEAST_s20230011200_e..._c....nc": "s3://noaa-goes16/ABI-L2-CMIPF/2023/001/12/CG_ABI-L2-CMIPF-M6C13_GEAST_s20230011200_e..._c....nc"
        }

    # Mock de download_files para crear archivos .nc en destino
    def fake_download_files(consulta_id, objetivos, dest, db):
        paths = []
        for s3_path in objetivos:
            name = Path(s3_path).name
            local = Path(dest) / name
            local.write_bytes(b"")
            paths.append(local)
        return paths, []

    # Aplicar mocks a la instancia real de Recover
    main.recover.s3.discover_files = fake_discover_files
    main.recover.s3.download_files = fake_download_files

    # Forzar que no haya archivos en lustre (no creamos ninguno)

    # L1b + ALL
    monkeypatch.setattr("main.generar_id_consulta", lambda: "TEST_RECOV_S3_L1B_ALL")
    req_l1b = {
        "nivel": "L1b",
        "dominio": "fd",
        "bandas": "ALL",
        "fechas": {"20230101": ["12:00"]}
    }
    r = client.post("/query", json=req_l1b)
    assert r.status_code == 200
    assert _wait_until_completed("TEST_RECOV_S3_L1B_ALL")
    res = client.get("/query/TEST_RECOV_S3_L1B_ALL?resultados=true").json()["resultados"]
    s3_files = res["fuentes"]["s3"]["archivos"]
    assert s3_files, "S3 no devolvió archivos"
    assert all(f.endswith(".nc") for f in s3_files), f"S3 devolvió no .nc: {s3_files}"
    assert all(not f.endswith(".tgz") for f in s3_files)

    # L2 + ALL/ALL
    monkeypatch.setattr("main.generar_id_consulta", lambda: "TEST_RECOV_S3_L2_ALL_ALL")
    req_l2 = {
        "nivel": "L2",
        "productos": "ALL",
        "bandas": "ALL",
        "dominio": "fd",
        "fechas": {"20230101": ["12:00"]}
    }
    r = client.post("/query", json=req_l2)
    assert r.status_code == 200
    assert _wait_until_completed("TEST_RECOV_S3_L2_ALL_ALL")
    res = client.get("/query/TEST_RECOV_S3_L2_ALL_ALL?resultados=true").json()["resultados"]
    s3_files = res["fuentes"]["s3"]["archivos"]
    assert s3_files, "S3 no devolvió archivos"
    assert all(f.endswith(".nc") for f in s3_files), f"S3 devolvió no .nc: {s3_files}"
    assert all(not f.endswith(".tgz") for f in s3_files)
