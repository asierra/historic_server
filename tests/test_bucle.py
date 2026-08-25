"""Pruebas del consumidor de la cola (`cola.BucleDeCola`).

Casi todas conducen el bucle a mano con `una_vuelta()` en vez de arrancar el
hilo: lo que se quiere fijar es la lógica —a quién reclama, qué hace cuando el
pipeline falla, cuándo barre— y hacerlo con hilos y esperas convierte cada
aserción en una carrera. El hilo tiene sus propias pruebas al final, que son
las únicas que esperan de verdad.
"""

import threading
import time
from datetime import datetime, timedelta

import pytest

from cola import BucleDeCola, barrido_efectivo
from database import ConsultasDatabase


QUERY = {"satelite": "GOES16", "sensor": "abi"}


@pytest.fixture
def db(tmp_path):
    return ConsultasDatabase(str(tmp_path / "cola.db"))


class RecoverFalso:
    """Doble del pipeline. Registra llamadas y hace lo que se le diga."""

    def __init__(self, efecto=None):
        self.llamadas = []
        self.efecto = efecto  # callable(db, consulta_id) o None

    def procesar_consulta(self, consulta_id, query_dict):
        self.llamadas.append((consulta_id, query_dict))
        if self.efecto:
            self.efecto(consulta_id)


def _bucle(db, recover, **kwargs):
    kwargs.setdefault("worker_id", "prueba")
    kwargs.setdefault("poll_s", 0.01)
    kwargs.setdefault("barrido_s", 0.0)  # barrer en cada vuelta salvo que la prueba diga otra cosa
    return BucleDeCola(db=db, recover=recover, **kwargs)


def _fila(db, consulta_id):
    import sqlite3
    with db._connect() as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            "SELECT * FROM consultas WHERE id = ?", (consulta_id,)
        ).fetchone()


# --------------------------------------------------------------------------
# Una vuelta
# --------------------------------------------------------------------------

def test_con_la_cola_vacia_no_hace_nada(db):
    recover = RecoverFalso()
    assert _bucle(db, recover).una_vuelta() is False
    assert recover.llamadas == []


def test_reclama_y_pasa_la_query_al_pipeline(db):
    """El bucle no interpreta la consulta: se la da al pipeline tal cual."""
    db.crear_consulta("PRIMERA1", QUERY)
    recover = RecoverFalso(efecto=lambda cid: db.guardar_resultados(cid, {"ok": True}))

    assert _bucle(db, recover).una_vuelta() is True

    assert recover.llamadas == [("PRIMERA1", QUERY)]
    assert _fila(db, "PRIMERA1")["estado"] == "completado"


def test_una_consulta_aplazada_no_se_toca(db):
    """Lo que espera su reintento no se reclama aunque esté en 'recibido'."""
    db.crear_consulta("ESPERAND", QUERY)
    db.fallar_con_reintento("ESPERAND", "falló", backoff_s=3600, max_intentos=9)

    recover = RecoverFalso()
    assert _bucle(db, recover).una_vuelta() is False
    assert recover.llamadas == []


# --------------------------------------------------------------------------
# Fallos
# --------------------------------------------------------------------------

def test_el_error_del_pipeline_se_traduce_a_reintento(db):
    """El pipeline atrapa sus excepciones y deja 'error'; el bucle lo reinterpreta.

    Sin esta traducción no habría reintentos nunca: `recover.procesar_consulta`
    no lanza, así que el bucle no se enteraría de que algo salió mal y la
    consulta se quedaría en 'error' a la primera. Es justo lo que la cola viene
    a arreglar.
    """
    db.crear_consulta("FALLONA1", QUERY)
    recover = RecoverFalso(
        efecto=lambda cid: db.actualizar_estado(cid, "error", 0, "Error: S3 no responde")
    )

    _bucle(db, recover, backoff_s=300, max_intentos=3).una_vuelta()

    fila = _fila(db, "FALLONA1")
    assert fila["estado"] == "recibido", "debería reintentarse, no rendirse a la primera"
    assert fila["disponible_desde"] is not None
    # Y conserva el mensaje que escribió el pipeline, no uno genérico
    assert "S3 no responde" in fila["mensaje"]


def test_al_agotar_intentos_se_queda_en_error(db):
    db.crear_consulta("TERCA001", QUERY)
    recover = RecoverFalso(
        efecto=lambda cid: db.actualizar_estado(cid, "error", 0, "reventó")
    )
    bucle = _bucle(db, recover, backoff_s=0, max_intentos=3)

    for _ in range(3):
        bucle.una_vuelta()

    fila = _fila(db, "TERCA001")
    assert fila["estado"] == "error"
    assert fila["intentos"] == 3
    assert bucle.una_vuelta() is False, "no debería volver a reclamarse"


def test_una_excepcion_del_pipeline_no_pierde_la_consulta(db):
    """Si el pipeline lanza en vez de atrapar, la consulta sigue teniendo destino."""
    db.crear_consulta("EXPLOTA1", QUERY)

    def revienta(cid):
        raise RuntimeError("pebble se atragantó")

    recover = RecoverFalso(efecto=revienta)
    _bucle(db, recover, backoff_s=60, max_intentos=3).una_vuelta()

    fila = _fila(db, "EXPLOTA1")
    assert fila["estado"] == "recibido"
    assert "pebble se atragantó" in fila["mensaje"]
    assert fila["lease_hasta"] is None, "un fallo tiene que soltar el lease"


def test_un_fallo_al_procesar_no_deja_la_consulta_en_curso_colgada(db):
    """`consulta_en_curso` se limpia pase lo que pase; lo lee /health."""
    db.crear_consulta("EXPLOTA2", QUERY)

    def revienta(cid):
        raise RuntimeError("x")

    bucle = _bucle(db, RecoverFalso(efecto=revienta))
    bucle.una_vuelta()

    assert bucle.consulta_en_curso is None


# --------------------------------------------------------------------------
# Barrido
# --------------------------------------------------------------------------

def test_el_barrido_recupera_lo_que_perdio_a_su_dueño(db):
    """Una consulta reclamada por un proceso que murió vuelve sola a la cola."""
    db.crear_consulta("HUERFANA", QUERY)
    db.reclamar_siguiente("el-que-murió", lease_s=1,
                          ahora=datetime.now() - timedelta(seconds=60))

    recover = RecoverFalso(efecto=lambda cid: db.guardar_resultados(cid, {}))
    assert _bucle(db, recover).una_vuelta() is True

    assert recover.llamadas[0][0] == "HUERFANA"


def test_el_barrido_respeta_su_intervalo(db):
    """No se barre en cada vuelta: es una escritura y no hace falta tan seguido."""
    llamadas = []
    bucle = _bucle(db, RecoverFalso(), barrido_s=3600)
    bucle.db = type("Espia", (), {
        "liberar_expiradas": lambda self, **kw: llamadas.append(1),
        "hay_trabajo": lambda self: False,
    })()

    for _ in range(5):
        bucle.una_vuelta()

    assert len(llamadas) == 1, "debería barrer una vez, no en cada vuelta"


# --------------------------------------------------------------------------
# Intervalo de barrido
# --------------------------------------------------------------------------

def test_el_barrido_nunca_es_mas_lento_que_medio_lease(db):
    """Regresión: bajar el lease tiene que acelerar la recuperación de verdad.

    Con un intervalo fijo de 60 s, configurar un lease de 20 s no servía de
    nada —el barrido lo limitaba— y el efecto era invisible: parecía que el
    lease no funcionaba. Se vio matando el servidor a media consulta.
    """
    assert barrido_efectivo(900) == 60.0, "con el lease normal, el tope manda"
    assert barrido_efectivo(20) == 10.0
    assert barrido_efectivo(10) == 5.0

    # Y el bucle lo aplica solo, sin que haya que pasárselo
    assert _bucle(db, RecoverFalso(), barrido_s=None, lease_s=20).barrido_s == 10.0


# --------------------------------------------------------------------------
# El hilo
# --------------------------------------------------------------------------

def test_arrancar_y_parar(db):
    bucle = _bucle(db, RecoverFalso())
    assert bucle.vivo is False

    bucle.arrancar()
    assert bucle.vivo is True

    bucle.parar(timeout_s=5)
    assert bucle.vivo is False


def test_el_hilo_drena_la_cola(db):
    """La prueba de punta a punta: se encola y aparece 'completado' sin empujar."""
    recover = RecoverFalso(efecto=lambda cid: db.guardar_resultados(cid, {"ok": True}))
    bucle = _bucle(db, recover)
    bucle.arrancar()
    try:
        for i in range(3):
            db.crear_consulta(f"DRENA{i:03d}", QUERY)

        limite = time.time() + 10
        while time.time() < limite and len(recover.llamadas) < 3:
            time.sleep(0.05)
    finally:
        bucle.parar(timeout_s=5)

    assert sorted(c for c, _ in recover.llamadas) == ["DRENA000", "DRENA001", "DRENA002"]


def test_parar_no_reencola_lo_que_esta_en_curso(db):
    """Se deja vencer el lease en vez de soltarlo.

    Soltarlo pondría la consulta a disposición de otro proceso mientras éste
    sigue escribiendo en el mismo directorio, que es el duplicado que se quiere
    evitar. Cuesta hasta un lease de espera y a cambio no hay dos escritores.
    """
    db.crear_consulta("ENCURSO1", QUERY)
    procesando = threading.Event()
    seguir = threading.Event()

    def lento(cid):
        procesando.set()
        seguir.wait(timeout=10)

    bucle = _bucle(db, RecoverFalso(efecto=lento))
    bucle.arrancar()
    try:
        assert procesando.wait(timeout=5), "el bucle no llegó a reclamar"
        bucle.parar(timeout_s=0.2)  # no espera a que termine

        fila = _fila(db, "ENCURSO1")
        assert fila["estado"] == "procesando"
        assert fila["lease_hasta"] is not None
        assert fila["worker_id"] == "prueba"
    finally:
        seguir.set()


def test_un_fallo_del_bucle_no_mata_el_hilo(db):
    """Si la base falla en una vuelta, el hilo aguanta y lo vuelve a intentar.

    Un hilo muerto deja la cola sin drenar y no se ve desde fuera salvo por
    /health, así que conviene que sólo muera cuando se le pide.
    """
    fallos = []

    class DbQueRevienta:
        def liberar_expiradas(self, **kw):
            pass

        def hay_trabajo(self):
            fallos.append(1)
            raise RuntimeError("base bloqueada")

    bucle = _bucle(db, RecoverFalso(), poll_s=0.01)
    bucle.db = DbQueRevienta()
    bucle.arrancar()
    try:
        limite = time.time() + 5
        while time.time() < limite and len(fallos) < 3:
            time.sleep(0.01)
    finally:
        bucle.parar(timeout_s=2)

    assert len(fallos) >= 3, "el hilo se murió al primer fallo"
