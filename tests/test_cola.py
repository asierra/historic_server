"""Pruebas de las primitivas de cola de `ConsultasDatabase`.

Fijan los invariantes de los que dependerá el worker: que un reclamo sea
exclusivo, que el orden sea FIFO, que un lease vencido devuelva el trabajo a la
cola y que una consulta que falla siempre acabe parando.

Ninguna espera de verdad. Todos los métodos aceptan `ahora`, así que el tiempo
se simula moviendo esa marca; una suite que duerme para probar timeouts es una
suite que acaba fallando en CI por motivos que no son el código.
"""

import json
import sqlite3
from datetime import datetime, timedelta

import pytest

from database import ConsultasDatabase


T0 = datetime(2026, 8, 25, 12, 0, 0)
LEASE = 900


@pytest.fixture
def db(tmp_path):
    """Base vacía y propia de cada prueba."""
    return ConsultasDatabase(str(tmp_path / "cola.db"))


def _encolar(db, consulta_id, creada=T0, **campos):
    """Crea una consulta y le fija timestamps/campos de cola de forma explícita.

    `crear_consulta` sella timestamp_creacion con datetime.now(), y dos filas
    creadas en la misma prueba pueden caer en el mismo microsegundo: el orden
    FIFO dejaría de ser comprobable. Aquí se fija a mano.
    """
    assert db.crear_consulta(consulta_id, {"satelite": "GOES16", "id": consulta_id})
    campos.setdefault("timestamp_creacion", creada.isoformat())
    campos.setdefault("timestamp_actualizacion", creada.isoformat())
    asignaciones = ", ".join(f"{k} = ?" for k in campos)
    with sqlite3.connect(db.db_path) as conn:
        conn.execute(
            f"UPDATE consultas SET {asignaciones} WHERE id = ?",
            (*campos.values(), consulta_id),
        )
        conn.commit()


def _fila(db, consulta_id):
    """Lee la fila cruda: _row_to_dict no expone las columnas de la cola a propósito."""
    with sqlite3.connect(db.db_path) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            "SELECT * FROM consultas WHERE id = ?", (consulta_id,)
        ).fetchone()


# --------------------------------------------------------------------------
# Reclamo
# --------------------------------------------------------------------------

def test_el_reclamo_es_exclusivo(db):
    """Dos reclamos sobre una única fila disponible: sólo uno se la lleva.

    Es el invariante que sustituye al cerrojo de reclamar_para_reproceso(), y
    el que impide que dos workers descarguen los mismos archivos en paralelo.
    """
    _encolar(db, "SOLOUNA1")

    primero = db.reclamar_siguiente("w1", LEASE, ahora=T0)
    segundo = db.reclamar_siguiente("w2", LEASE, ahora=T0)

    assert primero is not None
    assert primero[0] == "SOLOUNA1"
    assert segundo is None
    assert _fila(db, "SOLOUNA1")["worker_id"] == "w1"


def test_el_reclamo_devuelve_la_query_deserializada(db):
    """El worker recibe la query lista para pasársela al pipeline, sin JSON de por medio."""
    _encolar(db, "CONQUERY")

    consulta_id, query = db.reclamar_siguiente("w1", LEASE, ahora=T0)

    assert consulta_id == "CONQUERY"
    assert query == {"satelite": "GOES16", "id": "CONQUERY"}


def test_el_reclamo_marca_estado_lease_y_dueño(db):
    _encolar(db, "MARCADA1")

    db.reclamar_siguiente("tahan:4242", LEASE, ahora=T0)

    fila = _fila(db, "MARCADA1")
    assert fila["estado"] == "procesando"
    assert fila["worker_id"] == "tahan:4242"
    assert fila["lease_hasta"] == (T0 + timedelta(seconds=LEASE)).isoformat()
    assert fila["intentos"] == 1


def test_la_cola_es_fifo(db):
    """Se atiende por antigüedad de creación, no por orden de inserción."""
    _encolar(db, "TERCERA1", creada=T0 + timedelta(minutes=20))
    _encolar(db, "PRIMERA1", creada=T0)
    _encolar(db, "SEGUNDA1", creada=T0 + timedelta(minutes=10))

    ahora = T0 + timedelta(hours=1)
    orden = []
    while (reclamada := db.reclamar_siguiente("w1", LEASE, ahora=ahora)) is not None:
        orden.append(reclamada[0])

    assert orden == ["PRIMERA1", "SEGUNDA1", "TERCERA1"]


def test_la_cola_vacia_devuelve_none(db):
    assert db.reclamar_siguiente("w1", LEASE, ahora=T0) is None


def test_no_se_reclama_lo_que_no_esta_en_recibido(db):
    """Sólo 'recibido' significa «encolada y disponible»."""
    for consulta_id, estado in [("ENCURSO1", "procesando"),
                                ("LISTA001", "completado"),
                                ("FALLIDA1", "error")]:
        _encolar(db, consulta_id, estado=estado)

    assert db.reclamar_siguiente("w1", LEASE, ahora=T0) is None


# --------------------------------------------------------------------------
# Backoff: disponible_desde
# --------------------------------------------------------------------------

def test_disponible_desde_en_el_futuro_aplaza_el_reclamo(db):
    """Una consulta en espera de reintento sigue en 'recibido' pero no se coge."""
    _encolar(db, "ESPERAND", disponible_desde=(T0 + timedelta(minutes=5)).isoformat())

    assert db.reclamar_siguiente("w1", LEASE, ahora=T0) is None
    assert _fila(db, "ESPERAND")["estado"] == "recibido"

    reclamada = db.reclamar_siguiente("w1", LEASE, ahora=T0 + timedelta(minutes=5))
    assert reclamada is not None and reclamada[0] == "ESPERAND"


def test_una_aplazada_no_bloquea_a_las_demas(db):
    """La aplazada es más antigua, pero no debe tapar a la que sí está lista."""
    _encolar(db, "APLAZADA", creada=T0,
             disponible_desde=(T0 + timedelta(hours=1)).isoformat())
    _encolar(db, "DISPONIB", creada=T0 + timedelta(minutes=1))

    reclamada = db.reclamar_siguiente("w1", LEASE, ahora=T0 + timedelta(minutes=2))

    assert reclamada is not None and reclamada[0] == "DISPONIB"


# --------------------------------------------------------------------------
# Leases
# --------------------------------------------------------------------------

def test_un_lease_vencido_vuelve_a_la_cola(db):
    """El rescate deja de ser un caso especial del arranque: lo hace cualquier vuelta."""
    _encolar(db, "HUERFANA")
    db.reclamar_siguiente("w1", lease_s=60, ahora=T0)

    liberadas = db.liberar_expiradas(ahora=T0 + timedelta(seconds=61))

    assert liberadas == 1
    fila = _fila(db, "HUERFANA")
    assert fila["estado"] == "recibido"
    assert fila["worker_id"] is None
    assert fila["lease_hasta"] is None


def test_un_lease_vivo_no_se_toca(db):
    """Quitarle el trabajo a alguien que sigue vivo es peor que esperar de más."""
    _encolar(db, "TRABAJAN")
    db.reclamar_siguiente("w1", lease_s=900, ahora=T0)

    assert db.liberar_expiradas(ahora=T0 + timedelta(seconds=899)) == 0
    assert _fila(db, "TRABAJAN")["estado"] == "procesando"


def test_actualizar_estado_empuja_el_lease(db):
    """El pipeline ya late en cada avance; no hace falta instrumentar nada más."""
    _encolar(db, "AVANZAND")
    db.reclamar_siguiente("w1", lease_s=900, ahora=T0)

    mas_tarde = T0 + timedelta(seconds=800)
    db.actualizar_estado("AVANZAND", "procesando", progreso=42,
                         lease_s=900, ahora=mas_tarde)

    fila = _fila(db, "AVANZAND")
    assert fila["progreso"] == 42
    assert fila["lease_hasta"] == (mas_tarde + timedelta(seconds=900)).isoformat()
    # Y por eso, en el instante en que habría vencido el lease original, sigue viva
    assert db.liberar_expiradas(ahora=T0 + timedelta(seconds=901)) == 0


def test_el_pipeline_renueva_con_el_lease_configurado(db, tmp_path):
    """`actualizar_estado` usa el lease de la base, no un valor por defecto suyo.

    Regresión. El pipeline llama a `actualizar_estado` sin pasar `lease_s`
    —no conoce la configuración de la cola ni debe conocerla—, así que si el
    valor por defecto fuera fijo, el primer avance pisaría el lease configurado
    con 900 s y bajar QUEUE_LEASE_S no serviría de nada. Se vio matando el
    servidor a media consulta: con lease de 20 s no se recuperaba nunca.
    """
    corta = ConsultasDatabase(str(tmp_path / "corta.db"), lease_s=30)
    assert corta.crear_consulta("CONFIGUR", {"x": 1})
    corta.reclamar_siguiente("w1", ahora=T0)

    corta.actualizar_estado("CONFIGUR", "procesando", progreso=50, ahora=T0)

    fila = _fila(corta, "CONFIGUR")
    assert fila["lease_hasta"] == (T0 + timedelta(seconds=30)).isoformat()


def test_un_estado_terminal_no_renueva_el_lease(db):
    """Sólo 'procesando' retiene: en 'completado' el lease no significa nada."""
    _encolar(db, "TERMINAD")
    db.reclamar_siguiente("w1", lease_s=900, ahora=T0)
    lease_original = _fila(db, "TERMINAD")["lease_hasta"]

    db.actualizar_estado("TERMINAD", "completado", ahora=T0 + timedelta(seconds=500))

    assert _fila(db, "TERMINAD")["lease_hasta"] == lease_original


def test_una_huerfana_del_mundo_anterior_se_rescata(db):
    """Fila en 'procesando' sin lease: un BackgroundTask que murió con su proceso.

    Es literalmente el caso de GkpH6xne, 29 h al 89 %. Sin esto seguiría
    congelada para siempre después de migrar.
    """
    _encolar(db, "GkpH6xne", estado="procesando", progreso=89,
             timestamp_actualizacion=(T0 - timedelta(hours=29)).isoformat())

    assert db.liberar_expiradas(ahora=T0, latido_maximo_s=900) == 1
    assert _fila(db, "GkpH6xne")["estado"] == "recibido"


def test_un_backgroundtask_todavia_vivo_no_se_le_roba(db):
    """Misma forma que la anterior —sin lease— pero con el latido reciente.

    Durante el despliegue conviven el pipeline viejo y la cola; robarle una
    consulta al viejo significa descargar los mismos archivos dos veces.
    """
    _encolar(db, "VIVAAUN1", estado="procesando",
             timestamp_actualizacion=(T0 - timedelta(seconds=60)).isoformat())

    assert db.liberar_expiradas(ahora=T0, latido_maximo_s=900) == 0
    assert _fila(db, "VIVAAUN1")["estado"] == "procesando"


# --------------------------------------------------------------------------
# Reintentos
# --------------------------------------------------------------------------

def test_los_intentos_se_acumulan_entre_reclamos(db):
    """Cada reclamo gasta intento, aunque el worker muera sin poder registrar nada."""
    _encolar(db, "TERCA001")

    for vuelta in range(1, 4):
        ahora = T0 + timedelta(hours=vuelta)
        assert db.reclamar_siguiente("w1", lease_s=60, ahora=ahora) is not None
        assert _fila(db, "TERCA001")["intentos"] == vuelta
        db.liberar_expiradas(ahora=ahora + timedelta(seconds=61))


def test_al_agotar_los_intentos_queda_en_error_y_no_se_reclama_mas(db):
    """El tope es lo que impide que una consulta venenosa gire indefinidamente."""
    _encolar(db, "VENENOSA")

    for _ in range(3):
        db.reclamar_siguiente("w1", lease_s=60, ahora=T0)
        estado = db.fallar_con_reintento("VENENOSA", "explotó", backoff_s=0,
                                         max_intentos=3, ahora=T0)

    assert estado == "error"
    fila = _fila(db, "VENENOSA")
    assert fila["estado"] == "error"
    assert fila["mensaje"] == "explotó"
    assert fila["intentos"] == 3
    assert db.reclamar_siguiente("w2", LEASE, ahora=T0 + timedelta(days=1)) is None


def test_un_fallo_con_presupuesto_reencola_con_espera(db):
    """Aplazar va en disponible_desde, no en el estado: Django sigue viendo 'recibido'."""
    _encolar(db, "REINTENT")
    db.reclamar_siguiente("w1", LEASE, ahora=T0)

    estado = db.fallar_con_reintento("REINTENT", "S3 no responde", backoff_s=300,
                                     max_intentos=3, ahora=T0)

    fila = _fila(db, "REINTENT")
    assert estado == "recibido"
    assert fila["estado"] == "recibido"
    assert fila["worker_id"] is None and fila["lease_hasta"] is None
    assert fila["disponible_desde"] == (T0 + timedelta(seconds=300)).isoformat()
    assert db.reclamar_siguiente("w1", LEASE, ahora=T0 + timedelta(seconds=299)) is None


def test_el_backoff_admite_una_escala_por_intento(db):
    """(60, 300, 900) es el 1/5/15 min de la propuesta, sin maquinaria aparte."""
    escala = (60, 300, 900)
    _encolar(db, "ESCALA01")

    esperas = []
    ahora = T0
    for _ in range(3):
        # El reloj avanza hasta el instante en que la consulta vuelve a estar
        # disponible; si no, el propio backoff impide el siguiente reclamo y el
        # contador de intentos se queda clavado en 1.
        assert db.reclamar_siguiente("w1", LEASE, ahora=ahora) is not None
        db.fallar_con_reintento("ESCALA01", "falla", backoff_s=escala,
                                max_intentos=99, ahora=ahora)
        disponible = datetime.fromisoformat(_fila(db, "ESCALA01")["disponible_desde"])
        esperas.append(int((disponible - ahora).total_seconds()))
        ahora = disponible

    assert esperas == [60, 300, 900]
    # Y a partir del cuarto se repite el último escalón en vez de desbordar
    db.reclamar_siguiente("w1", LEASE, ahora=ahora)
    db.fallar_con_reintento("ESCALA01", "falla", backoff_s=escala,
                            max_intentos=99, ahora=ahora)
    disponible = datetime.fromisoformat(_fila(db, "ESCALA01")["disponible_desde"])
    assert int((disponible - ahora).total_seconds()) == 900


def test_fallar_una_consulta_inexistente_no_revienta(db):
    assert db.fallar_con_reintento("NOEXISTE", "x", backoff_s=60,
                                   max_intentos=3, ahora=T0) is None


# --------------------------------------------------------------------------
# Reencolar (lo que usará /restart)
# --------------------------------------------------------------------------

def test_reencolar_devuelve_a_la_cola_desde_cualquier_estado(db):
    """El botón de reiniciar del panel funciona igual sobre error que sobre completado."""
    for consulta_id, estado in [("DESDEERR", "error"), ("DESDEOK1", "completado")]:
        # Se parte de una fila sucia a propósito: con dueño, con lease vencido
        # y con un reintento aplazado. Si sólo se comprobara sobre campos ya
        # vacíos, la prueba pasaría aunque reencolar() no limpiara nada.
        # El lease va vencido y no vivo porque un lease vivo lo rechaza el
        # cerrojo — eso lo cubre test_reencolar_se_niega_si_alguien_la_tiene_viva.
        _encolar(db, consulta_id, estado=estado, progreso=100, intentos=3,
                 worker_id="tahan:4242",
                 lease_hasta=(T0 - timedelta(hours=1)).isoformat(),
                 disponible_desde=(T0 + timedelta(days=1)).isoformat())

        assert db.reencolar(consulta_id, ahora=T0) is True

        fila = _fila(db, consulta_id)
        assert fila["estado"] == "recibido"
        assert fila["progreso"] == 0
        # Presupuesto entero: si alguien pide el reinicio a mano, no hereda lo gastado
        assert fila["intentos"] == 0
        # Y queda disponible ya, no dentro de un día: el reinicio es una orden
        # explícita de una persona, no un reintento automático.
        assert fila["worker_id"] is None
        assert fila["lease_hasta"] is None
        assert fila["disponible_desde"] is None
        assert db.reclamar_siguiente("w1", LEASE, ahora=T0)[0] == consulta_id


def test_reencolar_se_niega_si_alguien_la_tiene_viva(db):
    """No se le quita el trabajo a un consumidor que está avanzando.

    Reencolar una consulta con el lease vivo la pondría a disposición de otro,
    y acabarían los dos escribiendo en el mismo directorio. Es el duplicado que
    costó descargar 2380 archivos por partida doble en agosto.
    """
    _encolar(db, "TRABAJAN")
    db.reclamar_siguiente("w1", lease_s=900, ahora=T0)

    assert db.reencolar("TRABAJAN", ahora=T0 + timedelta(seconds=60)) is False
    assert _fila(db, "TRABAJAN")["estado"] == "procesando"

    # En cuanto el lease vence, sí
    assert db.reencolar("TRABAJAN", ahora=T0 + timedelta(seconds=901)) is True
    assert _fila(db, "TRABAJAN")["estado"] == "recibido"


def test_reencolar_forzado_se_salta_el_lease(db):
    """El escape explícito, para cuando se sabe que el dueño ya no está."""
    _encolar(db, "FORZADA1")
    db.reclamar_siguiente("w1", lease_s=900, ahora=T0)

    assert db.reencolar("FORZADA1", forzar=True, ahora=T0) is True
    assert _fila(db, "FORZADA1")["estado"] == "recibido"


def test_reencolar_dos_veces_es_inocuo(db):
    """Sin cerrojo propio a propósito: es idempotente, y de 'recibido' sólo saca el reclamo."""
    _encolar(db, "DOBLERES", estado="error")

    assert db.reencolar("DOBLERES", ahora=T0) is True
    assert db.reencolar("DOBLERES", ahora=T0) is True

    assert db.reclamar_siguiente("w1", LEASE, ahora=T0) is not None
    assert db.reclamar_siguiente("w2", LEASE, ahora=T0) is None


def test_reencolar_una_consulta_inexistente_devuelve_false(db):
    assert db.reencolar("NOEXISTE", ahora=T0) is False


# --------------------------------------------------------------------------
# Concurrencia
# --------------------------------------------------------------------------

def test_varios_workers_a_la_vez_no_se_pisan(db):
    """La prueba que justifica todo esto: reclamos simultáneos, cero duplicados.

    Las demás llaman a reclamar_siguiente() en secuencia, que no demuestra
    exclusión mutua. Aquí ocho hilos con conexiones propias compiten por veinte
    filas: el invariante es que cada consulta la obtenga exactamente un hilo, y
    que entre todos no salgan ni más ni menos de veinte.

    Es el escenario que se dio en producción con los cuatro workers de
    gunicorn, cuando dos acabaron descargando los mismos 2380 archivos.
    """
    from concurrent.futures import ThreadPoolExecutor

    total = 20
    for i in range(total):
        _encolar(db, f"CARRERA{i:01x}", creada=T0 + timedelta(seconds=i))

    def vaciar(n):
        mias = []
        while (reclamada := db.reclamar_siguiente(f"w{n}", LEASE, ahora=T0)) is not None:
            mias.append(reclamada[0])
        return mias

    with ThreadPoolExecutor(max_workers=8) as pool:
        repartos = list(pool.map(vaciar, range(8)))

    obtenidas = [c for reparto in repartos for c in reparto]
    assert len(obtenidas) == total, f"se reclamaron {len(obtenidas)} de {total}"
    assert len(set(obtenidas)) == total, "alguna consulta la reclamó más de un hilo"
