"""Bucle consumidor de la cola de consultas.

Corre en un hilo dentro del propio proceso de la API. La alternativa —un
`worker.py` con su unidad de systemd— se evaluó con datos de tahan y se
descartó; el razonamiento está en `PLAN_COLA_DURABLE.md` §6.

Que haya un bucle por cada worker de gunicorn es seguro: el reclamo es un
UPDATE atómico (`database.reclamar_siguiente`) y de dos que compitan sólo uno
ve la fila. Tampoco sube el techo de concurrencia, porque hoy cada `POST` ya
lanzaba su `BackgroundTask` en el worker que lo atendiera.

Un hilo y no un `asyncio.Task`: el pipeline es bloqueante de principio a fin
—`ProcessPool`, disco, `boto3`— y dentro del bucle de eventos ahogaría a
uvicorn.
"""

import os
import socket
import threading
import time
from typing import Optional

import structlog

from database import LEASE_POR_DEFECTO_S

log = structlog.get_logger(__name__)

# Cada cuánto se pregunta por trabajo nuevo. La comprobación previa es de sólo
# lectura (`hay_trabajo`), así que sondear seguido no cuesta escrituras.
POLL_POR_DEFECTO_S = 5.0

# Tope de cada cuánto se barren los leases vencidos. Mucho más espaciado que el
# sondeo porque sí es una escritura, y porque una consulta huérfana puede
# esperar un minuto más sin que nadie lo note.
#
# Es un tope y no un valor fijo: con el lease por defecto (900 s) barrer cada
# minuto sobra, pero si alguien lo baja a 30 s esperando recuperar rápido, un
# barrido cada 60 s se lo impediría en silencio. Por eso el valor efectivo es
# el menor de los dos (ver `barrido_efectivo`).
BARRIDO_MAXIMO_S = 60.0

# Tope de intentos y espera entre ellos (1 / 5 / 15 min), según §8 del plan.
MAX_INTENTOS_POR_DEFECTO = 3
BACKOFF_POR_DEFECTO_S = (60, 300, 900)


def barrido_efectivo(lease_s: float) -> float:
    """Cada cuánto barrer, dado un lease.

    La mitad del lease: garantiza que nunca se tarde más de un lease y medio en
    recuperar trabajo huérfano, sea cual sea la configuración. Sin esto, bajar
    el lease no acelera la recuperación —la limita el barrido— y el efecto es
    invisible: parece que el lease no funciona.
    """
    return min(BARRIDO_MAXIMO_S, lease_s / 2)


def identificar_worker() -> str:
    """Identidad del consumidor, para poder seguirlo en el journal."""
    return f"{socket.gethostname()}:{os.getpid()}"


class BucleDeCola:
    """Reclama consultas de la cola y las pasa por el pipeline de siempre."""

    def __init__(
        self,
        db,
        recover,
        worker_id: Optional[str] = None,
        poll_s: float = POLL_POR_DEFECTO_S,
        barrido_s: Optional[float] = None,
        lease_s: int = LEASE_POR_DEFECTO_S,
        max_intentos: int = MAX_INTENTOS_POR_DEFECTO,
        backoff_s=BACKOFF_POR_DEFECTO_S,
    ):
        self.db = db
        self.recover = recover
        self.worker_id = worker_id or identificar_worker()
        self.poll_s = poll_s
        self.lease_s = lease_s
        self.barrido_s = (
            barrido_efectivo(lease_s) if barrido_s is None else barrido_s
        )
        self.max_intentos = max_intentos
        self.backoff_s = backoff_s

        self.consulta_en_curso: Optional[str] = None
        self._parar = threading.Event()
        self._hilo: Optional[threading.Thread] = None
        self._ultimo_barrido = 0.0

    # -- ciclo de vida -------------------------------------------------

    @property
    def vivo(self) -> bool:
        return self._hilo is not None and self._hilo.is_alive()

    def arrancar(self) -> None:
        if self.vivo:
            return
        self._parar.clear()
        self._hilo = threading.Thread(
            target=self._bucle, name=f"cola-{self.worker_id}", daemon=True
        )
        self._hilo.start()
        log.info("bucle_de_cola_arrancado", worker_id=self.worker_id,
                 poll_s=self.poll_s, lease_s=self.lease_s)

    def parar(self, timeout_s: float = 5.0) -> None:
        """Pide al bucle que no coja más trabajo y espera un margen corto.

        **No se reencola lo que esté en curso.** Sería tentador soltar el lease
        para que otro lo recoja ya, pero el pipeline sigue escribiendo en ese
        directorio mientras el proceso agoniza, y otro worker de gunicorn que
        aún viva podría reclamarlo y descargar los mismos archivos en paralelo
        —el duplicado que costó 2380 archivos por partida doble en agosto—. Se
        deja vencer el lease: cuesta hasta un cuarto de hora, y a cambio no
        puede haber dos escritores nunca.
        """
        self._parar.set()
        if self._hilo is not None:
            self._hilo.join(timeout=timeout_s)

        if self.consulta_en_curso:
            log.warning(
                "bucle_de_cola_parado_con_trabajo_en_curso",
                worker_id=self.worker_id,
                consulta_id=self.consulta_en_curso,
                detalle="se reanudará sola cuando venza el lease",
            )
        else:
            log.info("bucle_de_cola_parado", worker_id=self.worker_id)

    # -- trabajo -------------------------------------------------------

    def _bucle(self) -> None:
        while not self._parar.is_set():
            try:
                if not self.una_vuelta():
                    # Espera interrumpible: en el apagado se sale al momento.
                    self._parar.wait(self.poll_s)
            except Exception:
                # Un fallo aquí es del bucle, no de una consulta concreta.
                # Que no se lleve el hilo por delante: sin él la cola deja de
                # drenar y nadie se entera hasta que /health lo diga.
                log.exception("fallo_en_el_bucle_de_cola", worker_id=self.worker_id)
                self._parar.wait(self.poll_s)

    def una_vuelta(self) -> bool:
        """Una iteración completa. Devuelve True si procesó una consulta.

        Pública y sin hilos de por medio a propósito: así las pruebas pueden
        conducir el bucle paso a paso en vez de arrancarlo y esperar.
        """
        self._barrer_si_toca()

        if not self.db.hay_trabajo():
            return False

        reclamada = self.db.reclamar_siguiente(self.worker_id, self.lease_s)
        if reclamada is None:
            return False

        consulta_id, query = reclamada
        self._procesar(consulta_id, query)
        return True

    def _barrer_si_toca(self) -> None:
        ahora = time.monotonic()
        if ahora - self._ultimo_barrido < self.barrido_s:
            return
        self._ultimo_barrido = ahora
        self.db.liberar_expiradas(latido_maximo_s=self.lease_s)

    def _procesar(self, consulta_id: str, query: dict) -> None:
        self.consulta_en_curso = consulta_id
        structlog.contextvars.bind_contextvars(consulta_id=consulta_id)
        try:
            self.recover.procesar_consulta(consulta_id, query)

            # El pipeline atrapa sus propias excepciones y deja la consulta en
            # 'error' (recover.py). Si se dejara así no habría reintento nunca,
            # que es justo lo que la cola viene a arreglar: se traduce ese
            # 'error' a la decisión de reintentar o rendirse, conservando el
            # mensaje que escribió el pipeline.
            consulta = self.db.obtener_consulta(consulta_id)
            if consulta and consulta["estado"] == "error":
                self.db.fallar_con_reintento(
                    consulta_id,
                    consulta.get("mensaje") or "El pipeline terminó en error",
                    self.backoff_s,
                    self.max_intentos,
                )
        except Exception as e:
            log.exception("consulta_fallida_en_el_bucle", consulta_id=consulta_id)
            self.db.fallar_con_reintento(
                consulta_id, f"Error no controlado: {e}",
                self.backoff_s, self.max_intentos,
            )
        finally:
            self.consulta_en_curso = None
            structlog.contextvars.unbind_contextvars("consulta_id")
