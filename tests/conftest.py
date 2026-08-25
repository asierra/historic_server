"""Utilidades compartidas por las pruebas que necesitan que la cola drene.

Desde la entrega 2 los endpoints no lanzan trabajo: sólo dejan la fila en
'recibido' y un bucle la recoge (`PLAN_COLA_DURABLE.md` §5). En producción ese
bucle lo arranca el `lifespan`, pero el `TestClient` de estos módulos se crea a
nivel de módulo y **sin** `with`, así que el `lifespan` nunca corre. Las pruebas
que esperan a ver 'completado' tienen que arrancarlo ellas.
"""

import contextlib

import main
from cola import BucleDeCola


class _RecoverDinamico:
    """Reenvía a `main.recover` en el momento de la llamada, no al arrancar.

    Hace falta porque alguna prueba lo sustituye a mitad —por ejemplo para
    pasar de «Lustre responde» a «sólo responde S3»— y un bucle que lo hubiera
    capturado al arrancar seguiría usando el simulador viejo. En producción
    `recover` se construye una vez en el `lifespan` y no cambia, así que esta
    indirección es sólo de las pruebas y por eso vive aquí.
    """

    def procesar_consulta(self, consulta_id, query_dict):
        return main.recover.procesar_consulta(consulta_id, query_dict)


@contextlib.contextmanager
def cola_drenando(poll_s: float = 0.05):
    """Arranca un bucle contra los objetos ya parcheados en `main`.

    Se lee `main.db` al entrar, no al importar, para que recoja lo que haya
    puesto el fixture de la prueba.
    """
    bucle = BucleDeCola(
        db=main.db,
        recover=_RecoverDinamico(),
        worker_id="pytest",
        poll_s=poll_s,
        # Un segundo, no el minuto de producción ni cero: barrer es una
        # escritura, y hacerla en cada vuelta de 50 ms mete contención en la
        # base contra la propia prueba.
        barrido_s=1.0,
    )
    bucle.arrancar()
    try:
        yield bucle
    finally:
        bucle.parar(timeout_s=10.0)
