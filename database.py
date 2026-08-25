import sqlite3
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

DATABASE_PATH = "consultas_goes.db"

# Columnas que convierten `consultas` en una cola con custodia. Se declaran aquí,
# en un solo sitio, porque las usan dos rutas distintas —_init_db() para bases
# nuevas y migrate_db.py para las que ya existen— y si divergieran el fallo
# aparecería en producción, no en las pruebas.
COLUMNAS_COLA = (
    # Tope de reintentos: a la N-ésima la consulta va a 'error' en vez de girar
    # para siempre. NOT NULL DEFAULT 0 para que las filas viejas cuenten desde cero.
    ("intentos", "INTEGER NOT NULL DEFAULT 0"),
    # Hasta cuándo vale el reclamo. NULL = nadie la tiene.
    ("lease_hasta", "DATETIME"),
    # Quién la tiene, como 'hostname:pid'. Sólo para diagnosticar desde el journal.
    ("worker_id", "TEXT"),
    # Backoff entre reintentos: aplaza sin cambiar el estado, que es lo que ve
    # Django. NULL = disponible ya.
    ("disponible_desde", "DATETIME"),
)

# La consulta caliente del worker: filtra por estado y disponibilidad, ordena por
# antigüedad. Con las decenas de filas de hoy da igual para la velocidad; cuesta
# una línea y evita tener que acordarse cuando deje de dar igual.
INDICE_COLA = (
    "CREATE INDEX IF NOT EXISTS idx_consultas_cola "
    "ON consultas(estado, disponible_desde, timestamp_creacion)"
)

# Cuánto vale un reclamo antes de que otro pueda quedárselo. Es el mismo número
# que LATIDO_MAXIMO_S en main.py y por la misma razón: las fases de listado
# (Lustre, S3) tardan en producir el primer avance, y quitarle el trabajo a
# alguien que sigue vivo es peor que esperar de más. El pipeline lo renueva en
# cada llamada a actualizar_estado(), así que la ventana sólo se agota si el
# proceso murió de verdad.
LEASE_POR_DEFECTO_S = 900

class ConsultasDatabase:
    # Tiempo de espera (segundos) antes de lanzar OperationalError si SQLite está bloqueado.
    # Con WAL mode el único bloqueo posible es escritor-escritor; 30s es generoso.
    _CONNECT_TIMEOUT = 30

    def __init__(self, db_path: str = DATABASE_PATH):
        self.db_path = db_path
        logger.info(f"📂 Inicializando base de datos en: {db_path}")
        self._init_db()

    def _connect(self):
        """Abre una conexión SQLite con timeout explícito."""
        return sqlite3.connect(self.db_path, timeout=self._CONNECT_TIMEOUT)

    def _init_db(self):
        """Inicializa la base de datos con más logging"""
        try:
            with self._connect() as conn:
                # WAL: permite lectores concurrentes sin bloquear al escritor
                conn.execute("PRAGMA journal_mode=WAL")
                # Habilitar foreign keys y mejor manejo de errores
                conn.execute("PRAGMA foreign_keys = ON")
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS consultas (
                        id TEXT PRIMARY KEY,
                        estado TEXT NOT NULL,
                        query TEXT NOT NULL,
                        resultados TEXT,
                        progreso INTEGER DEFAULT 0,
                        mensaje TEXT,
                        timestamp_creacion DATETIME NOT NULL,
                        timestamp_actualizacion DATETIME NOT NULL,
                        usuario TEXT DEFAULT 'anonimo',
                        intentos INTEGER NOT NULL DEFAULT 0,
                        lease_hasta DATETIME,
                        worker_id TEXT,
                        disponible_desde DATETIME
                    )
                """)
                # CREATE TABLE IF NOT EXISTS no toca una tabla que ya existe, así
                # que las bases anteriores a la cola necesitan los ALTER. Es lo
                # mismo que hace migrate_db.py; se repite aquí para que arrancar
                # el servicio sin haber corrido la migración no reviente al
                # primer reclamo.
                self._asegurar_columnas_cola(conn)
                conn.execute(INDICE_COLA)
                conn.commit()
                logger.info("✅ Tabla 'consultas' creada/verificada correctamente")
                
        except Exception as e:
            logger.error(f"❌ Error inicializando base de datos: {e}")
            raise

    @staticmethod
    def _asegurar_columnas_cola(conn) -> list:
        """Añade las columnas de cola que falten. Aditivo e idempotente.

        Devuelve los nombres de las que ha añadido, que en una base ya migrada
        es una lista vacía.
        """
        existentes = {fila[1] for fila in conn.execute("PRAGMA table_info(consultas)")}
        añadidas = []
        for nombre, tipo in COLUMNAS_COLA:
            if nombre in existentes:
                continue
            conn.execute(f"ALTER TABLE consultas ADD COLUMN {nombre} {tipo}")
            añadidas.append(nombre)
        return añadidas

    def crear_consulta(self, consulta_id: str, query_dict: Dict) -> bool:
        """Crea una nueva consulta con logging detallado"""
        try:
            logger.debug(f"📝 Intentando crear consulta: {consulta_id}")
            
            if self._consulta_existe(consulta_id):
                logger.warning(f"⚠️  El ID {consulta_id} ya existe. Genera uno nuevo.")
                return False

            # Verificar que query_dict sea serializable a JSON
            query_json = json.dumps(query_dict, ensure_ascii=False, indent=2)
            logger.debug("✅ Query serializada correctamente a JSON")

            # Extraer el usuario del campo 'creado_por', si no existe, se usará el DEFAULT de la tabla.
            usuario = query_dict.get('creado_por')
            
            with self._connect() as conn:
                cursor = conn.execute("""
                    INSERT INTO consultas 
                    (id, estado, query, timestamp_creacion, timestamp_actualizacion, usuario)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    consulta_id,
                    "recibido",
                    query_json,
                    datetime.now().isoformat(),
                    datetime.now().isoformat(),
                    usuario # Si es None, SQLite usará el valor DEFAULT 'anonimo'
                ))
                conn.commit()
                
                logger.info(f"✅ Consulta {consulta_id} almacenada correctamente")
                return True
                
        except sqlite3.IntegrityError as e:
            logger.error(f"❌ Error de integridad (ID duplicado?): {e}")
            return False
        except TypeError as e:
            logger.error(f"❌ Error serializando JSON (posiblemente un tipo de dato no serializable): {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Error inesperado en crear_consulta: {e}")
            return False
    
    def _consulta_existe(self, consulta_id: str) -> bool:
        """Verifica si una consulta ya existe"""
        try:
            with self._connect() as conn:
                cursor = conn.execute(
                    "SELECT 1 FROM consultas WHERE id = ?", 
                    (consulta_id,)
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"❌ Error verificando existencia: {e}")
            return False
    
    def limpiar_consultas_test(self):
        """Limpia consultas de prueba (para desarrollo)"""
        try:
            with self._connect() as conn:
                conn.execute("DELETE FROM consultas WHERE id LIKE 'TEST_%'")
                conn.commit()
                logger.info("🧹 Consultas de prueba limpiadas")
        except Exception as e:
            logger.error(f"❌ Error limpiando consultas test: {e}")
            
    def actualizar_estado(
        self,
        consulta_id: str,
        estado: str,
        progreso: int = None,
        mensaje: str = None,
        lease_s: int = LEASE_POR_DEFECTO_S,
        ahora: Optional[datetime] = None,
    ):
        """Actualiza el estado de una consulta y, si sigue en curso, renueva el lease.

        El pipeline llama a esto en cada avance, así que no hace falta
        instrumentar un latido aparte: mientras haya progreso, el lease se
        empuja solo. Si el proceso muere, deja de llamarse y el lease vence.
        """
        try:
            ahora = ahora or datetime.now()
            with self._connect() as conn:
                query = """
                    UPDATE consultas 
                    SET estado = ?, timestamp_actualizacion = ?
                """
                params = [estado, ahora.isoformat()]

                if estado == "procesando":
                    # Sólo 'procesando' renueva: en un estado terminal el lease
                    # no significa nada, y en 'recibido' la consulta está a
                    # disposición de quien la reclame, no retenida.
                    query += ", lease_hasta = ?"
                    params.append((ahora + timedelta(seconds=lease_s)).isoformat())

                if progreso is not None:
                    query += ", progreso = ?"
                    params.append(progreso)
                
                if mensaje is not None:
                    query += ", mensaje = ?"
                    params.append(mensaje)
                
                query += " WHERE id = ?"
                params.append(consulta_id)
                
                conn.execute(query, params)
                conn.commit()
            return True
        except Exception as e:
            logging.error(f"Error actualizando estado: {e}")
            return False
    
    def guardar_resultados(self, consulta_id: str, resultados: Dict, mensaje: Optional[str] = None):
        """Guarda los resultados de una consulta completada con un mensaje final opcional."""
        try:
            mensaje_final = mensaje or 'Recuperación completada'
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE consultas 
                    SET resultados = ?, estado = 'completado', progreso = 100,
                        timestamp_actualizacion = ?, mensaje = ?
                    WHERE id = ?
                    """,
                    (
                        json.dumps(resultados),
                        datetime.now().isoformat(),
                        mensaje_final,
                        consulta_id,
                    ),
                )
                conn.commit()
            return True
        except Exception as e:
            logging.error(f"Error guardando resultados: {e}")
            return False
    
    def obtener_consulta(self, consulta_id: str) -> Optional[Dict]:
        """Obtiene una consulta por ID"""
        try:
            with self._connect() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("SELECT * FROM consultas WHERE id = ?", (consulta_id,))
                row = cursor.fetchone()
                
                if row:
                    return self._row_to_dict(row)
                return None
        except Exception as e:
            logging.error(f"Error obteniendo consulta: {e}")
            return None
    
    def reclamar_para_reproceso(
        self,
        consulta_id: str,
        estados_en_vuelo: tuple,
        latido_maximo_s: int,
        mensaje: str = "Consulta reenviada para procesamiento",
    ) -> bool:
        """Reclama una consulta para reprocesarla, en una sola operación atómica.

        Devuelve True si esta llamada se quedó con la consulta, y False si otra
        ya la tiene en vuelo. Sirve de cerrojo entre los workers de gunicorn, que
        no comparten memoria: sin esto, dos peticiones repartidas a workers
        distintos encolan cada una su propia tarea sobre el mismo consulta_id y
        acaban descargando los mismos archivos por duplicado, pisándose entre sí.

        El cerrojo es el propio `timestamp_actualizacion`: el pipeline lo refresca
        con cada avance, así que un latido reciente significa que hay alguien
        trabajando de verdad. Si está más viejo que `latido_maximo_s`, la tarea
        murió (típicamente porque se reinició el servicio, que se lleva por
        delante los BackgroundTasks) y la consulta puede reclamarse.

        Los estados fuera de `estados_en_vuelo` (error, completado) no tienen
        trabajo en curso: se reclaman siempre, sin mirar el latido.
        """
        try:
            ahora = datetime.now()
            umbral = (ahora - timedelta(seconds=latido_maximo_s)).isoformat()
            marcadores = ",".join("?" for _ in estados_en_vuelo)
            with self._connect() as conn:
                # UPDATE condicional: SQLite lo resuelve en una transacción, así que
                # de dos llamadas simultáneas solo una puede ver rowcount == 1.
                cursor = conn.execute(
                    f"""
                    UPDATE consultas
                       SET estado = 'recibido',
                           progreso = 0,
                           mensaje = ?,
                           timestamp_actualizacion = ?
                     WHERE id = ?
                       AND (estado NOT IN ({marcadores})
                            OR timestamp_actualizacion < ?)
                    """,
                    (mensaje, ahora.isoformat(), consulta_id, *estados_en_vuelo, umbral),
                )
                conn.commit()
                return cursor.rowcount == 1
        except Exception as e:
            logging.error(f"Error reclamando consulta {consulta_id} para reproceso: {e}")
            return False

    # ------------------------------------------------------------------
    # Primitivas de cola
    #
    # Generalizan lo que reclamar_para_reproceso() ya hacía para una consulta
    # concreta: un UPDATE condicional es un cerrojo atómico entre procesos que
    # no comparten memoria. Aquí el cerrojo pasa a ser «coge la siguiente».
    #
    # Todos aceptan `ahora` inyectable: sin eso, probar leases y backoff exige
    # esperas reales, y esta suite ya ha tenido bastante con dos esperas de
    # margen justo.
    # ------------------------------------------------------------------

    def reclamar_siguiente(
        self,
        worker_id: str,
        lease_s: int = LEASE_POR_DEFECTO_S,
        ahora: Optional[datetime] = None,
    ) -> Optional[tuple]:
        """Reclama la consulta encolada más antigua que esté disponible.

        Devuelve `(consulta_id, query)` o `None` si no hay nada que hacer.

        De dos workers que llamen a la vez, uno se lleva la fila y el otro ve
        otra distinta o `None`: SQLite resuelve el UPDATE en una transacción, y
        el segundo escritor reevalúa su subconsulta contra el estado ya
        commiteado del primero.

        `intentos` se incrementa aquí, en el reclamo, y no al fallar: así una
        consulta que tumba al worker antes de que pueda registrar nada —un OOM,
        un SIGKILL— también gasta intento y no gira para siempre.
        """
        try:
            ahora = ahora or datetime.now()
            marca = ahora.isoformat()
            with self._connect() as conn:
                cursor = conn.execute(
                    """
                    UPDATE consultas
                       SET estado = 'procesando',
                           worker_id = ?,
                           lease_hasta = ?,
                           intentos = intentos + 1,
                           timestamp_actualizacion = ?
                     WHERE id = (
                           SELECT id FROM consultas
                            WHERE estado = 'recibido'
                              AND (disponible_desde IS NULL OR disponible_desde <= ?)
                            ORDER BY timestamp_creacion
                            LIMIT 1
                     )
                    RETURNING id, query
                    """,
                    (
                        worker_id,
                        (ahora + timedelta(seconds=lease_s)).isoformat(),
                        marca,
                        marca,
                    ),
                )
                fila = cursor.fetchone()
                conn.commit()

                if fila is None:
                    return None
                consulta_id, query_json = fila
                logger.info(f"📥 {worker_id} reclama la consulta {consulta_id}")
                return consulta_id, json.loads(query_json)
        except Exception as e:
            logging.error(f"Error reclamando la siguiente consulta: {e}")
            return None

    def liberar_expiradas(
        self,
        ahora: Optional[datetime] = None,
        latido_maximo_s: int = LEASE_POR_DEFECTO_S,
    ) -> int:
        """Devuelve a 'recibido' las consultas cuyo dueño ha desaparecido.

        Devuelve cuántas ha liberado. Esto es lo que convierte el rescate en el
        funcionamiento normal de la cola en vez de un caso especial del
        arranque: cualquier worker lo recoge en su siguiente vuelta.

        Dos formas de quedarse huérfana, y las dos cuentan:

        - Lease vencido. El caso normal: alguien la reclamó y dejó de renovar.
        - Lease NULL con el latido frío. Son las que venían del mundo anterior
          a la cola, procesadas por un BackgroundTask que murió con su proceso:
          están en 'procesando' y nadie las mirará nunca más. Se pide además
          que `timestamp_actualizacion` esté frío para no robarle el trabajo a
          un BackgroundTask que siguiera vivo durante el despliegue — una fila
          reclamada por la cola nunca pasa por aquí, porque reclamar_siguiente()
          escribe estado y lease en la misma sentencia.
        """
        try:
            ahora = ahora or datetime.now()
            marca = ahora.isoformat()
            umbral = (ahora - timedelta(seconds=latido_maximo_s)).isoformat()
            with self._connect() as conn:
                cursor = conn.execute(
                    """
                    UPDATE consultas
                       SET estado = 'recibido',
                           worker_id = NULL,
                           lease_hasta = NULL,
                           timestamp_actualizacion = ?
                     WHERE estado = 'procesando'
                       AND (lease_hasta < ?
                            OR (lease_hasta IS NULL AND timestamp_actualizacion < ?))
                    """,
                    (marca, marca, umbral),
                )
                conn.commit()
                if cursor.rowcount:
                    logger.warning(
                        f"♻️  {cursor.rowcount} consulta(s) sin dueño devueltas a la cola"
                    )
                return cursor.rowcount
        except Exception as e:
            logging.error(f"Error liberando consultas expiradas: {e}")
            return 0

    def fallar_con_reintento(
        self,
        consulta_id: str,
        mensaje: str,
        backoff_s,
        max_intentos: int,
        ahora: Optional[datetime] = None,
    ) -> Optional[str]:
        """Registra un fallo: reencola con espera, o se rinde y deja 'error'.

        Devuelve el estado resultante ('recibido' o 'error'), o None si algo
        salió mal. `backoff_s` admite un número o una secuencia indexada por
        intento —(60, 300, 900) para el 1/5/15 min de la propuesta—; si se
        agota, se repite el último valor.

        El aplazamiento va en `disponible_desde` y no en el estado, para que
        Django siga viendo 'recibido' y lo traduzca a 'en_proceso' como
        siempre: una consulta esperando su reintento no es un error todavía.
        """
        try:
            ahora = ahora or datetime.now()
            with self._connect() as conn:
                fila = conn.execute(
                    "SELECT intentos FROM consultas WHERE id = ?", (consulta_id,)
                ).fetchone()
                if fila is None:
                    logging.error(f"No existe la consulta {consulta_id} al registrar el fallo")
                    return None
                intentos = fila[0] or 0

                if intentos >= max_intentos:
                    estado, disponible_desde = "error", None
                else:
                    estado = "recibido"
                    if isinstance(backoff_s, (list, tuple)):
                        # intentos ya cuenta el que acaba de fallar, así que el
                        # índice 0 corresponde al primer reintento.
                        espera = backoff_s[min(intentos, len(backoff_s)) - 1]
                    else:
                        espera = backoff_s
                    disponible_desde = (ahora + timedelta(seconds=espera)).isoformat()

                # Se suelta el lease en los dos casos: ni una consulta en espera
                # ni una fallida tienen dueño.
                conn.execute(
                    """
                    UPDATE consultas
                       SET estado = ?,
                           mensaje = ?,
                           worker_id = NULL,
                           lease_hasta = NULL,
                           disponible_desde = ?,
                           timestamp_actualizacion = ?
                     WHERE id = ?
                    """,
                    (estado, mensaje, disponible_desde, ahora.isoformat(), consulta_id),
                )
                conn.commit()

                if estado == "error":
                    logger.error(f"❌ {consulta_id} agotó {intentos} intento(s): {mensaje}")
                else:
                    logger.warning(
                        f"⏳ {consulta_id} falla el intento {intentos}, reintento tras "
                        f"{espera}s: {mensaje}"
                    )
                return estado
        except Exception as e:
            logging.error(f"Error registrando el fallo de {consulta_id}: {e}")
            return None

    def reencolar(
        self,
        consulta_id: str,
        mensaje: str = "Consulta reenviada para procesamiento",
        ahora: Optional[datetime] = None,
    ) -> bool:
        """Devuelve una consulta al principio de la cola, venga del estado que venga.

        Es lo que usará POST /query/{id}/restart. No necesita cerrojo propio,
        al revés que reclamar_para_reproceso(): dos reinicios simultáneos dejan
        la misma fila en 'recibido' —la operación es idempotente— y de ahí sólo
        puede sacarla un reclamo, que sí es exclusivo. El cerrojo se hereda de
        la cola en vez de reimplementarse en el endpoint.

        `intentos` se pone a cero: si una persona pide expresamente el
        reinicio, empieza con presupuesto entero y no con lo que quedara.
        """
        try:
            ahora = ahora or datetime.now()
            with self._connect() as conn:
                cursor = conn.execute(
                    """
                    UPDATE consultas
                       SET estado = 'recibido',
                           progreso = 0,
                           mensaje = ?,
                           intentos = 0,
                           worker_id = NULL,
                           lease_hasta = NULL,
                           disponible_desde = NULL,
                           timestamp_actualizacion = ?
                     WHERE id = ?
                    """,
                    (mensaje, ahora.isoformat(), consulta_id),
                )
                conn.commit()
                return cursor.rowcount == 1
        except Exception as e:
            logging.error(f"Error reencolando la consulta {consulta_id}: {e}")
            return False

    def listar_consultas(self, estado: str = None, usuario: str = None, limite: int = 100) -> List[Dict]:
        """Lista consultas con filtros opcionales"""
        try:
            with self._connect() as conn:
                conn.row_factory = sqlite3.Row
                query = "SELECT * FROM consultas WHERE 1=1"
                params = []
                
                if estado:
                    query += " AND estado = ?"
                    params.append(estado)
                
                if usuario:
                    query += " AND usuario = ?"
                    params.append(usuario)
                
                query += " ORDER BY timestamp_creacion DESC LIMIT ?"
                params.append(limite)
                
                cursor = conn.execute(query, params)
                return [self._row_to_dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Error listando consultas: {e}")
            return []
    
    def eliminar_consulta(self, consulta_id: str) -> bool:
        """Elimina una consulta por ID. Devuelve True si se eliminó alguna fila."""
        try:
            with self._connect() as conn:
                cur = conn.execute("DELETE FROM consultas WHERE id = ?", (consulta_id,))
                conn.commit()
                return cur.rowcount > 0
        except Exception as e:
            logging.error(f"Error eliminando consulta {consulta_id}: {e}")
            return False
    
    def _row_to_dict(self, row) -> Dict:
        """Convierte una fila a diccionario (simplificado)"""
        return {
            'id': row['id'],
            'estado': row['estado'],
            'query': json.loads(row['query']),  # ← Única query
            'resultados': json.loads(row['resultados']) if row['resultados'] else None,
            'progreso': row['progreso'],
            'mensaje': row['mensaje'],
            'timestamp_creacion': row['timestamp_creacion'],
            'timestamp_actualizacion': row['timestamp_actualizacion'],
            'usuario': row['usuario']
        }