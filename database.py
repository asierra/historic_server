import sqlite3
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

DATABASE_PATH = "consultas_goes.db"

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
                        usuario TEXT DEFAULT 'anonimo'
                    )
                """)
                conn.commit()
                logger.info("✅ Tabla 'consultas' creada/verificada correctamente")
                
        except Exception as e:
            logger.error(f"❌ Error inicializando base de datos: {e}")
            raise
    
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
            
    def actualizar_estado(self, consulta_id: str, estado: str, progreso: int = None, mensaje: str = None):
        """Actualiza el estado de una consulta"""
        try:
            with self._connect() as conn:
                query = """
                    UPDATE consultas 
                    SET estado = ?, timestamp_actualizacion = ?
                """
                params = [estado, datetime.now().isoformat()]
                
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