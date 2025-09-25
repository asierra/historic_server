import sqlite3
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional

# Configurar logging más detallado
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class ConsultasDatabase:
    def __init__(self, db_path: str = "consultas_goes.db"):
        self.db_path = db_path
        logger.info(f"📂 Inicializando base de datos en: {db_path}")
        self._init_db()
    
    def _init_db(self):
        """Inicializa la base de datos con más logging"""
        try:
            with sqlite3.connect(self.db_path) as conn:
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
            print(f"🔍 Query dict recibido: {query_dict}")
            query_json = json.dumps(query_dict, ensure_ascii=False, indent=2)
            print(f"📄 JSON generado: {query_json}")
            logger.debug("✅ Query serializada correctamente a JSON")
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    INSERT INTO consultas 
                    (id, estado, query, timestamp_creacion, timestamp_actualizacion)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    consulta_id,
                    "recibido",
                    query_json,
                    datetime.now().isoformat(),
                    datetime.now().isoformat()
                ))
                conn.commit()
                
                logger.info(f"✅ Consulta {consulta_id} almacenada correctamente")
                return True
                
        except sqlite3.IntegrityError as e:
            logger.error(f"❌ Error de integridad (ID duplicado?): {e}")
            return False
        except json.JSONEncodeError as e:
            logger.error(f"❌ Error serializando JSON: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Error inesperado en crear_consulta: {e}")
            return False
    
    def _consulta_existe(self, consulta_id: str) -> bool:
        """Verifica si una consulta ya existe"""
        try:
            with sqlite3.connect(self.db_path) as conn:
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
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM consultas WHERE id LIKE 'TEST_%'")
                conn.commit()
                logger.info("🧹 Consultas de prueba limpiadas")
        except Exception as e:
            logger.error(f"❌ Error limpiando consultas test: {e}")
            
    def actualizar_estado(self, consulta_id: str, estado: str, progreso: int = None, mensaje: str = None):
        """Actualiza el estado de una consulta"""
        try:
            with sqlite3.connect(self.db_path) as conn:
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
    
    def guardar_resultados(self, consulta_id: str, resultados: Dict):
        """Guarda los resultados de una consulta completada"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    UPDATE consultas 
                    SET resultados = ?, estado = 'completado', progreso = 100,
                        timestamp_actualizacion = ?, mensaje = 'Procesamiento completado'
                    WHERE id = ?
                """, (
                    json.dumps(resultados),
                    datetime.now().isoformat(),
                    consulta_id
                ))
                conn.commit()
            return True
        except Exception as e:
            logging.error(f"Error guardando resultados: {e}")
            return False
    
    def obtener_consulta(self, consulta_id: str) -> Optional[Dict]:
        """Obtiene una consulta por ID"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("SELECT * FROM consultas WHERE id = ?", (consulta_id,))
                row = cursor.fetchone()
                
                if row:
                    return self._row_to_dict(row)
                return None
        except Exception as e:
            logging.error(f"Error obteniendo consulta: {e}")
            return None
    
    def listar_consultas(self, estado: str = None, usuario: str = None, limite: int = 100) -> List[Dict]:
        """Lista consultas con filtros opcionales"""
        try:
            with sqlite3.connect(self.db_path) as conn:
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