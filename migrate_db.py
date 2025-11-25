#!/usr/bin/env python3
"""
Script de migración de base de datos para historic_server.

Este script verifica el esquema actual de la base de datos y realiza
las migraciones necesarias para mantener la compatibilidad con versiones
más recientes, preservando todos los datos existentes.

Uso:
    python migrate_db.py [ruta_a_base_de_datos.db]

Si no se proporciona una ruta, se usará el valor de DB_PATH del .env
o 'consultas_goes.db' por defecto.
"""

import sqlite3
import sys
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Tuple
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)


def get_db_path() -> str:
    """Obtiene la ruta de la base de datos desde argumentos o configuración."""
    if len(sys.argv) > 1:
        return sys.argv[1]
    
    # Intentar cargar desde .env
    try:
        from settings import settings
        return str(settings.db_path)
    except Exception:
        return "consultas_goes.db"


def backup_database(db_path: str) -> str:
    """Crea un backup de la base de datos antes de la migración."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{db_path}.backup_{timestamp}"
    
    log.info(f"📦 Creando backup en: {backup_path}")
    shutil.copy2(db_path, backup_path)
    log.info(f"✅ Backup creado exitosamente")
    
    return backup_path


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    """Obtiene la lista de columnas de una tabla."""
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cursor.fetchall()]


def column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    """Verifica si una columna existe en una tabla."""
    columns = get_table_columns(conn, table_name)
    return column_name in columns


def migrate_add_usuario_column(conn: sqlite3.Connection) -> bool:
    """Añade la columna 'usuario' si no existe."""
    if column_exists(conn, "consultas", "usuario"):
        log.info("  ℹ️  Columna 'usuario' ya existe")
        return False
    
    log.info("  🔧 Añadiendo columna 'usuario'")
    conn.execute("""
        ALTER TABLE consultas 
        ADD COLUMN usuario TEXT DEFAULT 'anonimo'
    """)
    conn.commit()
    log.info("  ✅ Columna 'usuario' añadida")
    return True


def verify_schema(conn: sqlite3.Connection) -> bool:
    """Verifica que el esquema tenga todas las columnas esperadas."""
    expected_columns = [
        'id', 'estado', 'query', 'resultados', 'progreso', 
        'mensaje', 'timestamp_creacion', 'timestamp_actualizacion', 'usuario'
    ]
    
    actual_columns = get_table_columns(conn, "consultas")
    
    missing = set(expected_columns) - set(actual_columns)
    if missing:
        log.error(f"❌ Faltan columnas: {missing}")
        return False
    
    log.info(f"✅ Esquema verificado correctamente ({len(actual_columns)} columnas)")
    return True


def get_migration_info(conn: sqlite3.Connection) -> dict:
    """Obtiene información sobre el estado actual de la base de datos."""
    cursor = conn.execute("SELECT COUNT(*) FROM consultas")
    total_consultas = cursor.fetchone()[0]
    
    cursor = conn.execute("SELECT COUNT(DISTINCT estado) FROM consultas")
    estados_unicos = cursor.fetchone()[0]
    
    columns = get_table_columns(conn, "consultas")
    
    return {
        'total_consultas': total_consultas,
        'estados_unicos': estados_unicos,
        'columnas': columns,
        'num_columnas': len(columns)
    }


def main():
    db_path = get_db_path()
    
    log.info("=" * 60)
    log.info("🔄 MIGRACIÓN DE BASE DE DATOS - HISTORIC SERVER")
    log.info("=" * 60)
    log.info(f"📂 Base de datos: {db_path}")
    
    # Verificar que existe
    if not Path(db_path).exists():
        log.error(f"❌ La base de datos no existe: {db_path}")
        log.info("💡 Si es una instalación nueva, el servidor creará la BD automáticamente")
        return 0
    
    # Crear backup
    backup_path = backup_database(db_path)
    
    try:
        # Conectar a la base de datos
        log.info("\n📊 Analizando base de datos...")
        with sqlite3.connect(db_path) as conn:
            # Habilitar foreign keys
            conn.execute("PRAGMA foreign_keys = ON")
            
            # Obtener info antes de migrar
            info_antes = get_migration_info(conn)
            log.info(f"  📈 Total de consultas: {info_antes['total_consultas']}")
            log.info(f"  🏷️  Columnas actuales: {info_antes['num_columnas']}")
            log.info(f"     {', '.join(info_antes['columnas'])}")
            
            # Ejecutar migraciones
            log.info("\n🔧 Ejecutando migraciones...")
            cambios_realizados = []
            
            if migrate_add_usuario_column(conn):
                cambios_realizados.append("Columna 'usuario' añadida")
            
            # Verificar esquema final
            log.info("\n🔍 Verificando esquema final...")
            if not verify_schema(conn):
                raise Exception("El esquema no es correcto después de la migración")
            
            # Info después de migrar
            info_despues = get_migration_info(conn)
            
            # Verificar integridad de datos
            log.info("\n🔍 Verificando integridad de datos...")
            if info_antes['total_consultas'] != info_despues['total_consultas']:
                raise Exception(
                    f"❌ Pérdida de datos detectada: "
                    f"{info_antes['total_consultas']} -> {info_despues['total_consultas']}"
                )
            log.info(f"  ✅ Todas las consultas preservadas ({info_despues['total_consultas']})")
            
            # Resumen
            log.info("\n" + "=" * 60)
            log.info("✅ MIGRACIÓN COMPLETADA EXITOSAMENTE")
            log.info("=" * 60)
            if cambios_realizados:
                log.info("📝 Cambios realizados:")
                for cambio in cambios_realizados:
                    log.info(f"  • {cambio}")
            else:
                log.info("ℹ️  No se requirieron cambios (esquema ya actualizado)")
            
            log.info(f"\n💾 Backup guardado en: {backup_path}")
            log.info("   (Puedes eliminarlo si todo funciona correctamente)")
            
    except Exception as e:
        log.error(f"\n❌ Error durante la migración: {e}")
        log.error(f"🔄 Puedes restaurar el backup desde: {backup_path}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
