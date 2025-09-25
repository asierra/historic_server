import sqlite3
import os
import json
import random
import string
from database import ConsultasDatabase

def generar_id_unico(prefijo="TEST_"):
    """Genera un ID único para pruebas"""
    random_suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=6))
    return f"{prefijo}{random_suffix}"

def diagnostico_base_datos():
    print("🔍 INICIANDO DIAGNÓSTICO DE BASE DE DATOS")
    
    db_path = "consultas_goes.db"
    
    # 1. Verificar archivo de base de datos
    print(f"1. Verificando archivo de base de datos: {db_path}")
    if os.path.exists(db_path):
        print(f"   ✅ Archivo existe. Tamaño: {os.path.getsize(db_path)} bytes")
    else:
        print("   ❌ Archivo no existe. Se creará automáticamente.")
    
    # 2. Probar conexión
    print("2. Probando conexión a la base de datos...")
    try:
        db = ConsultasDatabase()
        print("   ✅ Conexión exitosa")
    except Exception as e:
        print(f"   ❌ Error de conexión: {e}")
        return False
    
    # 3. Verificar estructura de la tabla
    print("3. Verificando estructura de la tabla...")
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM consultas")
            count = cursor.fetchone()[0]
            print(f"   📊 Consultas existentes en BD: {count}")
    except Exception as e:
        print(f"   ❌ Error verificando tabla: {e}")
        return False
    
    # 4. Probar inserción con ID único
    print("4. Probando inserción de datos...")
    try:
        # Generar ID único para esta prueba
        test_id = generar_id_unico()
        
        datos_prueba = {
            "satelite": "GOES-EAST",
            "nivel": "L1b", 
            "fechas": {"20240101": ["09:00-12:00"]},
            "bandas": ["ALL"]
        }
        
        success = db.crear_consulta(test_id, datos_prueba)
        if success:
            print(f"   ✅ Inserción exitosa con ID: {test_id}")
            
            # Verificar que se insertó correctamente
            with sqlite3.connect(db_path) as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM consultas")
                count = cursor.fetchone()[0]
                print(f"   📊 Total de consultas en BD: {count}")
        else:
            print("   ❌ Error en inserción de prueba")
            return False
            
    except Exception as e:
        print(f"   ❌ Error en prueba de inserción: {e}")
        return False
    
    # 5. Probar múltiples inserciones
    print("5. Probando múltiples inserciones...")
    try:
        for i in range(3):
            test_id = generar_id_unico(f"TEST_MULTI_{i}_")
            datos = {
                "satelite": "GOES-EAST",
                "nivel": "L1b", 
                "fechas": {f"2024010{i+1}": [f"0{i+9}:00-1{i+2}:00"]},
                "bandas": ["13", "08"]
            }
            
            success = db.crear_consulta(test_id, datos)
            if success:
                print(f"   ✅ Inserción {i+1} exitosa: {test_id}")
            else:
                print(f"   ❌ Error en inserción {i+1}")
                return False
                
    except Exception as e:
        print(f"   ❌ Error en inserciones múltiples: {e}")
        return False
    
    print("✅ DIAGNÓSTICO COMPLETADO")
    return True

if __name__ == "__main__":
    diagnostico_base_datos()