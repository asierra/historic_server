from database import ConsultasDatabase
import random
import string
import sqlite3

def generar_id_unico(prefijo="TEST_"):
    """Genera un ID único para pruebas"""
    random_suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    return f"{prefijo}{random_suffix}"

def test_database_directo():
    print("🧪 Test directo de base de datos")
    
    db = ConsultasDatabase()
    
    # Generar ID único para este test
    test_id = generar_id_unico("TEST_RAPIDO_")
    
    # Datos de prueba simples
    test_data = {
        "satelite": "GOES-EAST",
        "nivel": "L1b",
        "fechas": {"20240101": ["09:00-12:00"]},
        "bandas": ["ALL"]
    }
    
    print(f"🔍 Probando con ID: {test_id}")
    success = db.crear_consulta(test_id, test_data)
    print(f"Resultado: {'✅ Éxito' if success else '❌ Fallo'}")
    
    if success:
        # Verificar que realmente se guardó
        with sqlite3.connect("consultas_goes.db") as conn:
            cursor = conn.execute("SELECT estado FROM consultas WHERE id = ?", (test_id,))
            resultado = cursor.fetchone()
            if resultado:
                print(f"✅ Consulta verificada en BD. Estado: {resultado[0]}")
            else:
                print("❌ Consulta no encontrada en BD")

if __name__ == "__main__":
    test_database_directo()