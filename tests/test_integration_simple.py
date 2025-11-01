#!/usr/bin/env python3
"""
Prueba de integración simple que simula el flujo completo
sin necesitar el servidor web
"""
import json
import sys
import os
from datetime import datetime

# Añadir el directorio al path
sys.path.insert(0, os.path.dirname(__file__))

from database import ConsultasDatabase
from background_simulator import BackgroundSimulator
from processors import HistoricQueryProcessor
from config import SatelliteConfigGOES

def simular_flujo_completo():
    """Simula el flujo completo del endpoint /query"""
    
    print("="*70)
    print("PRUEBA DE INTEGRACIÓN COMPLETA: L1b con bandas=['ALL']")
    print("="*70)
    print()
    
    # 1. Inicializar componentes
    print("1. Inicializando componentes...")
    db = ConsultasDatabase(':memory:')
    simulator = BackgroundSimulator(db)
    config = SatelliteConfigGOES()
    processor = HistoricQueryProcessor()
    print("   ✅ Componentes inicializados")
    print()
    
    # 2. Preparar request
    print("2. Preparando request...")
    request = {
        "sat": "GOES-16",
        "sensor": "abi",
        "nivel": "L1b",
        "dominio": "fd",
        "bandas": ["ALL"],
        "fechas": {
            "20230101": ["12:00-12:20"]
        }
    }
    print(f"   Request: {json.dumps(request, indent=2)}")
    print()
    
    # 3. Validar y preparar (simular el flujo de main.py)
    print("3. Validando y preparando request...")
    data = request.copy()
    data['sat'] = request.get('sat', config.DEFAULT_SATELLITE)
    data['sensor'] = request.get('sensor', config.DEFAULT_SENSOR)
    data['nivel'] = request.get('nivel', config.DEFAULT_LEVEL)
    data['bandas'] = request.get('bandas') or config.DEFAULT_BANDAS
    data['bandas'] = config.validate_bandas(data['bandas'])
    print(f"   Bandas validadas: {data['bandas']}")
    print()
    
    # 4. Procesar query
    print("4. Procesando query...")
    query_obj = processor.procesar_request(data, config)
    query_dict = query_obj.to_dict()
    print(f"   Bandas originales: {query_dict['_original_request']['bandas']}")
    print(f"   Bandas expandidas: {query_dict['bandas']}")
    print()
    
    # 5. Guardar en BD
    print("5. Guardando en base de datos...")
    consulta_id = "TEST_INTEGRATION"
    if not db.crear_consulta(consulta_id, query_dict):
        print("   ❌ Error guardando consulta")
        return False
    print(f"   ✅ Consulta guardada: {consulta_id}")
    print()
    
    # 6. Procesar en background (simulado de forma síncrona)
    print("6. Procesando consulta (simulador)...")
    simulator.procesar_consulta(consulta_id, query_dict)
    print("   ✅ Procesamiento completado")
    print()
    
    # 7. Obtener resultados
    print("7. Obteniendo resultados...")
    consulta = db.obtener_consulta(consulta_id)
    
    if not consulta:
        print("   ❌ No se encontró la consulta")
        return False
    
    print(f"   Estado: {consulta['estado']}")
    print(f"   Progreso: {consulta['progreso']}%")
    print(f"   Mensaje: {consulta['mensaje']}")
    print()
    
    # 8. Verificar resultados
    print("8. Verificando resultados...")
    
    if consulta['estado'] != 'completado':
        print(f"   ❌ Estado inesperado: {consulta['estado']}")
        return False
    
    if not consulta.get('resultados'):
        print("   ❌ No hay resultados")
        return False
    
    resultados = consulta['resultados']
    lustre_archivos = resultados['fuentes']['lustre']['archivos']
    s3_archivos = resultados['fuentes']['s3']['archivos']
    todos = lustre_archivos + s3_archivos
    
    print(f"   Total archivos: {len(todos)}")
    print(f"   Tamaño total: {resultados['tamaño_total_mb']} MB")
    
    if not todos:
        print("   ❌ No se generaron archivos")
        return False
    
    ejemplo = todos[0]
    print(f"   Ejemplo: {ejemplo}")
    print()
    
    # Verificar tipo de archivos
    print("9. Verificando tipo de archivos...")
    
    if ejemplo.endswith('.tgz'):
        print("   ✅ CORRECTO: Devuelve archivos .tgz sin expandir")
        
        tgz_count = sum(1 for f in todos if f.endswith('.tgz'))
        nc_count = sum(1 for f in todos if f.endswith('.nc'))
        
        print(f"   Archivos .tgz: {tgz_count}")
        print(f"   Archivos .nc: {nc_count}")
        
        if nc_count > 0:
            print("   ⚠️  ADVERTENCIA: Se encontraron archivos .nc mezclados")
            return False
        
        print()
        print("="*70)
        print("✅ PRUEBA DE INTEGRACIÓN EXITOSA")
        print("="*70)
        return True
        
    elif ejemplo.endswith('.nc'):
        print("   ❌ ERROR: Devuelve archivos .nc expandidos (debería ser .tgz)")
        return False
    else:
        print(f"   ❌ ERROR: Formato de archivo desconocido: {ejemplo}")
        return False

if __name__ == "__main__":
    try:
        exito = simular_flujo_completo()
        sys.exit(0 if exito else 1)
    except Exception as e:
        print(f"\n❌ ERROR INESPERADO: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
