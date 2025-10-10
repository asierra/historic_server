#!/usr/bin/env python3
import requests
import json

# Configuración
url = "http://127.0.0.1:9041/validate"

# Consulta completa de julio (similar a la que dio 1.7TB real)  
test_data = {
    "dominio": "conus",
    "nivel": "L1b", 
    "bandas": ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16"],
    "fechas": {
        "20230701-20230731": ["00:00-23:59"] 
    }
}

print("🧪 Probando endpoint /validate...")
print(f"URL: {url}")
print(f"Data: {json.dumps(test_data, indent=2)}")

try:
    response = requests.post(url, json=test_data, timeout=10)
    
    print(f"\n📊 RESPUESTA:")
    print(f"Status Code: {response.status_code}")
    print(f"Headers: {dict(response.headers)}")
    
    if response.status_code == 200:
        result = response.json()
        print(f"✅ SUCCESS!")
        print(json.dumps(result, indent=2, ensure_ascii=False))
        
        # Extraer tamaño estimado si existe
        if 'tamanio_estimado' in result:
            tamaño = result['tamanio_estimado']
            print(f"\n🎯 Tamaño estimado: {tamaño} MB ({tamaño/1024:.2f} GB)")
    else:
        print(f"❌ ERROR: {response.status_code}")
        print(f"Response: {response.text}")
        
except requests.exceptions.ConnectionError:
    print("❌ Error: No se pudo conectar al servidor. ¿Está ejecutándose?")
except Exception as e:
    print(f"❌ Error inesperado: {e}")