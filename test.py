import requests
import json
import sys


# URL de tu API
BASE_URL = "http://localhost:8000"

# Datos de ejemplo
query_data = {
    "nivel": "L2",
    "dominio": "conus",
    "productos": ["CMIP", "ACTP"],
    "fechas": {
        "20240101-20240101": ["19:19-22:19"],
        "20240212-20240212": ["17:51-20:51", "19:31-22:31"]
    }
}

if len(sys.argv) > 1:
	with open(sys.argv[1], 'r') as file:
		strjson = file.read()
		query_data = json.loads(strjson)
		
# Validar query
response = requests.post(f"{BASE_URL}/api/validate", json=query_data)
print("Validación:", response)
#print("Validación:", response.json())

# Analizar query
response = requests.post(f"{BASE_URL}/api/analyze", json=query_data)
print("Análisis:", response.json())

# Obtener satélites válidos
response = requests.get(f"{BASE_URL}/api/satelites")
print("Satélites:", response.json())
