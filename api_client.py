import requests
import json
import time
import sys
from typing import Dict

# --- Configuración ---
BASE_URL = "http://localhost:8000"
POLL_INTERVAL = 2  # Segundos entre cada sondeo de estado
TIMEOUT = 60       # Segundos máximos para esperar que una consulta se complete

def print_separator(title: str):
    """Imprime un separador visual para la salida."""
    print(f"\n{'='*25} {title.upper()} {'='*25}")

def print_response(response: requests.Response):
    """Imprime de forma legible la respuesta de una solicitud."""
    print(f"-> Código de Estado: {response.status_code}")
    try:
        print("-> Respuesta JSON:")
        print(json.dumps(response.json(), indent=2, ensure_ascii=False))
    except json.JSONDecodeError:
        print(f"-> Respuesta (No-JSON): {response.text}")

def main(json_file_path: str):
    """
    Función principal que envía una solicitud desde un archivo JSON y monitorea el resultado.
    """
    # --- 1. Cargar la solicitud desde el archivo JSON ---
    print_separator(f"Cargando solicitud desde {json_file_path}")
    try:
        with open(json_file_path, 'r') as f:
            request_data = json.load(f)
        print("Solicitud cargada exitosamente.")
        print(json.dumps(request_data, indent=2, ensure_ascii=False))
    except FileNotFoundError:
        print(f"❌ Error: El archivo '{json_file_path}' no fue encontrado.")
        return
    except json.JSONDecodeError:
        print(f"❌ Error: El archivo '{json_file_path}' no contiene un JSON válido.")
        return

    # --- 2. Validar la solicitud ---
    print_separator("Paso 1: Validando la solicitud")
    try:
        response = requests.post(f"{BASE_URL}/validate", json=request_data)
        print_response(response)
        if response.status_code != 200:
            print("\n❌ La validación falló. Abortando.")
            return
    except requests.ConnectionError as e:
        print(f"❌ Error de conexión: No se pudo conectar a {BASE_URL}. ¿Está el servidor corriendo?")
        return

    # --- 3. Crear la consulta ---
    print_separator("Paso 2: Creando la consulta")
    response = requests.post(f"{BASE_URL}/query", json=request_data)
    print_response(response)
    if response.status_code != 200:
        print("\n❌ La creación de la consulta falló. Abortando.")
        return
    
    consulta_id = response.json().get("consulta_id")
    if not consulta_id:
        print("\n❌ No se recibió un ID de consulta. Abortando.")
        return

    # --- 4. Monitorear el estado de la consulta ---
    print_separator(f"Paso 3: Monitoreando la consulta '{consulta_id}'")
    start_time = time.time()
    final_status = None
    while time.time() - start_time < TIMEOUT:
        response = requests.get(f"{BASE_URL}/query/{consulta_id}")
        if response.status_code == 200:
            data = response.json()
            estado = data.get("estado")
            progreso = data.get("progreso")
            mensaje = data.get("mensaje")
            print(f"-> Estado: {estado} | Progreso: {progreso}% | Mensaje: {mensaje}")

            if estado in ["completado", "error"]:
                final_status = estado
                break
        else:
            print(f"-> Error al obtener estado: {response.status_code}")
        
        time.sleep(POLL_INTERVAL)

    if not final_status:
        print("\n⏰ Timeout esperando la finalización de la consulta.")
        return

    # --- 5. Obtener los resultados finales ---
    if final_status == "completado":
        print_separator("Paso 4: Obteniendo resultados finales")
        response = requests.get(f"{BASE_URL}/query/{consulta_id}?resultados=True")
        print_response(response)
    else:
        print_separator("Consulta finalizada con error")
        print("No se pueden obtener resultados.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python api_client.py <ruta_al_archivo.json>")
        sys.exit(1)
    
    json_file = sys.argv[1]
    main(json_file)