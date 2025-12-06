import requests
import json
import time
import argparse
from typing import Dict
from pathlib import Path

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

def validar_solicitud_remota(session: requests.Session, base_url: str, json_file_path: str) -> bool:
    """
    Carga y valida una solicitud contra el endpoint /validate.
    Retorna True si es válida, False en caso contrario.
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
        return False
    except json.JSONDecodeError:
        print(f"❌ Error: El archivo '{json_file_path}' no contiene un JSON válido.")
        return False

    # --- 2. Validar la solicitud ---
    print_separator("Validando la solicitud contra el servidor")
    try:
        validate_url = f"{base_url}/validate"
        response = session.post(validate_url, json=request_data)
        print_response(response)
        return response.status_code == 200
    except requests.ConnectionError:
        print(f"❌ Error de conexión: No se pudo conectar a {base_url}. ¿Está el servidor corriendo?")
        return False

def iniciar_nueva_consulta(session: requests.Session, base_url: str, json_file_path: str) -> str | None:
    """
    Carga, valida y crea una nueva consulta, devolviendo su ID.
    Retorna el ID de la consulta o None si falla.
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
        return None
    except json.JSONDecodeError:
        print(f"❌ Error: El archivo '{json_file_path}' no contiene un JSON válido.")
        return None

    # --- 2. Validar la solicitud ---
    print_separator("Paso 1: Validando la solicitud")
    try:
        validate_url = f"{base_url}/validate"
        response = session.post(validate_url, json=request_data)
        print_response(response)
        if response.status_code != 200:
            print("\n❌ La validación falló. Abortando.")
            return None
    except requests.ConnectionError:
        print(f"❌ Error de conexión: No se pudo conectar a {base_url}. ¿Está el servidor corriendo?")
        return None

    # --- 3. Crear la consulta ---
    print_separator("Paso 2: Creando la consulta")
    query_url = f"{base_url}/query"
    response = session.post(query_url, json=request_data)
    print_response(response)
    if response.status_code not in (200, 202):
        print("\n❌ La creación de la consulta no fue aceptada por el servidor. Abortando.")
        return None

    # ID preferido desde el cuerpo; fallback a Location header
    consulta_id = None
    try:
        consulta_id = (response.json() or {}).get("consulta_id")
    except Exception:
        consulta_id = None
    if not consulta_id:
        loc = response.headers.get("Location")
        if loc and "/" in loc:
            consulta_id = loc.rstrip("/").split("/")[-1]
    if not consulta_id:
        print("\n❌ No se recibió un ID de consulta (ni en JSON ni en Location). Abortando.")
        return None
    
    # Mostrar Location si viene
    if response.headers.get("Location"):
        print(f"-> Location: {response.headers['Location']}")

    return consulta_id

def monitorear_consulta(session: requests.Session, base_url: str, consulta_id: str, timeout: int, poll_interval: int):
    """
    Monitorea el estado de una consulta hasta que se complete, falle o se agote el tiempo.
    """
    query_status_url = f"{base_url}/query/{consulta_id}"
    print_separator(f"Paso 3: Monitoreando la consulta '{consulta_id}'")
    start_time = time.time()
    final_status = None

    while time.time() - start_time < timeout:
        response = session.get(query_status_url)
        if response.status_code in (200, 202):
            data = response.json()
            estado = data.get("estado")
            progreso = data.get("progreso")
            mensaje = data.get("mensaje")
            print(f"-> Estado: {estado} | Progreso: {progreso}% | Mensaje: {mensaje}")
            if estado in ["completado", "error"]:
                final_status = estado
                break
            # Si el servidor sugiere Retry-After, respetarlo; si no, usar poll_interval
            retry_after = response.headers.get("Retry-After")
            try:
                wait_s = int(retry_after) if retry_after else poll_interval
            except ValueError:
                wait_s = poll_interval
            time.sleep(wait_s)
            continue
        else:
            print(f"-> Error al obtener estado: {response.status_code}")
        time.sleep(poll_interval)

    if not final_status:
        print("\n⏰ Timeout esperando la finalización de la consulta.")
        return

    # --- Obtener los resultados finales ---
    if final_status == "completado":
        print_separator("Paso 4: Obteniendo resultados finales")
        response = session.get(query_status_url, params={"resultados": "True"})
        print_response(response)
    else:
        print_separator("Consulta finalizada con error")
        print("No se pueden obtener resultados.")

def main(base_url: str, json_file: str | None, timeout: int, poll_interval: int, resume_id: str | None, validate_only: bool):
    """
    Función principal para ejecutar el cliente de la API.
    """
    with requests.Session() as session:
        # --- Modo: Solo Validar ---
        if validate_only:
            if not json_file:
                print("❌ Error: Se requiere el archivo JSON para la validación (--validate).")
                return
            validar_solicitud_remota(session, base_url, json_file)
            return

        # --- Modo: Reanudar Monitoreo ---
        if resume_id:
            monitorear_consulta(session, base_url, resume_id, timeout, poll_interval)
            return

        # --- Modo: Nueva Consulta (por defecto) ---
        if not json_file:
            print("❌ Error: Se requiere un archivo JSON para iniciar una nueva consulta.")
            print("Uso: python api_client.py <base_url> <json_file>")
            print("O use --resume <id> para monitorear una consulta existente.")
            return

        # Iniciar y luego monitorear
        consulta_id = iniciar_nueva_consulta(session, base_url, json_file)
        if consulta_id:
            monitorear_consulta(session, base_url, consulta_id, timeout, poll_interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cliente para la API de solicitudes históricas.")
    parser.add_argument("base_url", help="URL base de la API (ej. http://localhost:9041).")
    parser.add_argument("json_file", nargs='?', default=None, help="Ruta al archivo JSON de la solicitud (requerido si no se usa --resume).")
    parser.add_argument("--resume", type=str, default=None, help="ID de una consulta existente para reanudar el monitoreo.")
    parser.add_argument("--validate", action="store_true", help="Solo valida el archivo JSON contra el endpoint /validate y sale.")
    parser.add_argument("--timeout", type=int, default=600, help="Tiempo máximo de espera en segundos para la consulta.")
    parser.add_argument("--poll-interval", type=int, default=10, help="Intervalo en segundos entre cada sondeo de estado.")

    args = parser.parse_args()

    main(args.base_url, args.json_file, args.timeout, args.poll_interval, args.resume, args.validate)