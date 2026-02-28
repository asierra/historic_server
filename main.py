from fastapi import FastAPI, HTTPException, BackgroundTasks, Body, Request
from fastapi.responses import JSONResponse
from database import ConsultasDatabase, DATABASE_PATH
from background_simulator import BackgroundSimulator
from recover import RecoverFiles # Importar el procesador real
from processors import HistoricQueryProcessor
from schemas import HistoricQueryRequest
from datetime import datetime
from typing import Dict, Any, Tuple
import os
import re
from contextlib import asynccontextmanager
import logging
from logging.handlers import RotatingFileHandler
from pydantic import ValidationError
from config import SatelliteConfigGOES
import uvicorn
import os
import shutil
import secrets
import string
from settings import settings
from pebble import ProcessPool
from logging_config import setup_logging

# --- Configuración de Logging ---
setup_logging()
log = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global executor, db, processor, recover
    """
    Gestiona el ciclo de vida de la aplicación. El código antes del `yield`
    se ejecuta al iniciar, y el código después se ejecuta al apagar.
    """
    # Código de inicio
    log.info("🚀 Servidor iniciando...")
    
    # Inicializar componentes
    db = ConsultasDatabase(db_path=str(DB_PATH))
    processor = HistoricQueryProcessor()
    
    MAX_WORKERS = settings.max_workers
    executor = ProcessPool(max_workers=MAX_WORKERS)
    
    if PROCESSOR_MODE == "real":
        S3_FALLBACK_ENABLED = settings.s3_fallback_enabled
        recover = RecoverFiles(
            db=db,
            source_data_path=str(SOURCE_DATA_PATH),
            base_download_path=str(DOWNLOAD_PATH),
            executor=executor,
            s3_fallback_enabled=S3_FALLBACK_ENABLED,
            lustre_enabled=settings.lustre_enabled,
            file_processing_timeout_seconds=settings.file_processing_timeout_seconds
        )
    else:
        recover = BackgroundSimulator(db)

    yield
    # Código de apagado
    log.info("⏳ Servidor recibiendo señal de apagado...")
    if executor:
        log.info("   Esperando a que las tareas de fondo se completen...")
        executor.close()
        executor.join()
    log.info("✅ Todas las tareas de fondo han finalizado. Servidor apagado.")

app = FastAPI(
    title="LANOT Historic Server",
    description="API para solicitudes de datos históricos del LANOT",
    version="1.0.0",
    lifespan=lifespan
)

# --- Seguridad opcional con API Key ---
API_KEY = settings.api_key

def _require_api_key(request: Request):
    if not API_KEY:
        return  # No protegido si no se configura
    provided = request.headers.get("X-API-Key")
    if provided != API_KEY:
        raise HTTPException(status_code=401, detail="API key inválida o ausente.")

# Registro de configuraciones de satélites disponibles
# A medida que agregues soporte para más satélites, importa su config y añádela aquí.
AVAILABLE_SATELLITE_CONFIGS = {
    "GOES": SatelliteConfigGOES(),
}

# --- Configuración y Componentes Dinámicos ---

# Usar variables de entorno para configurar rutas clave
DB_PATH = settings.db_path
SOURCE_DATA_PATH = settings.source_path
DOWNLOAD_PATH = settings.download_path

# --- Límites de consulta y disco ---
MAX_FILES_PER_QUERY = settings.max_files_per_query
MAX_SIZE_MB_PER_QUERY = settings.max_size_mb_per_query
MIN_FREE_SPACE_GB_BUFFER = settings.min_free_space_gb_buffer

# Selección del procesador de background mediante variable de entorno
PROCESSOR_MODE = settings.processor_mode

# Variables globales (se inicializan en lifespan)
executor = None
db = None
processor = None
recover = None


def generar_id_consulta() -> str:
    return ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(8))

@app.get("/")
async def health_check():
    """Health check"""
    return {"status": "active", "timestamp": datetime.now().isoformat()}

@app.get("/health")
async def health_check_detailed():
    """
    Verifica la salud de la aplicación y sus dependencias clave.
    """
    db_status = "ok"
    storage_status = "ok"
    overall_status = "ok"

    # 1. Verificar la conexión a la base de datos
    try:
        db.listar_consultas(limite=1) # Intenta una operación simple
    except Exception as e:
        db_status = f"error: {e}"
        overall_status = "error"

    # 2. Verificar que el almacenamiento primario (Lustre) esté accesible
    if not os.path.exists(SOURCE_DATA_PATH):
        storage_status = f"error: La ruta de origen '{SOURCE_DATA_PATH}' no existe o no es accesible."
        overall_status = "error"

    # 3. Reportar estado de Lustre y S3
    lustre_status = getattr(recover, "lustre_enabled", None)
    s3_status = getattr(recover, "s3_fallback_enabled", None)
    if lustre_status is None:
        lustre_status = False
    if s3_status is None:
        s3_status = False

    status_code = 200 if overall_status == "ok" else 503 # Service Unavailable

    return {
        "status": overall_status,
        "database": db_status,
        "storage": storage_status,
        "lustre_enabled": lustre_status,
        "s3_enabled": s3_status,
        "timestamp": datetime.now().isoformat()
    }


def _validate_and_prepare_request(request_data: Dict[str, Any]) -> Tuple[Dict[str, Any], Any]:
    """
    Función de ayuda reutilizable para validar y preparar una solicitud.
    Levanta HTTPException en caso de error.
    Devuelve (datos_validados, clase_de_configuracion).
    """
    # 1. Validar la estructura básica con Pydantic
    try:
        request = HistoricQueryRequest(**request_data)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors())

    # 2. Determinar la configuración correcta
    sat_name = request.sat or AVAILABLE_SATELLITE_CONFIGS["GOES"].DEFAULT_SATELLITE
    config = None
    if sat_name.startswith("GOES"):
        config = AVAILABLE_SATELLITE_CONFIGS["GOES"]

    if not config:
        raise HTTPException(status_code=400, detail=f"Satélite '{sat_name}' no es soportado o es inválido.")

    # 2.1. Validar fecha futura ANTES de procesar
    today = datetime.now().date()
    for fecha_str in request_data.get('fechas', {}).keys():
        fecha_a_validar_str = fecha_str.split('-')[-1]
        try:
            fecha_a_validar = datetime.strptime(fecha_a_validar_str, "%Y%m%d").date()
            if fecha_a_validar > today:
                raise HTTPException(status_code=400, detail=f"La fecha '{fecha_a_validar.strftime('%Y-%m-%d')}' está en el futuro y no es válida.")
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Formato de fecha inválido en la clave: '{fecha_str}'. Se esperaba 'YYYYMMDD' o 'YYYYMMDD-YYYYMMDD'.")

    # 3. Defaults
    data = request.model_dump()
    data['sat'] = sat_name
    data['sensor'] = request.sensor or config.DEFAULT_SENSOR
    data['nivel'] = request.nivel or config.DEFAULT_LEVEL

    # Lógica condicional para las bandas (CORREGIDA):
    nivel_upper = (data['nivel'] or '').upper()
    productos_upper = [str(p).strip().upper() for p in (request.productos or [])]
    is_cmi_product = any(p.startswith('CMI') for p in productos_upper)  # acepta CMIP, CMIPC, CMI, etc.
    tiene_all_productos = 'ALL' in productos_upper
    
    # L1b siempre requiere bandas
    # L2 requiere bandas si: tiene productos CMI, O tiene productos='ALL'
    requiere_bandas = (nivel_upper == 'L1B') or (nivel_upper == 'L2' and (is_cmi_product or tiene_all_productos))

    if requiere_bandas:
        data['bandas'] = request.bandas or config.DEFAULT_BANDAS
    else:
        # L2 sin productos CMI ni ALL: no exigir bandas
        data['bandas'] = []

    # 3.1 Validaciones de lógica de negocio (satélite, sensor, bandas, etc.)
    try:
        if not config.is_valid_satellite(data['sat']):
            raise ValueError(f"Satélite debe ser uno de: {config.VALID_SATELLITES}")
        if not config.is_valid_sensor(data['sensor']):
            raise ValueError(f"Sensor debe ser uno de: {config.VALID_SENSORS}")
        if not config.is_valid_level(data['nivel']):
            raise ValueError(f"Nivel debe ser uno de: {config.VALID_LEVELS}")
        if not config.is_valid_domain(data['dominio']):
            raise ValueError(f"Dominio debe ser uno de: {config.VALID_DOMAINS}")
        if requiere_bandas:
            data['bandas'] = config.validate_bandas(data['bandas'])
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # --- Validaciones de límites y espacio en disco ---
    # Se realizan después de la validación de Pydantic y la preparación de datos.

    # 4. Estimar archivos y tamaño para las validaciones
    estimation_summary = config.estimate_files_summary(data)
    archivos_estimados = estimation_summary["file_count"]
    tamanio_estimado_mb = estimation_summary["total_size_mb"]

    # 5. Validar contra límites de la consulta
    if MAX_FILES_PER_QUERY > 0 and archivos_estimados > MAX_FILES_PER_QUERY:
        raise HTTPException(
            status_code=413, # Payload Too Large
            detail=f"La consulta excede el límite de archivos permitidos ({archivos_estimados} estimados vs {MAX_FILES_PER_QUERY} máximo)."
        )

    if MAX_SIZE_MB_PER_QUERY > 0 and tamanio_estimado_mb > MAX_SIZE_MB_PER_QUERY:
        raise HTTPException(
            status_code=413,
            detail=f"La consulta excede el límite de tamaño permitido ({tamanio_estimado_mb:.2f} MB estimados vs {MAX_SIZE_MB_PER_QUERY} MB máximo)."
        )

    # 6. Validar espacio en disco disponible
    try:
        disk_usage = shutil.disk_usage(DOWNLOAD_PATH)
        free_space_mb = disk_usage.free / (1024 * 1024)
        buffer_mb = MIN_FREE_SPACE_GB_BUFFER * 1024

        if (free_space_mb - tamanio_estimado_mb) < buffer_mb:
            raise HTTPException(
                status_code=507, # Insufficient Storage
                detail=f"Espacio en disco insuficiente. Se requieren {tamanio_estimado_mb:.2f} MB pero solo hay {free_space_mb:.2f} MB libres (considerando un búfer de {MIN_FREE_SPACE_GB_BUFFER} GB)."
            )
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail=f"El directorio de descargas '{DOWNLOAD_PATH}' no existe.")

    return data, config

@app.post("/query")
async def crear_solicitud(
    background_tasks: BackgroundTasks,
    request_data: Dict[str, Any] = Body(...),
):
    """
    ✅ ENDPOINT PRINCIPAL: Crear y procesar solicitud
    """
    try:
        # 1. Validar y preparar la solicitud usando la función de ayuda
        #    Esta función ahora incluye las validaciones de límites y espacio.
        data, config = _validate_and_prepare_request(request_data)
        # 2. Procesar la solicitud ya validada y completada
        query_obj = processor.procesar_request(data, config)
        query_dict = query_obj.to_dict()
        
        consulta_id = str(request_data.get('id') or '').strip() or generar_id_consulta()
        
        if not db.crear_consulta(consulta_id, query_dict):
            raise HTTPException(status_code=500, detail="Error almacenando consulta")
        
        # Procesar en background
        background_tasks.add_task(recover.procesar_consulta, consulta_id, query_dict)
        
        body = {
            "success": True,
            "consulta_id": consulta_id,
            "estado": "recibido",
            "resumen": {
                "satelite": query_dict['satelite'],
                "sensor": query_dict['sensor'],
                "nivel": query_dict['nivel'],
                "fechas": len(query_dict['fechas']),
                "horas": query_dict['total_horas']
            }
        }
        return JSONResponse(content=body, status_code=202, headers={"Location": f"/query/{consulta_id}"})
        
    except HTTPException as e:
        # Relanzar excepciones HTTP (como 413 o 507 de la validación)
        raise e
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/validate")
async def validar_solicitud(request_data: Dict[str, Any] = Body(...)):
    """
    ✅ ENDPOINT DE VALIDACIÓN: Valida el JSON de entrada sin crear una consulta.
    Devuelve un resumen si es válido, o un error detallado si no lo es.
    """
    try:
        # 1. Validar y preparar la solicitud usando la función de ayuda
        #    Esta función ahora incluye las validaciones de límites y espacio.
        data, config = _validate_and_prepare_request(request_data)

        # 2. Estimar archivos y tamaño usando el método completo de la config
        estimation_summary = config.estimate_files_summary(data)

        return {
            "success": True,
            "message": "La solicitud es válida.",
            "archivos_estimados": estimation_summary["file_count"],
            "tamanio_estimado_mb": estimation_summary["total_size_mb"]
        }
    except HTTPException as e:
        # Relanzar excepciones HTTP que ya vienen preparadas (ej. 422, 413, 507, etc.)
        raise e

@app.post("/query/{consulta_id}/restart")
async def reiniciar_consulta(consulta_id: str, background_tasks: BackgroundTasks, request: Request):
    """
    ✅ ENDPOINT DE RECUPERACIÓN: Reinicia una consulta que se quedó atascada.
    Busca una consulta existente y la vuelve a encolar para su procesamiento.
    Es útil si el servidor se reinició o un proceso de fondo falló.
    """
    _require_api_key(request)

    consulta = db.obtener_consulta(consulta_id)
    if not consulta:
        raise HTTPException(status_code=404, detail="Consulta no encontrada.")

    # Permitir reiniciar consultas en proceso, con error, o completadas (para forzar reprocesamiento).
    if consulta["estado"] not in ["procesando", "error", "completado"]:
        raise HTTPException(
            status_code=400,
            detail=f"No se puede reiniciar una consulta en estado '{consulta['estado']}'. Solo se permiten 'procesando', 'error' o 'completado'."
        )

    # Resetear estado en DB antes de encolar para que el cliente no vea estado obsoleto
    db.actualizar_estado(consulta_id, "recibido", progreso=0, mensaje="Consulta reenviada para procesamiento")

    # Volver a encolar la tarea usando la query original guardada en la DB
    query_dict = consulta["query"]
    background_tasks.add_task(recover.procesar_consulta, consulta_id, query_dict)

    body = {
        "success": True,
        "message": f"La consulta '{consulta_id}' ha sido reenviada para su procesamiento."
    }
    return JSONResponse(content=body, status_code=202, headers={"Location": f"/query/{consulta_id}"})

@app.get("/query/{consulta_id}")
async def obtener_consulta(
    consulta_id: str,
    resultados: bool = False,
):
    """
    ✅ ENDPOINT ÚNICO PARA CONSULTAR: Estado y resultados
    Reemplaza a: /api/query/{id}, /api/query/{id}/resultados, /api/queries
    """
    consulta = db.obtener_consulta(consulta_id)
    if not consulta:
        raise HTTPException(status_code=404, detail="Consulta no encontrada")
    
    # Si se piden resultados específicos y la consulta está completada
    if resultados and consulta["estado"] == "completado" and consulta.get("resultados"):
        return {
            "consulta_id": consulta_id,
            "estado": "completado",
            "resultados": consulta["resultados"]
        }
    
    # Estado normal de la consulta (omitimos 'query' si no aplica)
    resp = {
        "consulta_id": consulta_id,
        "estado": consulta["estado"],
        "progreso": consulta["progreso"],
        "mensaje": consulta["mensaje"],
        "timestamp": consulta["timestamp_actualizacion"],
    }
    if consulta["estado"] == "recibido":
        resp["query"] = consulta["query"]
    
    # Si está completado, enriquecer la respuesta con los totales del reporte
    if consulta["estado"] == "completado" and consulta.get("resultados"):
        resultados_data = consulta["resultados"]
        fuentes = resultados_data.get("fuentes", {})
        lustre_info = fuentes.get("lustre", {})
        s3_info = fuentes.get("s3", {})
        
        resp["total_archivos"] = resultados_data.get("total_archivos", 0)
        resp["archivos_lustre"] = lustre_info.get("total", 0)
        resp["archivos_s3"] = s3_info.get("total", 0)

    # --- Enriquecer siempre la respuesta con ruta y tamaño (si está completado) ---
    try:
        dest_dir = os.path.join(DOWNLOAD_PATH, consulta_id)
        resp["ruta_destino"] = dest_dir
        resp["total_mb"] = None  # Por defecto es null

        if consulta["estado"] == "completado" and consulta.get("resultados"):
            resultados_data = consulta["resultados"]
            resp["total_mb"] = resultados_data.get("total_mb", 0)

        # Derivar etapa a partir del mensaje para dar más contexto
        msg = (consulta.get("mensaje") or "").lower()
        if "preparando entorno" in msg:
            etapa = "preparando"
        elif "identificados" in msg or "procesando archivo" in msg or "recuperando archivo" in msg:
            etapa = "recuperando-local"
        elif "descargas s3 pendientes" in msg:
            etapa = "s3-listado"
        elif "descargando de s3" in msg or "descarga s3" in msg:
            etapa = "s3-descargando"
        elif "reporte final" in msg:
            etapa = "finalizando"
        elif consulta["estado"] in ["completado", "error"]:
            etapa = consulta["estado"]
        else:
            etapa = "desconocida"
        resp["etapa"] = etapa

    except Exception:
        # No bloquear la respuesta si hay errores leyendo el FS
        resp["ruta_destino"] = None
        resp["total_mb"] = None
        resp["etapa"] = "error_lectura_fs"

    # Decidir código de estado según estado de la consulta
    estado = consulta["estado"]
    if estado == "completado":
        return JSONResponse(content=resp, status_code=200)
    elif estado in ("procesando", "recibido"):
        return JSONResponse(content=resp, status_code=202, headers={"Retry-After": "10"})
    elif estado == "error":
        return JSONResponse(content=resp, status_code=500)
    else:
        # Estado desconocido: devolver 200 con payload para no romper clientes
        return JSONResponse(content=resp, status_code=200)

@app.get("/queries")
async def listar_consultas(
    estado: str = None,
    limite: int = 20,
):
    """
    ✅ LISTADO SIMPLE: Para monitoreo
    """
    consultas = db.listar_consultas(estado=estado, limite=limite)
    
    # Formato mínimo para listado
    consultas_simples = []
    for c in consultas:
        consultas_simples.append({
            "id": c["id"],
            "estado": c["estado"],
            "progreso": c["progreso"],
            "satelite": c["query"]["satelite"],
            "timestamp": c["timestamp_creacion"]
        })
    
    return {
        "total": len(consultas_simples),
        "consultas": consultas_simples
    }

@app.delete("/query/{consulta_id}")
async def eliminar_consulta(request: Request, consulta_id: str, purge: bool = False, force: bool = False):
    """
    Elimina una consulta de la base de datos. Opcionalmente purga el directorio de trabajo.
    - purge=true para eliminar / purgar el directorio de archivos asociado a la consulta.
    - force=true para permitir purge aunque la consulta esté en estado 'procesando'.
    """
    _require_api_key(request)

    consulta = db.obtener_consulta(consulta_id)

    # Purga opcional del directorio asociado a la consulta
    if purge:
        # Bloquear purga si está procesando y no se forzó
        if consulta and (consulta.get("estado") == "procesando") and not force:
            raise HTTPException(
                status_code=409,
                detail="La consulta está en proceso; use force=true para purgar de todas formas."
            )
        try:
            dest_dir = os.path.join(DOWNLOAD_PATH, consulta_id)
            base = os.path.abspath(DOWNLOAD_PATH)
            target = os.path.abspath(dest_dir)
            if not target.startswith(base + os.sep) and target != base:
                raise HTTPException(status_code=400, detail="Ruta de destino inválida para purge.")
            if os.path.isdir(target):
                shutil.rmtree(target)
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error purgando directorio: {e}")

    # Eliminar registro en DB (si existe)
    ok = db.eliminar_consulta(consulta_id)
    if not ok and not purge:
        # Si no se solicitó purge y no hay registro en DB, devolver 404
        raise HTTPException(status_code=404, detail="Consulta no encontrada o ya eliminada.")

    # Mensaje consolidado
    partes = []
    partes.append("Registro de consulta eliminado." if ok else "Registro de consulta no encontrado.")
    if purge:
        partes.append("Directorio purgado.")
    return {"success": True, "message": " ".join(partes)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9041)
