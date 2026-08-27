from fastapi import FastAPI, HTTPException, Body, Request
from fastapi.responses import JSONResponse
from database import ConsultasDatabase, DATABASE_PATH
from background_simulator import BackgroundSimulator
from recover import RecoverFiles  # Importar el procesador real
from s3_recover import _s3_circuit_breaker
from processors import HistoricQueryProcessor
from schemas import HistoricQueryRequest
from datetime import datetime
from typing import Dict, Any, Tuple
import os
import re
import uuid
from contextlib import asynccontextmanager
import logging
import structlog
from pydantic import ValidationError
from esquema import errores_de_esquema
from config import SatelliteConfigGOES
import uvicorn
import shutil
import secrets
import string
from settings import settings
from pebble import ProcessPool
from cola import BucleDeCola
from logging_config import setup_logging

# --- Configuración de Logging ---
setup_logging()
log = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global executor, db, processor, recover, bucle
    """
    Gestiona el ciclo de vida de la aplicación. El código antes del `yield`
    se ejecuta al iniciar, y el código después se ejecuta al apagar.
    """
    # Código de inicio
    log.info("🚀 Servidor iniciando...")
    
    # Inicializar componentes
    # El lease se le pasa a la base porque quien lo renueva es
    # `actualizar_estado`, llamado desde dentro del pipeline.
    db = ConsultasDatabase(db_path=str(DB_PATH), lease_s=settings.queue_lease_s)
    processor = HistoricQueryProcessor()
    
    MAX_WORKERS = settings.max_workers
    executor = ProcessPool(max_workers=MAX_WORKERS)
    
    if PROCESSOR_MODE == "real":
        S3_ENABLED = settings.s3_enabled

        # Avisar en el journal si se arranca con un origen apagado. Es una
        # degradación silenciosa: el servicio funciona, sólo que más lento y
        # con menos cobertura, y no hay ninguna otra señal de que se está
        # trabajando a medias. Pasó en agosto de 2026, cuando una
        # actualización de kernel dejó a Lustre sin montar en tahan y el
        # servicio siguió sirviendo de S3 sin decir nada.
        if not settings.lustre_enabled:
            log.warning(
                "⚠️  Lustre DESHABILITADO: todo se recuperará de S3, más lento "
                "y sin lo que no esté en el bucket público. Si no es intencional, "
                "revisa LUSTRE_ENABLED y que %s esté montado.", SOURCE_DATA_PATH,
            )
        elif not os.path.exists(SOURCE_DATA_PATH):
            log.error(
                "❌ Lustre habilitado pero %s no existe: probablemente no está "
                "montado. Las consultas caerán a S3 archivo por archivo.",
                SOURCE_DATA_PATH,
            )
        if not S3_ENABLED:
            log.warning("⚠️  S3 DESHABILITADO: sólo se recuperará de Lustre.")

        recover = RecoverFiles(
            db=db,
            source_data_path=str(SOURCE_DATA_PATH),
            base_download_path=str(DOWNLOAD_PATH),
            executor=executor,
            s3_enabled=S3_ENABLED,
            lustre_enabled=settings.lustre_enabled,
            file_processing_timeout_seconds=settings.file_processing_timeout_seconds
        )
    else:
        recover = BackgroundSimulator(db)

    # El consumidor de la cola. Uno por proceso de gunicorn; el reclamo es
    # atómico, así que compiten sin pisarse (PLAN_COLA_DURABLE.md §5-2.1).
    bucle = BucleDeCola(
        db=db,
        recover=recover,
        poll_s=settings.queue_poll_s,
        lease_s=settings.queue_lease_s,
    )
    bucle.arrancar()

    yield
    # Código de apagado
    log.info("⏳ Servidor recibiendo señal de apagado...")
    # Primero el bucle, para que no reclame nada mientras se cierra el pool.
    # No suelta lo que tenga en curso a propósito: ver BucleDeCola.parar().
    if bucle:
        bucle.parar()
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


@app.middleware("http")
async def correlation_id_middleware(request: Request, call_next):
    """
    Middleware de trazabilidad distribuida.
    Lee el header X-Request-ID entrante o genera un UUID nuevo,
    lo inyecta en el contexto de structlog y lo propaga en la respuesta.
    """
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    structlog.contextvars.bind_contextvars(request_id=request_id)
    try:
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response
    finally:
        structlog.contextvars.unbind_contextvars("request_id")


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

# --- Estados con trabajo en vuelo ---
# 'recibido' = encolada y disponible para que un consumidor la reclame;
# 'procesando' = alguien la tiene, con el lease vivo.
#
# Los dos siguen protegiendo el directorio frente a un purge: 'procesando' porque
# hay alguien escribiendo ahora mismo, y 'recibido' porque el bucle puede
# reclamarla en cualquier momento y recrear lo que se acabe de borrar.
ESTADOS_EN_VUELO = ("recibido", "procesando")
ESTADOS_REINICIABLES = ESTADOS_EN_VUELO + ("error", "completado")

# Selección del procesador de background mediante variable de entorno
PROCESSOR_MODE = settings.processor_mode

# Variables globales (se inicializan en lifespan)
executor = None
db = None
processor = None
recover = None
bucle = None  # BucleDeCola: el consumidor de la cola de este proceso


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

    # 2. Qué orígenes están habilitados. Va antes que la comprobación de disco
    #    porque decide si esa comprobación significa algo.
    lustre_status = getattr(recover, "lustre_enabled", None)
    s3_status = getattr(recover, "s3_enabled", None)
    if lustre_status is None:
        lustre_status = False
    if s3_status is None:
        s3_status = False

    # 3. Verificar el almacenamiento primario, **sólo si se va a usar**.
    #    Comprobarlo incondicionalmente dejaba /health en 503 permanente en
    #    cualquier despliegue con LUSTRE_ENABLED=false y la ruta sin montar,
    #    que es el caso de tahan. Llevaba así desde 2622b63 (sep-2025), o sea
    #    que la señal estaba en rojo fijo y nadie la leía — el mismo patrón que
    #    tuvo el CI. Una ruta que el servicio no va a tocar no dice nada sobre
    #    su salud, y un 503 permanente ahoga a los que sí importan.
    if not lustre_status:
        storage_status = "no aplica: Lustre deshabilitado"
    elif not os.path.exists(SOURCE_DATA_PATH):
        storage_status = f"error: La ruta de origen '{SOURCE_DATA_PATH}' no existe o no es accesible."
        overall_status = "error"

    # Sin ningún origen no hay servicio que dar, aunque cada pieza esté sana.
    if not lustre_status and not s3_status:
        storage_status = "error: ni Lustre ni S3 están habilitados; no hay de dónde recuperar."
        overall_status = "error"

    # 4. La cola. Es el único fallo de esta arquitectura que no se ve desde
    #    fuera: si el hilo muere, la API sigue aceptando consultas y
    #    respondiendo 202 mientras la cola crece sin que nadie la drene.
    cola_status = {
        "hilo_vivo": bool(bucle and bucle.vivo),
        "worker_id": getattr(bucle, "worker_id", None),
        "consulta_en_curso": getattr(bucle, "consulta_en_curso", None),
    }
    try:
        por_estado = db.contar_por_estado()
        cola_status["encoladas"] = por_estado.get("recibido", 0)
        cola_status["procesando"] = por_estado.get("procesando", 0)
    except Exception as e:
        cola_status["error"] = str(e)

    # Un hilo muerto no tumba /health por sí solo —el proceso sigue sirviendo—
    # pero con trabajo encolado y nadie que lo coja, el servicio no está sano.
    if not cola_status["hilo_vivo"] and cola_status.get("encoladas"):
        overall_status = "error"

    # JSONResponse y no un dict pelado: el `status_code` se calculaba y se
    # tiraba, así que /health respondía 200 siempre —incluso con la base
    # caída— y el 503 que documenta el README no se ha enviado nunca. Un
    # monitor que mirara el código HTTP llevaba todo este tiempo viendo el
    # servicio sano pasara lo que pasara.
    status_code = 200 if overall_status == "ok" else 503 # Service Unavailable

    return JSONResponse(status_code=status_code, content={
        "status": overall_status,
        "database": db_status,
        "storage": storage_status,
        "cola": cola_status,
        "lustre_enabled": lustre_status,
        "s3_enabled": s3_status,
        "s3_circuit_breaker": {
            "state": _s3_circuit_breaker.state,
            "failures": _s3_circuit_breaker._failures,
            "failure_threshold": _s3_circuit_breaker.failure_threshold,
            "recovery_timeout_s": _s3_circuit_breaker.recovery_timeout,
        },
        "timestamp": datetime.now().isoformat()
    })


def _validate_and_prepare_request(request_data: Dict[str, Any]) -> Tuple[Dict[str, Any], Any]:
    """
    Función de ayuda reutilizable para validar y preparar una solicitud.
    Levanta HTTPException en caso de error.
    Devuelve (datos_validados, clase_de_configuracion).
    """
    # 1. Validar contra el contrato. historic_query_schema.json era hasta ahora un
    #    documento que nada aplicaba: lo unico que corria era el modelo Pydantic
    #    de abajo, mas laxo (fechas: Dict[str, List[str]] no valida ni las claves
    #    ni los horarios, asi que una clave como '-20260101' pasaba entera).
    problemas = errores_de_esquema(request_data)
    if problemas:
        raise HTTPException(status_code=422, detail=problemas)

    # 2. Estructura tipada para el resto de la funcion. Pydantic sigue siendo mas
    #    laxo que el esquema en 'nivel' y mas estricto en 'dominio'; alinearlo
    #    toca justo los dos campos que cambia el soporte de satelites polares.
    try:
        request = HistoricQueryRequest(**request_data)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors())

    # 3. Determinar la configuración correcta
    sat_name = request.sat or AVAILABLE_SATELLITE_CONFIGS["GOES"].DEFAULT_SATELLITE
    config = None
    if sat_name.startswith("GOES"):
        config = AVAILABLE_SATELLITE_CONFIGS["GOES"]

    if not config:
        raise HTTPException(status_code=400, detail=f"Satélite '{sat_name}' no es soportado o es inválido.")

    # 3.1. Validar fecha futura ANTES de procesar
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
    request: Request,
    request_data: Dict[str, Any] = Body(...),
):
    """
    ✅ ENDPOINT PRINCIPAL: Crear y procesar solicitud
    """
    # Protegido como /restart y DELETE: es el endpoint que compromete recursos
    # —encola descargas que han llegado a cientos de GB— así que dejarlo abierto
    # mientras los otros dos piden clave era una asimetría, no una decisión.
    # historic_query manda la clave en todas sus llamadas (un único helper,
    # `call_api`), así que esto no le cambia nada.
    _require_api_key(request)

    try:
        # 1. Validar y preparar la solicitud usando la función de ayuda
        #    Esta función ahora incluye las validaciones de límites y espacio.
        data, config = _validate_and_prepare_request(request_data)
        # 2. Procesar la solicitud ya validada y completada
        query_obj = processor.procesar_request(data, config)
        query_dict = query_obj.to_dict()
        
        consulta_id = str(request_data.get('id') or '').strip() or generar_id_consulta()
        
        if not db.crear_consulta(consulta_id, query_dict):
            raise HTTPException(status_code=409, detail=f"La consulta '{consulta_id}' ya existe. Use un ID diferente o elimine la consulta existente.")
        
        # No se lanza nada aquí: el INSERT ya deja la consulta encolada en
        # 'recibido' y el bucle la recogerá. Ésa es toda la diferencia — antes
        # el trabajo vivía en la memoria de este proceso y un reinicio se lo
        # llevaba; ahora vive en la fila.
        
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
async def reiniciar_consulta(consulta_id: str, request: Request):
    """
    ✅ ENDPOINT DE RECUPERACIÓN: Reinicia una consulta que se quedó atascada.
    Busca una consulta existente y la vuelve a encolar para su procesamiento.
    Es útil si el servidor se reinició o un proceso de fondo falló.
    """
    _require_api_key(request)

    consulta = db.obtener_consulta(consulta_id)
    if not consulta:
        raise HTTPException(status_code=404, detail="Consulta no encontrada.")

    # Permitir reiniciar consultas encoladas, en proceso, con error, o completadas
    # (estas últimas para forzar reprocesamiento). 'recibido' entra aquí a propósito:
    # es justo el estado en que queda una consulta cuya tarea de fondo nunca arrancó,
    # que es el caso de uso para el que existe este endpoint.
    if consulta["estado"] not in ESTADOS_REINICIABLES:
        raise HTTPException(
            status_code=400,
            detail=(
                f"No se puede reiniciar una consulta en estado '{consulta['estado']}'. "
                f"Solo se permiten {', '.join(repr(e) for e in ESTADOS_REINICIABLES)}."
            )
        )

    # Devolverla a la cola. `reencolar` se niega si alguien la tiene con el
    # lease vivo, que es el mismo cerrojo de antes con mejor señal: el lease lo
    # renueva el pipeline en cada avance, así que estar vivo significa estar
    # avanzando. Sin esto, reiniciar una consulta que se está descargando la
    # pondría a disposición de otro consumidor y acabarían los dos escribiendo
    # en el mismo directorio.
    if not db.reencolar(consulta_id):
        raise HTTPException(
            status_code=409,
            detail=(
                f"La consulta '{consulta_id}' ya se está procesando. Espera a que "
                "termine o falle antes de reiniciarla."
            )
        )

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
        elif "identificados" in msg or "recuperado archivo" in msg or "falla" in msg or "lustre" in msg:
            etapa = "recuperando-local"
        elif "buscando archivos adicionales en s3" in msg or "descargas s3 pendientes" in msg:
            etapa = "s3-listado"
        elif "s3 progreso" in msg or "descargando de s3" in msg or "descarga s3" in msg:
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
    - force=true para permitir purge aunque la consulta esté en estado 'recibido' o 'procesando'.
    """
    _require_api_key(request)

    consulta = db.obtener_consulta(consulta_id)

    # Purga opcional del directorio asociado a la consulta
    if purge:
        # Bloquear purga si hay trabajo en vuelo y no se forzó. Incluye 'recibido':
        # la tarea puede arrancar en cualquier momento y recrear el directorio que
        # acabamos de borrar, dejando archivos huérfanos sin registro en la DB.
        if consulta and (consulta.get("estado") in ESTADOS_EN_VUELO) and not force:
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
