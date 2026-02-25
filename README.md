# historic_server

API para la recuperación de datos históricos de satélite desde el sistema de almacenamiento masivo del LANOT.

## Objetivo:

Permitir que usuarios y sistemas externos puedan generar consultas complejas de datos históricos de satélite de manera asíncrona. La API valida las solicitudes, las procesa en segundo plano y permite monitorear el progreso hasta obtener los resultados, que incluyen los archivos recuperados y un reporte detallado.

## Características Principales

*   **Procesamiento Asíncrono**: Las solicitudes se procesan en segundo plano, permitiendo manejar consultas de larga duración sin bloquear al cliente.
*   **Paralelismo Robusto**: Utiliza un pool de procesos (`pebble.ProcessPool`) para aislar las tareas de E/S, previniendo que un error en un archivo detenga todo el lote. Incluye un mecanismo de apagado seguro (`graceful shutdown`).
*   **Sistema de Almacenamiento Dual**:
    1.  Busca y recupera archivos eficientemente desde un sistema de archivos primario de alto rendimiento (como **Lustre**).
    2.  Implementa un mecanismo de **fallback a S3** (NOAA GOES Bucket) para recuperar archivos que no se encuentren localmente.
*   **Extracción Inteligente**: Es capaz de copiar archivos `.tgz` completos o extraer selectivamente su contenido (`.nc`) según los parámetros de la solicitud, optimizando el uso de disco.
*   **Robustez y Recuperación**:
    *   Mecanismos de reintento con backoff exponencial para descargas de S3.
    *   Timeouts configurables para el procesamiento de archivos, evitando procesos "zombie".
    *   Capacidad de reiniciar consultas fallidas o atascadas a través de un endpoint (`/query/{id}/restart`).
    *   Genera una `consulta_recuperacion` para los archivos que no se pudieron encontrar, facilitando reintentos manuales.
*   **Reportes Detallados**: Al finalizar, genera un reporte en formato JSON que distingue los archivos recuperados desde el almacenamiento local y los descargados de S3, y provee una consulta de recuperación para los archivos que no se pudieron encontrar.
*   **Validación Avanzada**: Sistema de validación extensible basado en Pydantic y clases de configuración por satélite.
*   **Modo de Simulación**: Incluye un modo `simulador` para desarrollo y pruebas sin necesidad de acceder al sistema de archivos real, con tasas de éxito configurables.
*   **Monitoreo de Salud**: Endpoint `/health` que verifica el estado de la base de datos y la accesibilidad al almacenamiento primario.
*   **Filtrado Preciso por Hora y Minuto**: Permite solicitar archivos dentro de intervalos específicos de hora y minuto, tanto en almacenamiento local como en S3.
*   **Consultas Multi-Producto**: Soporta solicitudes que incluyan varias bandas y productos L2 en una sola consulta.

## Configuración

La aplicación se configura mediante variables de entorno:

| Variable                        | Descripción                                                              | Valor por defecto   |
|---------------------------------|--------------------------------------------------------------------------|---------------------|
| `DB_PATH`                       | Ruta al archivo SQLite                                                   | `consultas_goes.db` |
| `DOWNLOAD_PATH`                 | Directorio de descargas por consulta                                     | `/data/tmp`         |
| `FILE_PROCESSING_TIMEOUT_SECONDS` | Tiempo máximo por archivo (segundos)                                     | `120`               |
| `LUSTRE_ENABLED`                | Habilita o deshabilita el uso de Lustre (true/false, 1/0)               | `true`              |
| `MAX_FILES_PER_QUERY`           | Límite de archivos estimados por consulta (0 = sin límite)               | `0`                 |
| `MAX_SIZE_MB_PER_QUERY`         | Límite de tamaño estimado en MB por consulta (0 = sin límite)            | `0`                 |
| `MAX_WORKERS`                   | Número de procesos para E/S paralela                                    | `8`                 |
| `MIN_FREE_SPACE_GB_BUFFER`      | Búfer de seguridad en GB que debe quedar libre en disco                  | `10`                |
| `PROCESSOR_MODE`                | Modo del procesador de fondo: real o simulador                          | `real`              |
| `S3_CONNECT_TIMEOUT`            | Timeout de conexión para S3 (segundos)                                   | `5`                 |
| `S3_FALLBACK_ENABLED`           | Habilita o deshabilita el fallback a S3 (true/false, 1/0)               | `true`              |
| `S3_PROGRESS_STEP`              | Actualizar progreso de descarga S3 cada N archivos                       | `100`               |
| `S3_READ_TIMEOUT`               | Timeout de lectura para S3 (segundos)                                    | `30`                |
| `S3_RETRY_ATTEMPTS`             | Número de reintentos para operaciones S3                                 | `3`                 |
| `S3_RETRY_BACKOFF_SECONDS`      | Factor de backoff para reintentos S3 (segundos)                          | `1.0`               |
| `SIM_LOCAL_SUCCESS_RATE`        | Tasa de éxito local en modo simulador (0.0–1.0)                          | `0.8`               |
| `SIM_S3_SUCCESS_RATE`           | Tasa de éxito S3 en modo simulador (0.0–1.0)                             | `0.5`               |
| `SOURCE_PATH`                   | Ruta raíz del almacenamiento primario (Lustre)                          | `/depot/goes16`     |

### Perfiles de entorno (.env)
```ini
# .env.v1
PROCESSOR_MODE=real
DB_PATH=/data/db/historic_v1.db
SOURCE_PATH=/depot/goes16
DOWNLOAD_PATH=/data/tmp/v1
MAX_WORKERS=8
S3_FALLBACK_ENABLED=true
LUSTRE_ENABLED=1
```

## Uso de la API

**Ejemplo 1: Solicitud L1b:**
```json
{
    "sat": "GOES-16",
    "nivel": "L1b",
    "dominio": "fd",
    "bandas": ["02", "13"],
    "fechas": {
        "20231026": ["00:00-01:00", "15:30"],
        "20231027-20231028": ["23:00-23:59"]
    }
}
```

**Ejemplo 2: Solicitud L2:**
```json
{
    "sat": "GOES-16",
    "nivel": "L2",
    "productos": ["ACHA", "CMIP"],
    "dominio": "conus",
    "bandas": ["13"],
    "fechas": {
        "20210501": ["19:00-19:17", "20:00-22:05"]
    }
}
```

### Índice de Endpoints

| # | Método | Ruta | Descripción |
|---|--------|------|-------------|
| 1 | `GET` | `/` | Ping básico |
| 2 | `GET` | `/health` | Verificación de salud detallada |
| 3 | `POST` | `/validate` | Validar solicitud sin ejecutar |
| 4 | `POST` | `/query` | Crear y encolar consulta |
| 5 | `GET` | `/query/{consulta_id}` | Estado y progreso de una consulta |
| 5.1 | `GET` | `/query/{consulta_id}?resultados=True` | Resultados completos al finalizar |
| 5.2 | `POST` | `/query/{consulta_id}/restart` | Reiniciar consulta atascada o fallida |
| 5.3 | `DELETE` | `/query/{consulta_id}` | Eliminar consulta (y opcionalmente su directorio) |
| 6 | `GET` | `/queries` | Listar todas las consultas |

---

### 1. Ping básico (`GET /`)

Verificación mínima de que el servidor está activo.

**Respuesta:**
```json
{ "status": "active", "timestamp": "2024-01-15T10:00:00.000000" }
```

---

### 2. Verificación de salud (`GET /health`)

Verifica el estado de la base de datos, la accesibilidad al almacenamiento primario (Lustre) y reporta si Lustre y S3 están habilitados.

Códigos de respuesta:
- `200 OK` si todo está bien
- `503 Service Unavailable` si existe algún fallo

**Respuesta:**
```json
{
    "status": "ok",
    "database": "ok",
    "storage": "ok",
    "lustre_enabled": true,
    "s3_enabled": true,
    "timestamp": "2024-01-15T10:00:00.000000"
}
```

---

### 3. Validar una solicitud (`POST /validate`)

Verifica si una consulta es válida sin ejecutarla. Devuelve una estimación de archivos y tamaño.

**Respuesta al ejemplo 1:**
```json
{
  "success": true,
  "message": "La solicitud es válida.",
  "archivos_estimados": 40,
  "tamanio_estimado_mb": 1580.0
}
```

Códigos de error posibles:
- `400` parámetros inválidos (satélite, dominio, nivel, fechas futuras…)
- `413` la consulta excede el límite de archivos o tamaño configurado
- `422` estructura JSON inválida
- `507` espacio en disco insuficiente

---

### 4. Crear una consulta (`POST /query`)

Envía la solicitud para ser procesada en segundo plano. Ejecuta las mismas validaciones que `/validate` antes de encolar. Devuelve una `consulta_id`.

Códigos y headers:
- `202 Accepted`
- `Location: /query/{ID}`

**Respuesta (body):**
```json
{
    "success": true,
    "consulta_id": "aBcDeF12",
    "estado": "recibido",
    "resumen": {
        "satelite": "GOES-16",
        "sensor": "ABI",
        "nivel": "L1b",
        "fechas": 2,
        "horas": 3
    }
}
```

---

### 5. Estado de una consulta (`GET /query/{consulta_id}`)

Consulta el estado y progreso de una solicitud en curso o finalizada.

Códigos y headers:
- `200 OK` cuando `estado = "completado"`
- `202 Accepted` cuando la consulta está en curso (incluye header `Retry-After: 10`)
- `404 Not Found` si no existe
- `500 Internal Server Error` si `estado = "error"`

**Respuesta (en proceso):**
```json
{
    "consulta_id": "aBcDeF12",
    "estado": "procesando",
    "progreso": 45,
    "mensaje": "Recuperando archivo 50/112",
    "etapa": "recuperando-local",
    "ruta_destino": "/data/tmp/aBcDeF12",
    "total_mb": null,
    "timestamp": "2023-11-20T15:30:00.123456"
}
```

**Respuesta (completado):**
```json
{
    "consulta_id": "aBcDeF12",
    "estado": "completado",
    "progreso": 100,
    "mensaje": "Recuperación: T=112, L=110, S=2",
    "etapa": "completado",
    "ruta_destino": "/data/tmp/aBcDeF12",
    "total_mb": 12345.67,
    "total_archivos": 112,
    "archivos_lustre": 110,
    "archivos_s3": 2,
    "timestamp": "2023-11-20T15:35:10.654321"
}
```

Etapas derivadas del campo `etapa`:

| Valor | Descripción |
|-------|-------------|
| `preparando` | Configurando entorno de trabajo |
| `recuperando-local` | Copiando archivos desde Lustre |
| `s3-listado` | Identificando pendientes en S3 |
| `s3-descargando` | Descargando de S3 |
| `finalizando` | Generando reporte final |
| `completado` | Finalizado con éxito |
| `error` | Terminó con error |
| `desconocida` | Estado no reconocido |

---

### 5.1 Resultados completos (`GET /query/{consulta_id}?resultados=True`)

Una vez que el estado es `completado`, devuelve el reporte detallado con la lista completa de archivos por fuente.

**Respuesta de ejemplo:**
```json
{
    "consulta_id": "aBcDeF12",
    "estado": "completado",
    "resultados": {
        "fuentes": {
            "lustre": { "archivos": ["..."], "total": 110 },
            "s3":     { "archivos": ["..."], "total": 2 }
        },
        "conteo_por_producto": {
            "ACHA": 868,
            "ADP": 436,
            "COD": 4,
            "LST": 76,
            "VAA": 0
        },
        "conteo_por_producto_s3": {
            "ACHA": 860,
            "ADP": 430,
            "COD": 4,
            "LST": 72,
            "VAA": 0
        },
        "total_archivos": 112,
        "tamaño_total_mb": 12345.67,
        "directorio_destino": "/data/tmp/aBcDeF12",
        "consulta_recuperacion": { "...": "..." }
    }
}
```

Nota: el campo `consulta_recuperacion` contiene la consulta original filtrada a los archivos que **no** se pudieron recuperar, para facilitar reintentos.

---

### 5.2 Reiniciar una consulta (`POST /query/{consulta_id}/restart`)

Reencola una consulta existente para que vuelva a procesarse con la misma configuración original guardada. Útil tras reinicios del servidor o fallas temporales. No vuelve a recuperar archivos que ya existan en la carpeta destino.

Requiere header `X-API-Key` si la variable `API_KEY` está configurada.

Códigos y headers:
- `202 Accepted`
- `Location: /query/{ID}`
- `400` si el estado no permite reinicio (ej. `recibido`)
- `404` si no existe

Estados desde los que se puede reiniciar: `procesando`, `error`, `completado`.

**Respuesta:**
```json
{ "success": true, "message": "La consulta 'aBcDeF12' ha sido reenviada para su procesamiento." }
```

---

### 5.3 Eliminar una consulta (`DELETE /query/{consulta_id}`)

Elimina el registro de una consulta de la base de datos y opcionalmente purga el directorio de trabajo asociado.

Requiere header `X-API-Key` si la variable `API_KEY` está configurada.

Parámetros de query:
- `purge` (bool, opcional): si es `true`, elimina el directorio `{DOWNLOAD_PATH}/{ID}`.
- `force` (bool, opcional): si es `true`, permite purgar aunque la consulta esté en estado `procesando`.

Códigos de respuesta:
- `200 OK` si se eliminó o se informó estado
- `404` si no existe y no se indicó `purge`
- `409` si se intenta purgar una consulta en proceso sin `force=true`

Ejemplos:
```bash
# Eliminar solo el registro de la base
curl -X DELETE "http://127.0.0.1:9041/query/$ID"

# Eliminar registro y purgar directorio
curl -X DELETE "http://127.0.0.1:9041/query/$ID?purge=true"

# Forzar la purga aunque esté procesando
curl -X DELETE "http://127.0.0.1:9041/query/$ID?purge=true&force=true"
```

Respuestas típicas:
```json
{ "success": true, "message": "Registro de consulta eliminado. Directorio purgado." }
{ "success": true, "message": "Registro de consulta no encontrado. Directorio purgado." }
```

---

### 6. Listar consultas (`GET /queries`)

Devuelve un listado resumido de consultas almacenadas en la base de datos. Útil para monitoreo.

Parámetros de query:
- `estado` (string, opcional): filtra por estado (`recibido`, `procesando`, `completado`, `error`).
- `limite` (int, opcional, default `20`): número máximo de resultados.

**Respuesta:**
```json
{
    "total": 2,
    "consultas": [
        {
            "id": "aBcDeF12",
            "estado": "completado",
            "progreso": 100,
            "satelite": "GOES-16",
            "timestamp": "2023-11-20T15:30:00.123456"
        },
        {
            "id": "xYzWwW99",
            "estado": "procesando",
            "progreso": 60,
            "satelite": "GOES-16",
            "timestamp": "2023-11-20T16:00:00.000000"
        }
    ]
}
```

## Arquitectura

*   **API (main.py)**: Construida con FastAPI, maneja las rutas, la validación inicial y delega el trabajo pesado a un procesador de fondo.
*   **Procesador de Solicitudes (processors.py)**: Parsea y enriquece la solicitud del usuario, manejando la lógica de fechas, horarios y bandas.
*   **Base de Datos (database.py)**: Utiliza SQLite para persistir el estado, progreso y resultados de cada consulta.
*   **Procesador de Fondo (recover.py / s3_recover.py / background_simulator.py)**: 
    - `recover.py`: Lógica de recuperación local (Lustre) y orquestación.
    - `s3_recover.py`: Toda la lógica de descubrimiento y descarga desde S3, incluyendo filtrado avanzado por hora y minuto.
    - `background_simulator.py`: Facilita el desarrollo y pruebas sin acceso a los sistemas reales.
*   **Configuración (config.py)**: Clases que definen la lógica de validación específica para cada tipo de satélite, haciendo el sistema extensible.

## Reglas de validación de bandas
- Nivel L1B: requiere bandas (lista o "ALL").
- Nivel L2:
  - Productos CMI/CMIP/CMIPC: requieren bandas (lista o "ALL").
  - Otros productos (por ejemplo ACHA): no requieren bandas; se ignoran si se envían.

## Patrones de archivos
- L2 CMI (conus, banda 13):
  CG_ABI-L2-CMIPC-M6C13_G16_sYYYYJJJHHMMSSS_eYYYYJJJHHMMSSS_cYYYYJJJHHMMSSS.nc
- L2 ACHA (conus):
  CG_ABI-L2-ACHAC-M6_G16_sYYYYJJJHHMMSSS_eYYYYJJJHHMMSSS_cYYYYJJJHHMMSSS.nc
- L1B (ejemplo):
  OR_ABI-L1b-RadC-M6C13_G16_sYYYYJJJHHMMSSS_eYYYYJJJHHMMSSS_cYYYYJJJHHMMSSS.nc

## Testing
```bash
pytest              # todas las pruebas
pytest -m "not real_io"  # sin I/O real
```

## Cabeceras relevantes
- Location: devuelto por POST /query y POST /query/{ID}/restart
- Retry-After: 10 en GET /query/{ID} cuando la consulta está en curso

## Despliegue y reanudación segura

- Las descargas desde S3 son idempotentes: si un archivo ya existe localmente, se omite. Esto permite reinicios o despliegues sin perder trabajo ya descargado.
- El progreso durante S3 se actualiza en cortes cada 100 archivos (85%→95%). Al finalizar el reporte pasa a 100%.
- Si una consulta parece detenida tras un reinicio, puedes reencolarla:
    - `POST /query/{consulta_id}/restart` (opcionalmente protegido con API_KEY)
    - El proceso retomará sin volver a descargar lo ya presente en disco.

## Ajustes operativos

- FILE_PROCESSING_TIMEOUT_SECONDS: tiempo máximo por archivo (segundos). Predeterminado: 120.
- Logging con rotación: define LOG_FILE (por defecto app.log), LOG_MAX_BYTES (10MB) y LOG_BACKUP_COUNT (7).
- S3 timeouts y reintentos:
  - S3_CONNECT_TIMEOUT (default 5)
  - S3_READ_TIMEOUT (default 30)
  - S3_RETRY_ATTEMPTS (hereda valor por defecto interno)
  - S3_RETRY_BACKOFF_SECONDS (base de backoff; se aplica jitter pequeño)
- MAX_FILES_IN_REPORT (opcional): limita cuántos nombres de archivo se incluyen en `resultados.fuentes.*.archivos` cuando el volumen es muy grande.
    - Predeterminado: 1000.
    - Solo recorta las listas para hacer la respuesta y el guardado en DB más ligeros; los campos `total` siguen reportando el conteo real.
    - Útil cuando hay cientos de miles de archivos recuperados.
- S3_PROGRESS_STEP (opcional): frecuencia de actualización de progreso durante descargas S3 (en número de archivos).
    - Predeterminado: 100.
    - Disminuir para ver actualizaciones más frecuentes en consultas grandes (p. ej., 50).
- Seguridad API opcional:
  - API_KEY: si se define, los endpoints de restart y delete requieren el header X-API-Key con ese valor.

Ejemplo de configuración:

```bash
export MAX_FILES_IN_REPORT=800
export LOG_FILE=/var/log/historic_server/app.log
export LOG_MAX_BYTES=$((20*1024*1024))
export LOG_BACKUP_COUNT=10
export S3_CONNECT_TIMEOUT=5
export S3_READ_TIMEOUT=30
export API_KEY=mi_secreto
```

## Sistema de Logging

El proyecto utiliza la librería `structlog` para generar logs estructurados, lo cual es fundamental para la monitorización y depuración en un entorno de producción.

### Formato de Logs

El sistema está configurado para funcionar de dos maneras:

1.  **Desarrollo (Consola)**: Si la aplicación se ejecuta en una terminal interactiva, los logs se mostrarán en un formato legible para humanos y con colores, optimizado para la depuración durante el desarrollo.

2.  **Producción (JSON)**: Si la aplicación se ejecuta en un entorno no interactivo (como un contenedor Docker, un servicio de `systemd`, etc.), los logs se generarán en formato **JSON**. Este formato es el estándar para sistemas de recolección de logs como Datadog, Splunk o el stack ELK, ya que permite indexar, buscar y filtrar los logs de manera eficiente.

### Contexto Automático con `consulta_id`

La ventaja más importante de `structlog` en este proyecto es su capacidad para manejar el contexto. Al inicio del procesamiento de una solicitud, el `consulta_id` se enlaza (`bind`) al logger. A partir de ese momento, **todos los logs generados** durante el ciclo de vida de esa solicitud incluirán automáticamente el `consulta_id`, sin necesidad de añadirlo manualmente en cada mensaje.

Esto permite seguir la traza completa de una solicitud a través de diferentes funciones y módulos con una simple búsqueda.

### ¿Cómo Registrar un Evento?

Para mantener la estructura, los logs deben registrarse de la siguiente manera:

1.  Obtener una instancia del logger: `log = structlog.get_logger(__name__)`
2.  El mensaje principal debe ser un identificador corto y estático.
3.  Los datos dinámicos deben pasarse como argumentos `key=value`.

**Ejemplo práctico:**

```python
# Forma INCORRECTA (estilo antiguo):
log.info(f"S3 progreso: {completados}/{total_obj}")

# Forma CORRECTA (con structlog):
log.info("s3_download_progress", completados=completados, total=total_obj)
```

Salida en formato JSON:

```json
{
  "event": "s3_download_progress",
  "completados": 50,
  "total": 200,
  "consulta_id": "aBcDeF12",
  "log_level": "info",
  "timestamp": "2023-11-21T10:30:00.123456Z",
  "logger": "s3_recover"
}
```