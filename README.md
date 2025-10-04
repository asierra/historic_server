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
*   **Consultas Multi-Producto y Multi-Dominio**: Soporta solicitudes que incluyan varios productos L2 y diferentes dominios (ej. `fd`, `conus`) en una sola consulta.

## Configuración

La aplicación se configura mediante variables de entorno:

| Variable                        | Descripción                                                              | Valor por defecto |
|---------------------------------|--------------------------------------------------------------------------|-------------------|
| `PROCESSOR_MODE`                | Modo del procesador de fondo: real o simulador                          | `real`            |
| `HISTORIC_DB_PATH`              | Ruta al archivo SQLite                                                   | `consultas_goes.db` |
| `HISTORIC_SOURCE_PATH`          | Ruta raíz del almacenamiento primario (Lustre)                          | `/depot/goes16`    |
| `HISTORIC_DOWNLOAD_PATH`        | Directorio de descargas por consulta                                     | `/data/tmp`        |
| `HISTORIC_MAX_WORKERS`          | Número de procesos para E/S paralela                                    | `8`                |
| `S3_FALLBACK_ENABLED`           | Habilita o deshabilita el fallback a S3 (true/false, 1/0)               | `true`            |
| `LUSTRE_ENABLED`                | Habilita o deshabilita el uso de Lustre (true/false, 1/0)               | `true`            |
| `DISABLE_LUSTRE`                | Alternativa para deshabilitar Lustre (true/false, 1/0)                  | `false`           |
| `ENV_FILE`                      | Archivo .env a cargar al inicio                                          | `.env`            |
| `FILE_PROCESSING_TIMEOUT_SECONDS` | Tiempo máximo por archivo (segundos)                                     | `120`            |
| `SIM_LOCAL_SUCCESS_RATE`        | Tasa de éxito local en modo simulador (0.0–1.0)                          | `0.8`            |
| `SIM_S3_SUCCESS_RATE`           | Tasa de éxito S3 en modo simulador (0.0–1.0)                             | `0.5`            |

### Perfiles de entorno (.env)
```ini
# .env.v1
PROCESSOR_MODE=real
HISTORIC_DB_PATH=/data/db/historic_v1.db
HISTORIC_SOURCE_PATH=/depot/goes16
HISTORIC_DOWNLOAD_PATH=/data/tmp/v1
HISTORIC_MAX_WORKERS=8
S3_FALLBACK_ENABLED=true
LUSTRE_ENABLED=1
```

Arranque con perfil:
```bash
ENV_FILE=.env.v1 uvicorn main:app --host 0.0.0.0 --port 9041
```

## Uso de la API

### 1. Validar una solicitud (`POST /validate`)

Verifica si una consulta es válida sin ejecutarla.

**Ejemplo de Solicitud:**
```json
{
    "sat": "GOES-16",
    "nivel": "L1b",
    "bandas": ["02", "13"],
    "fechas": {
        "20231026": ["00:00-01:00", "15:30"],
        "20231027-20231028": ["23:00-23:59"]
    }
}
```

**Ejemplo de Solicitud avanzada:**
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

### 2. Crear una consulta (`POST /query`)

Envía la solicitud para ser procesada en segundo plano. Devuelve un `consulta_id`.

**Respuesta:**
```json
{
    "success": true,
    "consulta_id": "aBcDeF12",
    "estado": "recibido",
    "resumen": { ... }
}
```

### 3. Monitorear el estado (`GET /query/{consulta_id}`)

Consulta el estado y progreso de una solicitud en curso.

**Respuesta:**
```json
{
    "consulta_id": "aBcDeF12",
    "estado": "procesando",
    "progreso": 45,
    "mensaje": "Recuperando archivo 50/112",
    ...
}
```

Nota: la clave `query` solo aparece cuando el estado es `recibido`. Usa `?detalles=true` si necesitas más contexto. Para reanudar una consulta interrumpida: `POST /query/{consulta_id}/restart`.

Comando útil para ver detalles en vivo (opcional):

```bash
# Reemplaza $ID por tu consulta_id
curl -s "http://127.0.0.1:9041/query/$ID?detalles=true" | jq
```

Ejemplo de campo `detalles`:

```json
{
    "detalles": {
        "archivos_en_directorio": 23542,
        "tamaño_descargado_mb": 8123.77,
        "etapa": "recuperando-local",
        "s3_pendientes": 229913
    }
}
```

### 4. Obtener resultados (`GET /query/{consulta_id}?resultados=True`)

Una vez que el estado es `completado`, usa este endpoint para obtener el reporte final.

**Respuesta de ejemplo:**
```json
{
    "consulta_id": "aBcDeF12",
    "estado": "completado",
    "mensaje": "Recuperación: T=112, L=110, S=2",
    "resultados": {
        "fuentes": {
            "lustre": { "archivos": [...], "total": 110 },
            "s3": { "archivos": [...], "total": 2 }
        },
        "total_archivos": 112,
        "tamaño_total_mb": 12345.67,
        "directorio_destino": "/data/tmp/aBcDeF12",
        "consulta_recuperacion": { ... }
    }
}
```

Notas de estado y progreso:
- Etapas derivadas comunes (en `detalles` si usas `?detalles=true`):
    - "preparando" ("preparando entorno")
    - "recuperando-local" (mensajes como "identificados", "procesando archivo", "recuperando archivo")
    - "s3-listado" ("descargas s3 pendientes")
    - "s3-descargando" ("descargando de s3" / "descarga s3")
    - "finalizando" ("reporte final")
    - "completado", "error" o "desconocida"
- Mensaje final conciso al completar: "Recuperación: T=NN, L=AA, S=BB[, F=FF]"

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

## Despliegue y reanudación segura

- Las descargas desde S3 son idempotentes: si un archivo ya existe localmente, se omite. Esto permite reinicios o despliegues sin perder trabajo ya descargado.
- El progreso durante S3 se actualiza en cortes cada 100 archivos (85%→95%). Al finalizar el reporte pasa a 100%.
- Si una consulta parece detenida tras un reinicio, puedes reencolarla:
    - `POST /query/{consulta_id}/restart`
    - El proceso retomará sin volver a descargar lo ya presente en disco.

## Ajustes operativos

- MAX_FILES_IN_REPORT (opcional): limita cuántos nombres de archivo se incluyen en `resultados.fuentes.*.archivos` cuando el volumen es muy grande.
    - Predeterminado: 1000.
    - Solo recorta las listas para hacer la respuesta y el guardado en DB más ligeros; los campos `total` siguen reportando el conteo real.
    - Útil cuando hay cientos de miles de archivos recuperados.

Ejemplo de configuración:

```bash
export MAX_FILES_IN_REPORT=800
```
