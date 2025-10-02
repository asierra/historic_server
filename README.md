# historic_request

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

| Variable                        | Descripción                                                              | Valor por Defecto        |
|---------------------------------|--------------------------------------------------------------------------|--------------------------|
| `PROCESSOR_MODE`                | Modo de operación del procesador de fondo. `'real'` o `'simulador'`.      | `real`                   |
| `HISTORIC_DB_PATH`              | Ruta al archivo de la base de datos SQLite.                              | `consultas_goes.db`      |
| `HISTORIC_SOURCE_PATH`          | Ruta al directorio raíz del almacenamiento primario (Lustre).            | `/depot/goes16`          |
| `HISTORIC_DOWNLOAD_PATH`        | Ruta base donde se guardarán los archivos recuperados para cada consulta.| `/data/tmp`              |
| `HISTORIC_MAX_WORKERS`          | Número de procesos para operaciones de E/S en paralelo.                  | `8`                      |
| `S3_FALLBACK_ENABLED`           | Habilita (`true`) o deshabilita (`false`) el fallback a S3.              | `true`                   |
| `FILE_PROCESSING_TIMEOUT_SECONDS` | Tiempo máximo en segundos para procesar un archivo antes de cancelarlo.  | `120`                    |
| `SIM_LOCAL_SUCCESS_RATE`        | Tasa de éxito (0.0-1.0) para hallazgos locales en modo simulador.        | `0.8`                    |
| `SIM_S3_SUCCESS_RATE`           | Tasa de éxito (0.0-1.0) para descargas de S3 en modo simulador.          | `0.5`                    |

## Instalación y Ejecución

1.  **Instalar dependencias:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Ejecutar el servidor:**
    ```bash
    uvicorn main:app --reload
    ```
    El servidor estará disponible en `http://127.0.0.1:8000`.

3.  **Ejecutar en Producción (con Gunicorn):**
    Para un despliegue en servidor, se recomienda usar Gunicorn para gestionar los procesos de Uvicorn.
    ```bash
    # Ejemplo con 4 procesos de trabajo
    gunicorn -w 4 -k uvicorn.workers.UvicornWorker main:app --bind 0.0.0.0:8000
    ```
    Asegúrate de configurar las variables de entorno (`PROCESSOR_MODE=real`, etc.) antes de ejecutar este comando.

3.  **Ejecutar los tests:**
    ```bash
    pytest
    ```
    Para ejecutar también las pruebas de integración que requieren acceso a internet (S3):
    ```bash
    pytest -m real_io
    ```
**Dependencias clave:**
- `s3fs`: Para acceso a buckets S3 públicos de NOAA.
- `pebble`: Para pools de procesos y threads robustos.

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
    "mensaje": "Buscando y recuperando archivo 50/112",
    ...
}
```

### 4. Obtener resultados (`GET /query/{consulta_id}?resultados=True`)

Una vez que el estado es `completado`, usa este endpoint para obtener el reporte final.

**Respuesta de ejemplo:**
```json
{
    "consulta_id": "aBcDeF12",
    "estado": "completado",
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

## Arquitectura

*   **API (main.py)**: Construida con FastAPI, maneja las rutas, la validación inicial y delega el trabajo pesado a un procesador de fondo.
*   **Procesador de Solicitudes (processors.py)**: Parsea y enriquece la solicitud del usuario, manejando la lógica de fechas, horarios y bandas.
*   **Base de Datos (database.py)**: Utiliza SQLite para persistir el estado, progreso y resultados de cada consulta.
*   **Procesador de Fondo (recover.py / s3_recover.py / background_simulator.py)**: 
    - `recover.py`: Lógica de recuperación local (Lustre) y orquestación.
    - `s3_recover.py`: Toda la lógica de descubrimiento y descarga desde S3, incluyendo filtrado avanzado por hora y minuto.
    - `background_simulator.py`: Facilita el desarrollo y pruebas sin acceso a los sistemas reales.
*   **Configuración (config.py)**: Clases que definen la lógica de validación específica para cada tipo de satélite, haciendo el sistema extensible.
