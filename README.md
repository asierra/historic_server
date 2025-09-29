# historic_request

API para la recuperación de datos históricos de satélite desde el sistema de almacenamiento masivo del LANOT.

## Objetivo:

Permitir que usuarios y sistemas externos puedan generar consultas complejas de datos históricos de satélite de manera asíncrona. La API valida las solicitudes, las procesa en segundo plano y permite monitorear el progreso hasta obtener los resultados, que incluyen los archivos recuperados y un reporte detallado.

## Características Principales

*   **Procesamiento Asíncrono**: Las solicitudes se procesan en segundo plano, permitiendo manejar consultas de larga duración sin bloquear al cliente.
*   **Sistema de Almacenamiento Dual**:
    1.  Busca y recupera archivos eficientemente desde un sistema de archivos primario de alto rendimiento (como **Lustre**).
    2.  Implementa un mecanismo de **fallback a S3** (NOAA GOES Bucket) para recuperar archivos que no se encuentren localmente.
*   **E/S Paralelizada**: Utiliza un pool de hilos (`ThreadPoolExecutor`) para realizar operaciones de búsqueda, copia y descarga de archivos en paralelo, maximizando el rendimiento.
*   **Extracción Inteligente**: Es capaz de copiar archivos `.tgz` completos o extraer selectivamente su contenido (`.nc`) según los parámetros de la solicitud, optimizando el uso de disco.
*   **Robustez**: Incluye mecanismos de reintento con backoff exponencial para operaciones de red (descargas de S3) y un manejo de errores que permite identificar archivos corruptos.
*   **Reportes Detallados**: Al finalizar, genera un reporte en formato JSON que distingue los archivos recuperados desde el almacenamiento local y los descargados de S3, y provee una consulta de recuperación para los archivos que no se pudieron encontrar.
*   **Validación Avanzada**: Sistema de validación extensible basado en Pydantic y clases de configuración por satélite.
*   **Modo de Simulación**: Incluye un modo de `simulador` para desarrollo y pruebas sin necesidad de acceder al sistema de archivos real.

## Configuración

La aplicación se configura mediante variables de entorno:

| Variable                 | Descripción                                                              | Valor por Defecto        |
|--------------------------|--------------------------------------------------------------------------|--------------------------|
| `PROCESSOR_MODE`         | Modo de operación del procesador de fondo. `'real'` o `'simulador'`.      | `real`                   |
| `HISTORIC_DB_PATH`       | Ruta al archivo de la base de datos SQLite.                              | `consultas_goes.db`      |
| `HISTORIC_SOURCE_PATH`   | Ruta al directorio raíz del almacenamiento primario (Lustre).            | `/depot/goes16`          |
| `HISTORIC_DOWNLOAD_PATH` | Ruta base donde se guardarán los archivos recuperados para cada consulta.| `/data/tmp`              |
| `HISTORIC_MAX_WORKERS`   | Número de hilos para operaciones de E/S en paralelo.                     | `8`                      |

## Instalación y Ejecución

1.  **Instalar dependencias:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Ejecutar el servidor:**
    ```bash
    uvicorn main:app --reload
    ```
    El servidor estará disponible en `http://localhost:8000`.

4.  **Ejecutar en Producción (con Gunicorn):**
    Para un despliegue en servidor, se recomienda usar Gunicorn para gestionar los procesos de Uvicorn.
    ```bash
    # Ejemplo con 4 procesos de trabajo
    gunicorn -w 4 -k uvicorn.workers.UvicornWorker main:app
    ```
    Asegúrate de configurar las variables de entorno (`PROCESSOR_MODE=real`, etc.) antes de ejecutar este comando.

3.  **Ejecutar los tests:**
    ```bash
    pytest
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
*   **Procesador de Fondo (recover.py / background_simulator.py)**: Componente clave que se ejecuta de forma asíncrona. La implementación `real` interactúa con los sistemas de almacenamiento, mientras que la `simulador` facilita el desarrollo.
*   **Configuración (config.py)**: Clases que definen la lógica de validación específica para cada tipo de satélite, haciendo el sistema extensible.
