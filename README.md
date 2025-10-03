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
