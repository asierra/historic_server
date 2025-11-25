
# Guía de Despliegue en Servidor Rocky 9

Esta guía detalla los pasos necesarios para preparar un servidor con Rocky Linux 9 y desplegar la aplicación `historic_server` en un entorno de producción.

## Requisitos Previos

La aplicación necesita los siguientes componentes en el servidor:

1.  **Python 3.11** (o una versión moderna) y `pip`.
2.  **Git** para clonar el repositorio.
3.  **Herramientas de compilación** (`Development Tools`) para dependencias de Python.
4.  Un **entorno virtual** para aislar las dependencias del proyecto.
5.  **Gunicorn** como servidor de aplicaciones ASGI para producción.

---

## ACTUALIZACIÓN DE SERVIDOR EXISTENTE

Si ya tienes el servidor desplegado con el commit `486d70336afcd4aa34fe413482e58799cb2213fd` o anterior, **sigue estos pasos para actualizar de forma segura**:

### 1. Detener el Servicio Actual

```bash
# Si usas systemd
sudo systemctl stop historic-server.service

# O si usas el script server.sh
./server.sh stop
```

### 2. Hacer Backup de la Base de Datos

```bash
# Crear un backup manual con timestamp
cp consultas_goes.db consultas_goes.db.backup_$(date +%Y%m%d_%H%M%S)
```

### 3. Actualizar el Código

```bash
cd /opt/historic_server

# Guardar cambios locales si los hay
git stash

# Descargar los últimos cambios
git fetch origin

# Actualizar a la última versión
git pull origin main

# Aplicar cambios guardados si es necesario
# git stash pop
```

### 4. Actualizar Dependencias

```bash
# Activar el entorno virtual
source venv/bin/activate  # o .venv/bin/activate según tu configuración

# Actualizar las dependencias
pip install -r requirements.txt --upgrade
```

### 5. Ejecutar Migración de Base de Datos

El proyecto incluye un script de migración automática que preserva todos tus datos:

```bash
# Con el entorno virtual activado
python migrate_db.py

# O especificando la ruta de la BD
python migrate_db.py /ruta/a/consultas_goes.db
```

El script:
- ✅ Crea un backup automático antes de migrar
- ✅ Verifica la integridad de los datos
- ✅ Añade nuevas columnas sin perder información
- ✅ Muestra un resumen detallado de los cambios

### 6. Actualizar Configuración (.env)

Revisa si hay nuevas variables de configuración:

```bash
# Generar una API Key segura (recomendado)
python3 -c "import secrets; print('API_KEY=' + secrets.token_urlsafe(32))"

# Añadir al .env junto con otras variables nuevas:
# S3_RETRY_ATTEMPTS=3
# S3_RETRY_BACKOFF_SECONDS=1.0
# S3_CONNECT_TIMEOUT=5
# S3_READ_TIMEOUT=30
# S3_PROGRESS_STEP=100
```

### 7. Reiniciar el Servicio

```bash
# Si usas systemd
sudo systemctl start historic-server.service
sudo systemctl status historic-server.service

# O si usas el script (actualizado a usar gunicorn)
./server.sh start
./server.sh status
```

### 8. Verificar que Todo Funciona

```bash
# Verificar la API
curl http://localhost:9041/health

# Ver logs en tiempo real
tail -f server.log  # o
sudo journalctl -u historic-server.service -f
```

---

## INSTALACIÓN DESDE CERO

Si es una instalación nueva en un servidor limpio, sigue estos pasos:

## Paso 1: Actualizar el Sistema

Asegúrate de que todos los paquetes del sistema estén actualizados a su última versión.

```bash
sudo dnf update -y
```

---

## Paso 2: Instalar Dependencias del Sistema

Instala Python, Git y las herramientas de desarrollo necesarias para compilar algunas librerías de Python.

```bash
# Instala Python 3.11, su gestor de paquetes pip, y git
sudo dnf install -y python3.11 python3.11-pip git

# Instala el grupo "Development Tools" (gcc, make, etc.)
# Es crucial para compilar extensiones en C de algunas librerías de Python.
sudo dnf groupinstall -y "Development Tools"

# Instala los encabezados de desarrollo de Python, necesarios para la compilación
sudo dnf install -y python3.11-devel
```

---

## Paso 3: Clonar el Repositorio del Proyecto

Clona el código de la aplicación en un directorio apropiado, como `/opt`.

```bash
# Navega al directorio de despliegue
cd /opt

# Clona tu repositorio (reemplaza la URL con la tuya)
sudo git clone https://tu-repositorio.com/historic_server.git

# Asigna la propiedad del directorio a tu usuario de despliegue
# Reemplaza 'tu_usuario' y 'tu_grupo' con los correctos
sudo chown -R tu_usuario:tu_grupo /opt/historic_server

# Entra al directorio del proyecto
cd /opt/historic_server
```

---

## Paso 4: Configurar el Entorno Virtual

Es una práctica recomendada aislar las dependencias de tu proyecto para evitar conflictos con las librerías del sistema.

```bash
# Asegúrate de estar en el directorio raíz de tu proyecto
cd /opt/historic_server

# Crea el entorno virtual usando la versión de Python que instalaste
python3.11 -m venv venv

# Activa el entorno virtual
source venv/bin/activate

# A partir de ahora, tu terminal mostrará (venv) al principio.
```

---

## Paso 5: Instalar Dependencias de Python

Con el entorno virtual activado, instala todas las librerías del proyecto.

```bash
pip install -r requirements.txt
```

---

## Paso 6: Crear Directorios de Datos

Crea los directorios necesarios para la base de datos y descargas:

```bash
# Directorio para la base de datos (recomendado)
sudo mkdir -p /var/lib/historic_server
sudo chown tu_usuario:tu_grupo /var/lib/historic_server
sudo chmod 750 /var/lib/historic_server

# Directorio para descargas temporales
sudo mkdir -p /data/historic_downloads
sudo chown tu_usuario:tu_grupo /data/historic_downloads
sudo chmod 755 /data/historic_downloads
```

---

## Paso 7: Configurar Variables de Entorno

Crea un archivo `.env` a partir del ejemplo:

```bash
# Copia el archivo de ejemplo
cp .env.example .env

# Edita según tu configuración
nano .env
```

Variables importantes que debes ajustar:

```ini
PROCESSOR_MODE=real
DB_PATH=/var/lib/historic_server/consultas_goes.db
SOURCE_PATH=/depot/goes16
DOWNLOAD_PATH=/data/historic_downloads
MAX_WORKERS=8              # Ajustar según CPUs disponibles (2*cores + 1)
S3_FALLBACK_ENABLED=True
LUSTRE_ENABLED=True
```

**Genera una API Key segura** (recomendado para producción):

```bash
# Genera una clave aleatoria segura
python3 -c "import secrets; print(secrets.token_urlsafe(32))"

# Copia el resultado y añádelo a tu .env
echo "API_KEY=<clave_generada>" >> .env
```

---

## Paso 7: Ejecución en Producción con Gunicorn

El proyecto incluye un script `server.sh` que facilita el manejo del servidor. Este script ahora usa **Gunicorn con workers de Uvicorn** para mejor rendimiento y estabilidad.

**Nota:** El archivo `.env` debe estar en el directorio `/opt/historic_server/` para que el script y systemd lo carguen correctamente.

### Opción A: Usar el script server.sh (Recomendado)

```bash
# Dar permisos de ejecución
chmod +x server.sh

# Iniciar el servidor
./server.sh start

# Ver estado
./server.sh status

# Reiniciar
./server.sh restart

# Detener
./server.sh stop
```

### Opción B: Ejecutar Gunicorn manualmente

```bash
# Activar el entorno virtual
source venv/bin/activate

# Iniciar con Gunicorn (ajusta workers según tu CPU)
gunicorn main:app \
    --workers 4 \
    --worker-class uvicorn.workers.UvicornWorker \
    --bind 0.0.0.0:9041 \
    --access-logfile - \
    --error-logfile server.log \
    --log-level info
```

**Recomendación de Workers:** Usa `(2 * núcleos_cpu) + 1` workers. Por ejemplo, para un servidor con 4 CPUs, usa 9 workers.

---

## Paso 9 (Recomendado): Crear un Servicio Systemd

Para que la aplicación se ejecute de forma persistente como un servicio del sistema (y se inicie automáticamente), crea un archivo de servicio `systemd`.

1.  **Crea el archivo de servicio:**
    ```bash
    sudo nano /etc/systemd/system/historic-server.service
    ```

2.  **Pega el siguiente contenido** (ajusta las rutas y el usuario si es necesario):

    ```ini
    [Unit]
    Description=Gunicorn instance to serve Historic Server API
    After=network.target

    [Service]
    User=tu_usuario
    Group=tu_grupo
    WorkingDirectory=/opt/historic_server
    # Carga las variables de entorno desde un archivo .env. La ruta es absoluta.
    EnvironmentFile=/opt/historic_server/.env
    
    ExecStart=/opt/historic_server/venv/bin/gunicorn -w 4 -k uvicorn.workers.UvicornWorker main:app --bind 0.0.0.0:9041

    Restart=always

    [Install]
    WantedBy=multi-user.target
    ```

3.  **Recarga, habilita e inicia el servicio:**

    ```bash
    # Recarga systemd para que reconozca el nuevo servicio
    sudo systemctl daemon-reload

    # Habilita el servicio para que inicie automáticamente en el arranque
    sudo systemctl enable historic-server.service

    # Inicia el servicio ahora mismo
    sudo systemctl start historic-server.service

    # (Opcional) Verifica el estado del servicio
    sudo systemctl status historic-server.service
    ```

---

## Paso 10 (Opcional): Configurar un Proxy Inverso con Nginx

Exponer Gunicorn directamente a internet no es seguro ni eficiente. Nginx debe actuar como un proxy inverso para manejar el tráfico entrante.

1.  **Instala Nginx:**
    ```bash
    sudo dnf install -y nginx
    ```

2.  **Crea un archivo de configuración para tu API:**
    ```bash
    sudo nano /etc/nginx/conf.d/historic-server.conf
    ```

3.  **Pega la siguiente configuración.** Esto redirige el tráfico del puerto 80 al puerto 9041 donde corre Gunicorn.

    ```nginx
    server {
        listen 80;
        server_name tu_dominio.com; # O la IP del servidor

        location / {
            proxy_pass http://127.0.0.1:9041;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }
    }
    ```

4.  **Habilita e inicia Nginx:**
    ```bash
    # Permite el tráfico HTTP/HTTPS en el firewall (si está activo)
    sudo firewall-cmd --permanent --add-service=http
    sudo firewall-cmd --reload

    # Habilita e inicia el servicio de Nginx
    sudo systemctl enable nginx
    sudo systemctl start nginx
    ```

---

## Paso 11: Verificación del Despliegue

```bash
# Verificar que el servicio está corriendo
sudo systemctl status historic-server.service

# Probar el endpoint de salud
curl http://localhost:9041/health

# Ver la documentación interactiva
curl http://localhost:9041/docs
```

---

## Resumen de Cambios en Esta Versión

Desde el commit `486d703` (11 Oct 2025) hasta ahora, los cambios principales incluyen:

1. **🔧 Centralización de configuración**: Todas las variables ahora se gestionan desde `settings.py` + `.env`
2. **📊 Mejoras en estimación**: Mejor cálculo de archivos y tamaños esperados
3. **🔒 API Key opcional**: Posibilidad de proteger endpoints con autenticación
4. **⚡ Mejoras en S3**: Mejor manejo de reintentos y timeouts
5. **📝 Logging estructurado**: Uso de `structlog` para logs más claros
6. **🎯 Productos L2 actualizados**: Pesos y periodicidades más precisos
7. **🛠️ Script de migración**: Actualización segura de base de datos preservando datos

---

## Notas Operativas

- Reinicios seguros: si reinicias el servicio (deploy o restart), las descargas S3 en curso se pausarán, pero al reiniciar el proceso se reanudarán. Los archivos ya presentes en disco no se vuelven a descargar.
- Progreso en S3: el porcentaje avanza por cortes (cada 100 archivos) entre 85% y 95%; al terminar se genera el reporte final (100%).
- Reintentos: si una consulta queda en estado inconsistente, puedes reencolarla vía `POST /query/{consulta_id}/restart`.
- Mensajes y etapas: el mensaje final es conciso, p.ej. `Recuperación: T=NN, L=AA, S=BB[, F=FF]`. En `?detalles=true`, la etapa local se reporta como `recuperando-local`.