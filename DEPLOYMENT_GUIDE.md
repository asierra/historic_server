
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

## Paso 6: Ejecución en Producción con Gunicorn

Para ejecutar la aplicación, primero configura las variables de entorno y luego inicia Gunicorn.

```bash
# 1. Activa el entorno virtual (si no lo está)
source /opt/historic_server/venv/bin/activate

# 2. Configura las variables de entorno para el modo de producción
export PROCESSOR_MODE="real"
export HISTORIC_DB_PATH="/var/data/historic_api/consultas.db"
export HISTORIC_SOURCE_PATH="/ruta/a/lustre/depot/goes16"
export HISTORIC_DOWNLOAD_PATH="/var/data/historic_api/downloads"
export HISTORIC_MAX_WORKERS="16" # Ajusta según los cores de tu CPU

# 3. Inicia el servidor con Gunicorn
# El flag '-w' indica el número de procesos "worker". Una buena regla es (2 * N_CORES) + 1.
# El flag '-k' especifica la clase de worker de Uvicorn.
# El flag '--bind' indica en qué dirección IP y puerto escuchar.
gunicorn -w 4 -k uvicorn.workers.UvicornWorker main:app --bind 127.0.0.1:9041
```

**Nota:** Asegúrate de que los directorios para la base de datos (`/var/data/historic_api`) y las descargas existan y tengan los permisos correctos.

---

## Paso 7 (Recomendado): Crear un Servicio `systemd`

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
    # Carga las variables de entorno desde un archivo .env
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

## Paso 8 (Recomendado): Configurar un Proxy Inverso con Nginx

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
    ```

---

## Notas operativas

- Reinicios seguros: si reinicias el servicio (deploy o restart), las descargas S3 en curso se pausarán, pero al reiniciar el proceso se reanudarán. Los archivos ya presentes en disco no se vuelven a descargar.
- Progreso en S3: el porcentaje avanza por cortes (cada 100 archivos) entre 85% y 95%; al terminar se genera el reporte final (100%).
- Reintentos: si una consulta queda en estado inconsistente, puedes reencolarla vía `POST /query/{consulta_id}/restart`.