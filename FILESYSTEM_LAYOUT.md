# Estructura de Archivos en Producción - Historic Server

## 📂 Layout del Sistema de Archivos

### Directorios Principales

```
/opt/historic_server/          # Aplicación y código
├── venv/                      # Entorno virtual Python
├── main.py                    # Aplicación principal
├── .env                       # Configuración (NO en Git)
├── server.sh                  # Script de gestión
├── migrate_db.py             # Script de migración
└── ...                       # Otros archivos del proyecto

/var/lib/historic_server/      # Datos persistentes (BASE DE DATOS)
└── consultas_goes.db         # Base de datos SQLite

/data/historic_downloads/      # Archivos temporales de consultas
├── abc123/                   # Directorio por consulta_id
│   ├── archivo1.nc
│   ├── archivo2.nc
│   └── resultado.tar.gz
└── def456/
    └── ...

/depot/goes16/                 # Almacenamiento Lustre (lectura)
└── ABI-L1b-RadF/
    └── ...

/var/log/                      # Logs (si usas syslog)
└── historic_server/
    └── server.log

/etc/systemd/system/           # Configuración de servicio
└── historic-server.service
```

---

## 📝 Ubicaciones Detalladas

### 1. Base de Datos: `/var/lib/historic_server/`

**Ruta completa:** `/var/lib/historic_server/consultas_goes.db`

**Por qué esta ubicación:**
- ✅ Estándar FHS (Filesystem Hierarchy Standard) para datos de aplicaciones
- ✅ Persiste entre actualizaciones de código
- ✅ Backups más sencillos (un solo directorio)
- ✅ Permisos controlados separados del código

**Crear el directorio:**
```bash
sudo mkdir -p /var/lib/historic_server
sudo chown tu_usuario:tu_grupo /var/lib/historic_server
sudo chmod 750 /var/lib/historic_server
```

**En `.env`:**
```ini
DB_PATH=/var/lib/historic_server/consultas_goes.db
```

---

### 2. Descargas/Consultas: `/data/historic_downloads/`

**Ruta completa:** `/data/historic_downloads/{consulta_id}/`

**Por qué esta ubicación:**
- ✅ Montaje en partición/disco con mucho espacio
- ✅ Fácil de limpiar archivos antiguos
- ✅ Puede estar en un volumen diferente (NFS, otro disco, etc.)
- ✅ No mezcla datos con código o BD

**Crear el directorio:**
```bash
sudo mkdir -p /data/historic_downloads
sudo chown tu_usuario:tu_grupo /data/historic_downloads
sudo chmod 755 /data/historic_downloads
```

**En `.env`:**
```ini
DOWNLOAD_PATH=/data/historic_downloads
```

---

### 3. Código de Aplicación: `/opt/historic_server/`

**Por qué `/opt/`:**
- ✅ Estándar para software adicional/opcional
- ✅ Separado de paquetes del sistema
- ✅ Fácil de gestionar con Git

**Configuración `.env`:**
```bash
# Debe estar en /opt/historic_server/.env
cd /opt/historic_server
nano .env
```

---

### 4. Logs

**Opción 1: Logs en el directorio del proyecto (por defecto)**
```bash
/opt/historic_server/server.log
```

**Opción 2: Logs en /var/log (recomendado para producción)**
```bash
sudo mkdir -p /var/log/historic_server
sudo chown tu_usuario:tu_grupo /var/log/historic_server

# En server.sh, cambiar:
LOG="/var/log/historic_server/server.log"
```

**Opción 3: Systemd Journal (si usas systemd)**
```bash
# Los logs van automáticamente a journalctl
sudo journalctl -u historic-server.service -f
```

---

## 🔒 Permisos Recomendados

```bash
# Base de datos (solo el usuario de la app)
chmod 750 /var/lib/historic_server
chmod 640 /var/lib/historic_server/consultas_goes.db

# Configuración (CRÍTICO: debe ser legible por systemd)
chmod 640 /opt/historic_server/.env
chown tu_usuario:tu_grupo /opt/historic_server/.env

# Descargas (accesible para lectura general si es necesario)
chmod 755 /data/historic_downloads
chmod 755 /data/historic_downloads/*

# Scripts ejecutables
chmod 755 /opt/historic_server/server.sh
chmod 755 /opt/historic_server/migrate_db.py
```

---

## 💾 Estrategia de Backups

### Base de Datos

```bash
#!/bin/bash
# /usr/local/bin/backup-historic-db.sh

BACKUP_DIR="/var/backups/historic_server"
DB_PATH="/var/lib/historic_server/consultas_goes.db"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

mkdir -p "$BACKUP_DIR"

# Backup de SQLite (con checkpoint para consistencia)
sqlite3 "$DB_PATH" ".backup '$BACKUP_DIR/consultas_goes_$TIMESTAMP.db'"

# Comprimir
gzip "$BACKUP_DIR/consultas_goes_$TIMESTAMP.db"

# Mantener solo últimos 30 días
find "$BACKUP_DIR" -name "consultas_goes_*.db.gz" -mtime +30 -delete

echo "Backup completado: consultas_goes_$TIMESTAMP.db.gz"
```

**Automatizar con cron:**
```bash
# Editar crontab
crontab -e

# Backup diario a las 2 AM
0 2 * * * /usr/local/bin/backup-historic-db.sh
```

### Archivos de Consultas

```bash
# Las consultas completadas pueden archivarse periódicamente
find /data/historic_downloads -type d -mtime +7 -exec tar -czf {}.tar.gz {} \; -exec rm -rf {} \;
```

---

## 🔄 Migración de Datos Existentes

Si ya tienes datos en otra ubicación:

```bash
# Detener servicio
sudo systemctl stop historic-server

# Mover base de datos
sudo mkdir -p /var/lib/historic_server
sudo mv /opt/historic_server/consultas_goes.db /var/lib/historic_server/
sudo chown tu_usuario:tu_grupo /var/lib/historic_server/consultas_goes.db

# Actualizar .env
sed -i 's|^DB_PATH=.*|DB_PATH=/var/lib/historic_server/consultas_goes.db|' /opt/historic_server/.env

# Reiniciar
sudo systemctl start historic-server
```

---

## 📊 Monitoreo de Espacio en Disco

```bash
# Ver espacio usado por cada directorio
du -sh /var/lib/historic_server
du -sh /data/historic_downloads

# Limpiar consultas antiguas (ejemplo: más de 30 días)
find /data/historic_downloads -type d -mtime +30 -exec rm -rf {} \;
```

---

## 🌐 Variables de Entorno para Producción

Ejemplo completo de `.env` para producción:

```ini
# === Configuración de Producción ===
PROCESSOR_MODE=real

# Datos persistentes
DB_PATH=/var/lib/historic_server/consultas_goes.db

# Almacenamiento
SOURCE_PATH=/depot/goes16
DOWNLOAD_PATH=/data/historic_downloads

# Rendimiento
MAX_WORKERS=16

# Características
S3_ENABLED=True
LUSTRE_ENABLED=True

# Seguridad
API_KEY=xK8vJ2nP9mQ4wR6tY7uZ3aB5cD1eF2gH

# Límites
MAX_FILES_PER_QUERY=100000
MAX_SIZE_MB_PER_QUERY=50000
MIN_FREE_SPACE_GB_BUFFER=20

# S3
S3_RETRY_ATTEMPTS=3
S3_CONNECT_TIMEOUT=10
S3_READ_TIMEOUT=60
```
