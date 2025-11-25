#!/bin/bash

# Cargar variables de entorno desde .env si existe
#envfile=".env"
envfile="nolustre.env"
if [ -f "$envfile" ]; then
    export $(grep -v '^#' "$envfile" | xargs)
fi

APP="main:app"
HOST="0.0.0.0"
PORT="9041"
LOG="server.log"
# Detectar ubicación del venv (relativo o absoluto)
if [ -d "venv" ]; then
    VENV_BIN="venv/bin"
elif [ -d ".venv" ]; then
    VENV_BIN=".venv/bin"
else
    # Fallback a ruta hardcodeada o asumir en PATH
    VENV_BIN="/opt/historic_server/.venv/bin"
fi

start() {
    echo "Matando procesos gunicorn/uvicorn previos..."
    pkill -9 -f "gunicorn.*$APP"
    pkill -9 -f "uvicorn.*$APP"
    sleep 2
    echo "Arrancando servidor con Gunicorn..."
    # Usar gunicorn con clase de worker uvicorn para alto rendimiento
    # -w 4: 4 workers (ajustar según CPU)
    # -k uvicorn.workers.UvicornWorker: Worker ASGI
    nohup $VENV_BIN/gunicorn "$APP" \
        --workers 4 \
        --worker-class uvicorn.workers.UvicornWorker \
        --bind $HOST:$PORT \
        --access-logfile - \
        --error-logfile $LOG \
        --log-level info \
        > $LOG 2>&1 &
    
    echo "Servidor iniciado. Logs en $LOG"
}

stop() {
    echo "Matando procesos gunicorn..."
    pkill -f "gunicorn.*$APP"
    echo "Servidor detenido."
}

status() {
    ps -ef | grep gunicorn | grep "$APP" | grep -v grep
}

case "$1" in
    start) start ;;
    stop) stop ;;
    status) status ;;
    restart)
        stop
        sleep 2
        start
        ;;
    *)
        echo "Uso: $0 {start|stop|restart|status}"
        exit 1
        ;;
esac