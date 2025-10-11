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
VENV="/opt/historic_server/.venv/bin"

start() {
    echo "Matando procesos uvicorn previos..."
    pkill -9 -f uvicorn
    sleep 2
    echo "Arrancando uvicorn..."
    nohup $VENV/uvicorn $APP --host $HOST --port $PORT > $LOG 2>&1 &
    echo "Servidor iniciado."
}

stop() {
    echo "Matando procesos uvicorn..."
    pkill -9 -f uvicorn
    echo "Servidor detenido."
}

status() {
    ps -ef | grep uvicorn | grep -v grep
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