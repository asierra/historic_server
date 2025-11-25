# Checklist de Pre-Deployment - Historic Server

## ✅ Lista de Verificación antes del Deployment

### 1. Código y Dependencias
- [ ] Todos los cambios están commiteados en Git
- [ ] No hay archivos con TODOs críticos sin resolver
- [ ] `requirements.txt` está actualizado
- [ ] No hay errores de linting/tipos (verificar con `get_errors`)
- [ ] Tests principales pasan correctamente

### 2. Configuración
- [ ] Archivo `.env.example` está actualizado con todas las variables
- [ ] Variables sensibles (API_KEY, rutas) están documentadas
- [ ] Configuración de S3 está validada
- [ ] Rutas de almacenamiento (Lustre, Download) están configuradas

### 3. Base de Datos
- [ ] Script `migrate_db.py` está probado
- [ ] Se ha creado backup de la BD de producción
- [ ] Migración se ejecutó exitosamente en entorno de prueba
- [ ] Esquema actualizado preserva todos los datos

### 4. Documentación
- [ ] `DEPLOYMENT_GUIDE.md` está actualizado
- [ ] Instrucciones de actualización están claras
- [ ] Notas de versión documentadas
- [ ] README.md refleja funcionalidades actuales

### 5. Scripts de Despliegue
- [ ] `server.sh` tiene permisos de ejecución
- [ ] `migrate_db.py` tiene permisos de ejecución
- [ ] Script usa Gunicorn en vez de Uvicorn directo
- [ ] Configuración de workers es apropiada

### 6. Seguridad
- [ ] API Key configurada (si se requiere)
- [ ] Permisos de archivos verificados
- [ ] Acceso a Lustre/S3 validado
- [ ] No hay credenciales hardcodeadas en el código

### 7. Servidor de Producción
- [ ] Espacio en disco suficiente verificado
- [ ] Backup de datos actuales realizado
- [ ] Servicio systemd configurado (opcional)
- [ ] Nginx configurado como proxy reverso (opcional)

### 8. Post-Deployment
- [ ] Servicio se inició correctamente
- [ ] Endpoint `/health` responde OK
- [ ] Logs no muestran errores críticos
- [ ] Consulta de prueba ejecuta correctamente
- [ ] Monitoreo de recursos (CPU, memoria, disco)

---

## 🚀 Pasos de Deployment

1. **Detener servicio actual**
   ```bash
   sudo systemctl stop historic-server
   ```

2. **Backup de BD**
   ```bash
   cp consultas_goes.db consultas_goes.db.backup_$(date +%Y%m%d_%H%M%S)
   ```

3. **Actualizar código**
   ```bash
   git pull origin main
   ```

4. **Actualizar dependencias**
   ```bash
   source venv/bin/activate
   pip install -r requirements.txt --upgrade
   ```

5. **Migrar base de datos**
   ```bash
   python migrate_db.py
   ```

6. **Reiniciar servicio**
   ```bash
   sudo systemctl start historic-server
   ```

7. **Verificar**
   ```bash
   curl http://localhost:9041/health
   sudo systemctl status historic-server
   ```

---

## 📊 Cambios desde commit 486d703

- Centralización de configuración en `settings.py`
- API Key opcional para seguridad
- Mejoras en manejo de S3 (reintentos, timeouts)
- Script de migración automática de BD
- Gunicorn como servidor de producción
- Logging estructurado con structlog
- Productos L2 actualizados
- Mejor estimación de archivos y tamaños

---

## 🆘 Rollback en caso de problemas

```bash
# Detener servicio
sudo systemctl stop historic-server

# Restaurar código
git checkout 486d703

# Restaurar base de datos
cp consultas_goes.db.backup_TIMESTAMP consultas_goes.db

# Reiniciar
sudo systemctl start historic-server
```
