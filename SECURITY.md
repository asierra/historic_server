# Guía de Seguridad - Historic Server

## 🔐 API Key

### ¿Por qué usar una API Key?

La API Key protege ciertos endpoints sensibles del servidor, como:
- `DELETE /query/{id}` - Eliminación de consultas
- `POST /query/{id}/restart` - Reinicio de consultas

### Generar una API Key Segura

**NUNCA uses palabras simples o predecibles**. Genera una clave criptográficamente segura:

#### Opción 1: Python (Recomendado)

```bash
# Genera 32 caracteres aleatorios (URL-safe)
python3 -c "import secrets; print(secrets.token_urlsafe(32))"

# Para mayor seguridad, usa 64 caracteres
python3 -c "import secrets; print(secrets.token_urlsafe(64))"
```

**Ejemplo de salida:**
```
xK8vJ2nP9mQ4wR6tY7uZ3aB5cD1eF2gH4iJ6kL8mN0oP
```

#### Opción 2: OpenSSL

```bash
openssl rand -base64 32
```

#### Opción 3: UUID

```bash
python3 -c "import uuid; print(str(uuid.uuid4()))"
```

### Configurar la API Key

1. **Generar la clave:**
   ```bash
   python3 -c "import secrets; print(secrets.token_urlsafe(32))"
   ```

2. **Añadirla al archivo `.env`:**
   ```bash
   echo "API_KEY=TU_CLAVE_GENERADA_AQUI" >> .env
   ```

3. **Verificar que está configurada:**
   ```bash
   grep API_KEY .env
   ```

### Usar la API Key

Incluye la clave en el header `X-API-Key` de tus peticiones:

```bash
# Con curl
curl -X DELETE http://localhost:9041/query/abc123 \
  -H "X-API-Key: TU_CLAVE_AQUI"

# Con Python requests
import requests

headers = {"X-API-Key": "TU_CLAVE_AQUI"}
response = requests.delete(
    "http://localhost:9041/query/abc123",
    headers=headers
)
```

### ⚠️ Advertencias de Seguridad

1. **NO** compartas tu API Key públicamente
2. **NO** la incluyas en el código fuente
3. **NO** la guardes en Git (el `.env` está en `.gitignore`)
4. **SÍ** usa variables de entorno o gestores de secretos
5. **SÍ** genera una nueva si crees que fue comprometida
6. **SÍ** usa HTTPS en producción para proteger el header

### Rotar la API Key

Si necesitas cambiar la clave:

```bash
# 1. Generar nueva clave
NEW_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")

# 2. Actualizar .env
sed -i "s/^API_KEY=.*/API_KEY=$NEW_KEY/" .env

# 3. Reiniciar el servidor
sudo systemctl restart historic-server

# 4. Actualizar todos los clientes con la nueva clave
```

### API Key Opcional

Si no configuras `API_KEY` en el `.env`, los endpoints protegidos estarán **abiertos**. Esto es útil para:
- Desarrollo local
- Redes internas protegidas por firewall
- Entornos de prueba

Para producción con acceso público, **siempre configura una API Key**.

### Configuración Adicional con Nginx

Si usas Nginx como proxy reverso, puedes añadir protección adicional:

```nginx
# Limitar acceso por IP
location / {
    allow 192.168.1.0/24;  # Tu red interna
    deny all;
    proxy_pass http://127.0.0.1:9041;
}

# Rate limiting
limit_req_zone $binary_remote_addr zone=api:10m rate=10r/s;

location /query {
    limit_req zone=api burst=20;
    proxy_pass http://127.0.0.1:9041;
}
```

### HTTPS en Producción

Para producción, **siempre** usa HTTPS con certificados SSL/TLS:

```bash
# Instalar certbot para Let's Encrypt
sudo dnf install certbot python3-certbot-nginx

# Obtener certificado
sudo certbot --nginx -d tu-dominio.com

# Auto-renovación
sudo systemctl enable certbot-renew.timer
```

---

## 🛡️ Otras Consideraciones de Seguridad

1. **Permisos de archivos:**
   ```bash
   chmod 600 .env  # Solo el propietario puede leer
   ```

2. **Firewall:**
   ```bash
   # Solo permitir puerto 80/443 (HTTP/HTTPS)
   sudo firewall-cmd --permanent --add-service=http
   sudo firewall-cmd --permanent --add-service=https
   sudo firewall-cmd --reload
   ```

3. **Logs:**
   - No registres la API Key en los logs
   - Revisa regularmente `server.log` por actividad sospechosa

4. **Actualizaciones:**
   - Mantén Python y dependencias actualizadas
   - Revisa `pip list --outdated` regularmente
