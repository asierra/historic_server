# Instrucciones para verificar y arreglar el problema

## Paso 1: Verificar que los cambios están aplicados

Ejecuta este comando para verificar tu última consulta en la base de datos:

```bash
python3 debug_consulta.py
```

O para una consulta específica:

```bash
python3 debug_consulta.py <CONSULTA_ID>
```

## Paso 2: Si sigue mostrando archivos .nc

Si el script muestra que se están generando archivos `.nc` cuando deberían ser `.tgz`, entonces:

### Opción A: Reiniciar el servidor (RECOMENDADO)

Si estás usando el servidor en modo background:

```bash
# Detener el servidor
pkill -f "python.*main.py"

# O si conoces el PID
kill <PID_DEL_SERVIDOR>

# Verificar que se detuvo
ps aux | grep main.py

# Iniciar de nuevo en modo simulador
export PROCESSOR_MODE=simulador
python3 main.py
```

### Opción B: Verificar que los archivos están actualizados

```bash
# Ver la fecha de modificación de los archivos clave
ls -lh background_simulator.py processors.py

# Ver las últimas líneas de los archivos para confirmar los cambios
tail -20 background_simulator.py
tail -20 processors.py
```

## Paso 3: Hacer una nueva consulta de prueba

Envía esta consulta de prueba:

```bash
curl -X POST http://localhost:9041/query \
  -H "Content-Type: application/json" \
  -d '{
    "nivel": "L2",
    "productos": "ALL",
    "bandas": "ALL",
    "dominio": "fd",
    "creado_por": "test@debug.com",
    "fechas": {
      "20240510": ["18:00"]
    }
  }'
```

Guarda el `consulta_id` que te devuelva y luego verifica:

```bash
# Ver el estado
curl http://localhost:9041/query/<CONSULTA_ID>

# Cuando esté completado, ver resultados
curl "http://localhost:9041/query/<CONSULTA_ID>?resultados=true"
```

## Paso 4: Verificar los logs del servidor

```bash
# Si el servidor está en background con logs
tail -f server.log

# O ver los logs del simulador
grep "Simulador -" server.log
```

Deberías ver líneas como:
```
INFO:root:Simulador - Consulta XXX: nivel=L2, bandas_originales=['ALL'], productos_originales=['ALL']
INFO:root:Simulador - copiar_tgz_completo=True, tiene_all_bandas=True, tiene_all_productos=True
INFO:root:Simulador - Devolviendo archivos .tgz sin expandir para consulta XXX
```

## Archivos modificados

Estos son los archivos que deberían tener los cambios:

1. **background_simulator.py** - Lógica principal del simulador
2. **processors.py** - Guardar bandas/productos originales

## ¿Qué debería pasar ahora?

### Para L1b + bandas="ALL":
- ✅ Debería devolver: `ABI-L1B-RadF-M6_G16-s20230011200.tgz`
- ❌ NO debería devolver: `OR_ABI-L1B-RadF-M6C01_G16_...nc`

### Para L2 + bandas="ALL" + productos="ALL":
- ✅ Debería devolver: `ABI-L2F-M6_G16-s20230011200.tgz`
- ❌ NO debería devolver: `OR_ABI-L2-ACHAF-M6_G16_...nc`

### Para L2 + bandas específicas o productos específicos:
- ✅ Debería devolver: Archivos `.nc` individuales
