# Plan: cola durable de consultas

Sacar el procesamiento del proceso que sirve HTTP, y convertir la tabla `consultas` en una
cola con *leases*. Sin broker, sin estados nuevos, sin tocar `historic_query`.

| | |
|---|---|
| **Rama base** | `fix/ci-y-timeouts` (`d6d64aa`) |
| **Fecha** | 24-ago-2026 · entrega 1 implementada el 25-ago |
| **Estado** | **Entrega 1 hecha** (§4): esquema y primitivas, sin cambio de comportamiento, desplegable sola. **Entrega 2 aplazada** el 25-ago a falta de dos números de tahan (§6). |

---

## 1. El problema

El proceso que sirve HTTP es el mismo que descarga los archivos. Las tareas de fondo viven en
la memoria de ese proceso (`BackgroundTasks`), así que cualquier cosa que lo mate —un
despliegue, `server.sh restart`, un OOM, el `Restart=always` de systemd— se lleva por delante
el trabajo en curso.

La fila en SQLite, en cambio, se queda intacta en `recibido` o `procesando`. Nada la vuelve a
mirar: no hay barrido, ni reintento, ni rescate. Django la sondea, ve `en_proceso`, y ahí se
queda para siempre. Le pasó a `GkpH6xne`: 29 horas al 89% porque el servicio se reinició a
media descarga. La única salida hoy es que una persona se dé cuenta y pulse *Reiniciar
consulta*.

De ahí salen tres consecuencias que hoy se tratan como casos especiales, y que en realidad son
el mismo defecto visto desde tres ángulos:

- `POST /query/{id}/restart` acepta `recibido` porque es donde aterriza una tarea que nunca
  arrancó.
- `DELETE ?purge=true` devuelve 409 en vuelo porque la tarea puede arrancar en cualquier
  momento y recrear el directorio.
- `reclamar_para_reproceso()` existe porque los 4 workers de gunicorn no comparten memoria y
  dos reinicios simultáneos descargaban los mismos 2380 archivos.

> **El punto de partida.** La pieza difícil ya está hecha. El `UPDATE` condicional de
> `reclamar_para_reproceso()` es un cerrojo atómico probado en producción. Todo lo que sigue es
> generalizarlo.

---

## 2. La tesis

Persistir la cola no basta si el trabajo sigue viviendo dentro de la API. El cambio de fondo es
**separar los dos procesos**: la API escribe filas, un worker aparte las consume.

```
HOY · un proceso                    DESPUÉS · dos unidades
┌─────────────────────────┐         ┌─────────────────────────┐
│ gunicorn -w 4           │         │ historic-server         │
│ sirve HTTP + descarga   │         │ sirve HTTP · INSERT y ya│
└─────────────────────────┘         └─────────────────────────┘
          ↓ reiniciar                ┌─────────────────────────┐
┌─────────────────────────┐         │ historic-worker         │
│ trabajo perdido         │         │ reclama · procesa       │
│ fila congelada sin dueño│         └─────────────────────────┘
└─────────────────────────┘                   ↓ reiniciar la API
                                     el worker sigue descargando
```

Lo que se gana, en orden de importancia real:

1. **Desplegar la API deja de tocar el trabajo en curso.** Hoy cualquier `systemctl restart
   historic-server` es una decisión operativa con consecuencias; después es rutina.
2. ~~**El worker se puede parar solo.** Durante la fuga de 1.1 TB del 19-ago se habrían podido
   frenar las descargas sin tirar la API ni perder estado.~~ ❌ **Falso, corregido el 25-ago.**
   La bitácora dice que aquel día no había nada en vuelo —«las 35 filas del QueryProcessor en
   `completado`»— y la fuga era de punteros perdidos, no de descargas desbocadas: parar el worker
   no habría servido de nada. No hay ningún incidente registrado que pidiera esta capacidad.
3. **Escala por unidades, no por `-w`.** Hoy subir `-w` sube las dos cosas a la vez y multiplica
   el riesgo de duplicados.
4. **La ventana de corte es segura por construcción.** Entre desplegar la API nueva y arrancar
   el worker, las consultas se acumulan en `recibido` sin perderse. Eso es exactamente lo que
   hoy no pasa.

**No es sólo añadir.** `background_tasks.add_task` sale de los dos endpoints.
`reclamar_para_reproceso()` se disuelve en el reclamo genérico. `/restart` se vuelve «pon
`recibido`, limpia el lease». El `ProcessPool` sale del `lifespan` de la API. La estimación es
que `main.py` sale en negativo neto de líneas.

---

## 3. Diseño

### 3.1 Cuatro columnas y un índice

La tabla `consultas` ya tiene id, estado, la query, el progreso y un latido que el pipeline
refresca solo. Le falta lo que convierte eso en una cola con custodia:

| Columna | Tipo | Para qué |
|---|---|---|
| `intentos` | `INTEGER DEFAULT 0` | Cortar el veneno: a la N-ésima, `error` y se queda ahí en vez de reintentar para siempre. |
| `lease_hasta` | `DATETIME` | Hasta cuándo vale el reclamo. El pipeline lo extiende en cada avance, igual que hoy hace con el latido. |
| `worker_id` | `TEXT` | Quién la tiene, como `hostname:pid`. Para diagnosticar desde el journal sin adivinar. |
| `disponible_desde` | `DATETIME` | Backoff entre reintentos. Permite aplazar sin cambiar el estado, que es lo que ve Django. |

Más un índice, que hoy no existe — la tabla sólo tiene el autoindex de la clave primaria:

```sql
CREATE INDEX IF NOT EXISTS idx_consultas_cola
    ON consultas(estado, disponible_desde, timestamp_creacion);
```

Con 26 filas da igual para la velocidad; se pone porque es la consulta caliente del worker y
cuesta una línea.

### 3.2 El reclamo

Una sola sentencia atómica. SQLite 3.46 en la máquina admite `RETURNING`, así que reclamar y
leer la query es un solo viaje:

```sql
UPDATE consultas
   SET estado='procesando', worker_id=?, lease_hasta=?, intentos=intentos+1
 WHERE id = (SELECT id FROM consultas
              WHERE estado='recibido'
                AND (disponible_desde IS NULL OR disponible_desde <= ?)
              ORDER BY timestamp_creacion LIMIT 1)
RETURNING id, query;
```

Dos workers compitiendo: uno recibe fila, el otro nada. Es el mismo invariante de
`reclamar_para_reproceso()`, aplicado a «coge la siguiente» en vez de «coge ésta».

### 3.3 El lease es el latido que ya existe

`actualizar_estado()` se llama en cada avance del pipeline y ya escribe
`timestamp_actualizacion`. Se extiende para que, cuando el estado sea `procesando`, empuje
también `lease_hasta = ahora + LEASE_S`. No hay que instrumentar nada nuevo: el pipeline ya
late.

Un lease vencido vuelve a `recibido` y lo coge quien sea. **El rescate deja de ser un caso
especial del arranque y pasa a ser el funcionamiento normal de la cola.** Cualquier worker
recoge en su tick:

```sql
UPDATE consultas SET estado='recibido', worker_id=NULL
 WHERE estado='procesando' AND lease_hasta < ?;
```

### 3.4 Ciclo de vida

```
                  reclamo
   ┌──────────┐ ─────────────► ┌────────────┐ ──────────► completado
   │ recibido │                │ procesando │
   └──────────┘ ◄───────────── └────────────┘ ──────────► error
        ▲   ▲     lease vencido        │                  (intentos = 3)
        │   └───────────────────────────┘
        │      falla · disponible_desde = ahora + backoff
```

Los dos caminos de vuelta a `recibido` —lease vencido y fallo con backoff— son lo que hoy no
existe. Todo lo demás ya está implementado.

### 3.5 No hacen falta estados nuevos

> **Cero cambios en `historic_query`.** `recibido` pasa a significar «encolada y disponible»;
> `procesando`, «con lease vivo». Django colapsa los dos en `en_proceso` con
> `translate_api_estado()` antes de persistir, así que el otro repo no se entera de nada.
> `intentos`, `lease_hasta` y `disponible_desde` son internos y no aparecen en ninguna
> respuesta.

---

## 4. Entrega 1 — sin cambio de comportamiento

El esquema y las primitivas de la cola, con pruebas. Al terminar esta entrega **el servicio se
comporta exactamente igual que hoy**: todo sigue pasando por `BackgroundTasks` y nadie llama a
los métodos nuevos. Es deliberado — lo verificable va primero y lo arriesgado va solo.

> **Hecho.** Lo que sigue describe lo implementado. Dos desviaciones respecto a lo propuesto,
> ambas ampliando la cobertura y ninguna cambiando el contrato:
>
> - `liberar_expiradas()` rescata también las filas en `procesando` **sin lease** cuyo latido
>   esté frío. Son las huérfanas del mundo anterior a la cola —`GkpH6xne` entre ellas—, que con
>   el `WHERE` literal de §3.3 se habrían quedado congeladas para siempre después de migrar. Se
>   exige el latido frío para no robarle trabajo a un `BackgroundTask` que siguiera vivo durante
>   el despliegue.
> - `_init_db()` aplica también los `ALTER TABLE`, no sólo el `CREATE TABLE` de bases nuevas, para
>   que arrancar el servicio sin haber corrido `migrate_db.py` no reviente al primer reclamo.
>
> Y un detalle que no estaba en la propuesta: `backoff_s` admite una secuencia además de un
> número, así que el 1/5/15 min de §8 sale sin maquinaria aparte.

### 1.1 Migración del esquema

- `migrate_db.py` — añadir `migrate_add_columnas_cola()` siguiendo el patrón `column_exists()` +
  `ALTER TABLE` que ya usa `migrate_add_usuario_column()`; crear el índice; ampliar
  `verify_schema()` con las cuatro columnas.
- `database.py` — `_init_db()` crea las columnas y el índice para bases nuevas.

Idempotente y aditivo: las filas existentes quedan con `intentos=0` y los tres campos a `NULL`,
que el reclamo interpreta como «disponible». El script ya respalda el `.db` antes de tocarlo.

**Verificación:** correr `migrate_db.py` dos veces seguidas sobre una copia; la segunda no debe
cambiar nada y `verify_schema` debe pasar.

### 1.2 Primitivas de cola en `ConsultasDatabase`

En `database.py`:

- `reclamar_siguiente(worker_id, lease_s, ahora=None)` → `(id, query)` o `None`
- `liberar_expiradas(ahora=None)` → nº de filas devueltas a `recibido`
- `fallar_con_reintento(id, mensaje, backoff_s, max_intentos, ahora=None)`
- `reencolar(id)` — lo que usará `/restart` en la entrega 2
- `actualizar_estado()` extiende `lease_hasta` cuando el estado es `procesando`

> **Detalle que decide si las pruebas son sanas.** Todos los métodos aceptan `ahora`
> inyectable. Sin eso, probar leases y backoff exige `sleep`, y la suite ya ha tenido bastante
> con dos esperas de margen justo.

**Verificación:** las pruebas de 1.3, todas sin esperas reales.

### 1.3 Pruebas de la cola

Módulo nuevo `tests/test_cola.py`. Casos que fijan los invariantes:

- Dos reclamos consecutivos sobre una sola fila disponible: el primero la obtiene, el segundo
  devuelve `None`.
- Se respeta el orden `timestamp_creacion` (FIFO).
- Una fila con `disponible_desde` en el futuro no se reclama; al pasar el instante, sí.
- Un lease vencido vuelve a `recibido`; uno vivo no se toca.
- `intentos` incrementa en cada reclamo; al llegar al tope, `fallar_con_reintento` deja `error`
  y ya no se reclama más.
- `actualizar_estado('procesando', ...)` empuja el lease.

**Verificación:** `pytest -q -m "not real_io"` verde, y las nuevas fallando si se revierte 1.2.

---

## 5. Entrega 2 — el corte

### 2.1 `worker.py`

Proceso propio, sin FastAPI. Construye `ConsultasDatabase`, el `ProcessPool` y `RecoverFiles`
con la misma configuración de `settings.py`, y entra en bucle:

1. `liberar_expiradas()`
2. `reclamar_siguiente()`; si no hay nada, dormir `POLL_S` (5 s) y repetir
3. Procesar con `recover.procesar_consulta(id, query)` — el pipeline no se toca
4. Si lanza excepción, `fallar_con_reintento()`
5. Registrar latido de salud (2.4)

Apagado limpio con `SIGTERM`: termina la consulta en curso o suelta el lease, cierra el
`ProcessPool`. Como el lease caduca solo, incluso un `SIGKILL` se recupera sin intervención.

**Verificación:** prueba de integración con el simulador, y una consulta real de punta a punta
en tren2 antes de tocar tahan.

### 2.2 Podar `main.py`

- `POST /query` — quitar `background_tasks.add_task`; el `INSERT` ya deja la consulta encolada.
- `POST /query/{id}/restart` — `reclamar_para_reproceso()` → `reencolar()`; el cerrojo lo hereda
  de la cola.
- `lifespan` — fuera el `ProcessPool` y `RecoverFiles`; la API ya no procesa nada.
- `BackgroundTasks` desaparece de los *imports* y de las dos firmas.

Lo que **no** cambia: el 409 del purge sigue haciendo falta, porque el worker puede estar
escribiendo en ese directorio ahora mismo. Y `ESTADOS_EN_VUELO` se queda como está.

### 2.3 `historic-worker.service`

Mismo `User`, mismo `EnvironmentFile`, mismo `WorkingDirectory` que `historic-server.service`.
`Restart=always`. `ExecStart` apuntando a `/opt/historic_server/.venv/bin/python worker.py`.

> ⚠️ **La trampa ya conocida.** Los units de `tempoftp` en el repo apuntaban a
> `/opt/tempoftp/venv`, que no existe —el entorno es `.venv` en los tres repos—, y fallaban con
> `203/EXEC` todos los días sin que nadie mirara ese journal. Este unit hay que **arrancarlo y
> leer el journal**, no darlo por bueno porque el archivo existe.

`server.sh` también queda desfasado: su `pkill -9 -f "gunicorn.*$APP"` no toca el worker, así
que un `restart` por esa vía dejaría el worker huérfano y la API nueva. O aprende a gestionar
las dos unidades, o se retira en favor de systemd.

**Verificación:** `systemctl start` + `journalctl -u historic-worker -f` mostrando el primer
tick, y un `systemctl restart historic-server` con una descarga en curso que **no** se
interrumpe.

### 2.4 Que `/health` deje de mentir

El circuit breaker de S3 es un singleton de módulo. Si el trabajo de S3 se muda al worker, el de
la API queda siempre inmaculado y `/health` informa de un componente que ya no hace nada.

Tabla pequeña `salud_worker(worker_id PK, timestamp, cb_estado, cb_fallos, consulta_en_curso)`
que el worker escribe en cada tick y `/health` lee. De paso arregla algo que *ya* es medio
mentira: hoy cada uno de los 4 workers de gunicorn tiene su propio breaker y `/health` enseña el
del que conteste.

**Verificación:** parar el worker y comprobar que `/health` lo reporta como ausente en vez de
callar.

### 2.5 Documentación

- `CLAUDE.md` — la sección de arquitectura describe `BackgroundTasks` en memoria como un hecho
  central; hay que reescribirla.
- `README.md` — despliegue con dos unidades; `/restart` cambia de significado.
- `DEPLOYMENT_GUIDE.md` y `FILESYSTEM_LAYOUT.md` — el worker es un proceso más con acceso de
  escritura a `DOWNLOAD_PATH`.

---

## 6. Un servicio o dos

La entrega 2 se puede hacer de dos formas, y conviene saberlo antes de empezar. Lo que cambia es
**quién corre el bucle**, no la cola.

La variante de un solo servicio deja el bucle dentro del proceso de la API, en un hilo lanzado
desde el `lifespan`. Funciona porque el lease no sabe ni le importa quién consume: los 4 workers
de gunicorn corriendo cada uno su bucle es seguro por el mismo `UPDATE` atómico de §3.2. No hay
`worker.py`, no hay unit nuevo, no hay journal nuevo.

| | Un servicio | Dos servicios |
|---|---|---|
| Consulta congelada para siempre | Arreglado | Arreglado |
| Reintentos, backoff, rescate | Sí | Sí |
| Reiniciar la API mata la descarga | Sí, pero se reanuda sola en ≤ un lease | **No la toca** |
| Parar las descargas sin tirar la API | No | **Sí** |
| Escalar un proceso sin escalar el otro | No | **Sí** |
| Superficie operativa nueva | **Ninguna** | Un unit, un journal, un fallo silencioso |

Dicho de otro modo: la versión de un servicio arregla el defecto que originó todo esto
—`GkpH6xne` congelada 29 horas— y lo degrada a «se reanuda sola en un cuarto de hora». Lo que no
da es que desplegar deje de interrumpir, ni el botón de parar las descargas que habría venido
bien el 19-ago.

### Evaluación del 25-ago (con la entrega 1 ya hecha)

**Resultado: aplazada.** Se despliega la entrega 1 sola y se decide la 2 más adelante, con datos
reales de tahan. La recomendación sobre la mesa es **un servicio**, por lo que sigue.

Lo que cambió respecto a la tabla de arriba, ahora que la cola existe:

- **El chequeo manual desaparece en las dos variantes.** Ése era el coste recurrente de verdad,
  y está escrito en la bitácora del 19-ago: *«sigue sin haber rescate al arranque, así que ese
  chequeo hay que repetirlo en cada despliegue de tahan»*. Con la cola, ninguna de las dos
  opciones lo necesita.
- **Lo que separa a las dos se encoge a ≤15 min de reanudación** en un despliegue a media
  descarga, y son minutos, no archivos: el pipeline es idempotente en las dos rutas, así que al
  reanudar sólo repite el listado. Sobre un trabajo de horas es ruido.
- **De los cuatro beneficios que §2 le atribuía a separar procesos, uno era falso** (el de la
  fuga del 19-ago, ver arriba), dos son especulativos —parar descargas aparte, escalar por
  unidades: ningún incidente los ha pedido nunca— y el cuarto vale ese cuarto de hora.
- **En contra hay algo que no es especulativo:** §0-ter de la bitácora tiene una sección entera
  titulada «El despliegue de tempoftp, con tres trampas» —unit apuntando a un venv inexistente,
  timer sin instalar, fallando a diario en silencio—. Es exactamente el modo de fallo de
  `historic-worker`, con precedente demostrado en este entorno.
- **Los cuatro bucles de gunicorn no son una regresión.** Hoy ya puede haber cuatro descargas
  simultáneas: cada `POST` lanza su `BackgroundTask` en el worker que lo atienda, y el incidente
  de los 2380 archivos duplicados fue precisamente eso. Cuatro bucles con `WORKER_SLOTS=1`
  mantienen el mismo techo. Lo único nuevo es que la cola drena sola, así que un primer arranque
  sacaría cuatro a la vez — lo cubre la decisión de §8 sobre lo muy viejo.

**Ritmo de despliegue medido** (commits por mes, como aproximación): 61, 47, 6, 1, 0, 5, 3, 5, 0,
0, 7. Tras la construcción inicial, meses enteros en silencio y luego racimos, que además caen
donde peor vienen: los siete de agosto son todos de la ventana de incidencias.

**Qué falta para cerrarla.** No hay base de producción en bucéfalo, así que estos dos números
quedaron sin medir y son los que decidirían:

```bash
# En tahan · cuántas veces un reinicio pilló trabajo en vuelo
journalctl -u historic-server --since "6 months ago" | grep -c "Stopping historic-server"

# En tahan · cuánto duran las consultas de verdad (creación → última actualización)
sqlite3 /var/lib/historic_server/consultas_goes.db \
  "SELECT id, estado, ROUND((julianday(timestamp_actualizacion)
   - julianday(timestamp_creacion)) * 24, 2) AS horas
     FROM consultas ORDER BY horas DESC LIMIT 20;"
```

Si los despliegues resultan ser mucho más frecuentes de lo que sugieren los commits, o si alguna
vez hubo ganas de frenar las descargas sin tirar la API, la recomendación se invierte.

> **Esta decisión no bloquea nada.** La entrega 1 es idéntica en los dos caminos. Las cuatro
> columnas, el reclamo, los leases, los reintentos y sus pruebas no cambian ni una línea según
> quién corra el bucle. Sólo cambia la entrega 2: un `worker.py` con su unit (2.1 y 2.3), o unas
> veinte líneas en el `lifespan`. Se puede decidir cuando la 1 esté hecha, y cambiar de idea
> después sin rehacer nada.

**Si se elige un solo servicio:**

- Los pasos 2.1 y 2.3 se sustituyen por el hilo en el `lifespan` y su apagado limpio.
- 2.2 (podar `main.py`) se queda, salvo que el `ProcessPool` no se va: lo sigue necesitando el
  bucle.
- 2.4 se simplifica: el breaker vive en el mismo proceso que responde `/health`, aunque sigue
  habiendo cuatro copias, una por worker de gunicorn.
- §7 (despliegue) se queda en un solo corte: desplegar y reiniciar. Se pierde la ventana segura,
  pero también deja de hacer falta.

**Si se eligen dos**, importa qué primitiva de systemd se usa para enlazarlos, porque es fácil
elegir la que destruye el beneficio:

- `Wants=historic-worker.service` en la API — arrancar la API arranca el worker, y **reiniciar
  la API no reinicia el worker**. Es la correcta.
- `PartOf=` o `BindsTo=` — reiniciar la API reinicia el worker. Es justo lo que estamos
  evitando; con eso, el plan entero no sirve de nada.

Con ambas `enable`, un reinicio de máquina las levanta sin intervención, y el despliegue de
código sigue siendo uno: mismo `/opt/historic_server`, mismo `.venv`, un `git pull`.

> **No confundir con lo descartado en §10.** Lo que se descarta ahí es un *rescate* dentro del
> `lifespan` **sin cola**: un barrido que reencola huérfanas y ya. Esta variante es la cola
> completa —leases, reintentos, backoff, una sola fuente de verdad— y sólo comparte con aquélla
> el sitio donde corre.

---

## 7. Despliegue

**La entrega 1 se despliega sola**, y es lo acordado el 25-ago. Son tres pasos y no cambia el
comportamiento: respaldo del `.db` (`sqlite3 .backup` + gzip, como el cron), `migrate_db.py`, y
desplegar el código. Nadie llama todavía a las primitivas nuevas, así que revertir es un `git
checkout`: las cuatro columnas se quedan sin usar y no estorban.

Lo que sigue es el corte de la entrega 2, pendiente.

### Variante de dos servicios

El orden importa, y la propiedad que lo hace seguro es que **una consulta encolada no se pierde
aunque nadie la procese todavía**.

| # | Paso | Si sale mal |
|---|---|---|
| 1 | Respaldo del `.db` (`sqlite3 .backup` + gzip, como el cron) | — |
| 2 | Correr `migrate_db.py`. La API vieja sigue corriendo y no ve las columnas nuevas | Restaurar el respaldo; nada más ha cambiado |
| 3 | Esperar a que no haya nada en vuelo, o dejar que termine | — |
| 4 | Desplegar la API nueva y reiniciarla. Las consultas nuevas se acumulan en `recibido` | Revertir el código; las filas encoladas las recoge el `/restart` de siempre |
| 5 | Instalar y arrancar `historic-worker`. Vacía la cola acumulada | Parar el worker: las filas vuelven a `recibido` solas al vencer el lease |
| 6 | Verificar en el journal un ciclo completo, y reiniciar la API a propósito con una descarga en curso | — |

> **Dos instancias.** Hay `historic-server` en tahan (`172.16.0.9:9041`) y en tren2
> (`172.16.1.101:9041`), con el traslado de `QUERY_PROCESSOR_URL` aún pendiente. **Tren2 es el
> sitio para estrenar esto**: Django todavía no le habla, así que un fallo no afecta a nadie.

WAL admite varios procesos sobre el mismo archivo, que es lo que hace viable el corte, pero
necesita memoria compartida: la base tiene que seguir en disco local
(`/var/lib/historic_server`), nunca en NFS.

---

## 8. Decisiones para revisar

Cuatro cosas donde hay que elegir. Cada una lleva una recomendación, pero son del mantenedor.

### Concurrencia del worker

Cuántas consultas a la vez. El paralelismo real ya está dentro del pipeline: `ProcessPool` con
`MAX_WORKERS` por archivo.

**Propuesta:** una consulta a la vez, con `WORKER_SLOTS=1` como variable. Subirlo es un cambio
de configuración, no de código.

### Reintentos y backoff

Reintentar es barato porque el pipeline es idempotente en las dos rutas: `scan_existing_files()`
en `recover.py:118` para Lustre, y el `exists() and st_size > 0` de `s3_recover.py:203,301` para
S3. Un reintento cuesta el listado más lo que faltara.

**Propuesta:** 3 intentos con backoff de 1 / 5 / 15 min.

### Qué hacer con lo muy viejo

Al encender el worker por primera vez, la cola arranca *todo* lo que lleve meses parado. En el
panel hay consultas de enero cuyo solicitante ya no espera nada, y arrancarlas gasta cómputo y
disco.

**Propuesta:** no reencolar nada creado hace más de 7 días: pasa a `error` con un mensaje
explícito («abandonada tras N días; usa Reiniciar consulta»). Es más honesto que una fila
congelada para siempre, y `error` es reiniciable desde el panel. El umbral rescata a `GkpH6xne`
(29 h) y no resucita enero.

### El progreso retrocede

Una consulta rescatada al 89% vuelve a subir desde el 10% («Preparando entorno»), porque el
pipeline arranca por el principio aunque no vuelva a descargar nada. Es visible en el panel de
Django.

**Propuesta:** dejarlo. Arreglarlo es hacer que el progreso refleje archivos ya presentes antes
de empezar, y eso toca el pipeline — otro cambio, otro riesgo, ningún trabajo duplicado de por
medio.

---

## 9. Riesgos y lo que no arregla

| Riesgo | Mitigación |
|---|---|
| Dos unidades donde había una: más superficie de despliegue, y un worker caído es silencioso | El latido de `salud_worker` en `/health` (2.4) lo hace visible; `Restart=always` lo levanta |
| El unit nuevo repite la trampa del `venv` inexistente | Arrancar y leer el journal como parte del paso, no después |
| Escritura concurrente sobre SQLite desde dos procesos | WAL ya está activo y el `timeout` de conexión es 30 s; el volumen es de unas pocas consultas al día |
| Una consulta que tumba al worker una y otra vez | `intentos` con tope la manda a `error` en vez de dejarla girando |
| `server.sh` deja de servir y nadie lo nota | Se adapta o se retira en 2.3, no se deja a medias |

**Lo que no arregla:** sigue habiendo una sola máquina procesando; no hay prioridades ni cuotas
por usuario; y la cola no sobrevive a que se pierda el archivo `.db` —eso lo cubre el respaldo,
no esto—. Tampoco toca el pipeline: si una descarga es lenta, seguirá siéndolo.

---

## 10. Alternativas descartadas

| Opción | Por qué no |
|---|---|
| Celery o RQ con Redis | Un broker más que puede estar caído sin que nadie mire, y el estado del trabajo vive fuera de `consultas`. Con un cliente y unas pocas consultas al día no aporta nada. |
| huey con `SqliteHuey` | Más ligero, mismo defecto de fondo: sus tablas son un segundo sitio donde un trabajo puede existir sin fila, o una fila sin trabajo. |
| Timer de systemd con un comando *oneshot* | Sin leases ni control de concurrencia, y encaja mal con trabajos de horas. |
| Rescate al arranque dentro del `lifespan`, **sin cola** | Es una red debajo del defecto, no el defecto. Deja intacto lo que más molesta: que desplegar la API mate las descargas. (No confundir con la variante de §6.) |

> **El argumento decisivo: una sola fuente de verdad.** `reconcile_downloads`,
> `tools/get_query.py` y el panel de Django ya leen la tabla `consultas`. Cualquier broker crea
> un segundo sitio donde el estado puede divergir — que es exactamente la clase de puntero
> perdido que costó 1.1 TB en agosto.

---

## 11. Orden de trabajo

| Paso | Alcance | Riesgo |
|---|---|---|
| 1.1 Migración | 2 archivos, aditivo | Bajo · patrón que ya existe |
| 1.2 Primitivas | `database.py`, 5 métodos | Bajo · nadie las llama todavía |
| 1.3 Pruebas | módulo nuevo | Ninguno |
| 2.1 `worker.py` | archivo nuevo | Medio · reusa el pipeline entero sin tocarlo |
| 2.2 Podar `main.py` | 2 endpoints + lifespan | Medio · es el corte |
| 2.3 Unit | archivo nuevo + `server.sh` | Medio · trampas de despliegue conocidas |
| 2.4 `/health` | tabla + endpoint | Bajo |
| 2.5 Docs | 4 archivos | Ninguno |

La entrega 1 se puede mezclar y desplegar sola sin cambiar nada del comportamiento, y la 2
esperar a que haya hueco para vigilar el corte. Si la 2 nunca llega, la 1 no estorba: son cuatro
columnas sin usar.

---

*Entrega 1 implementada y en `fix/ci-y-timeouts`, junto al arreglo del CI y la API key en
`POST /query`. La entrega 2 sigue siendo propuesta, y §6 sigue sin decidir.*
