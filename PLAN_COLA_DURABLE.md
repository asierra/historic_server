# Plan: cola durable de consultas

Sacar el procesamiento del proceso que sirve HTTP, y convertir la tabla `consultas` en una
cola con *leases*. Sin broker, sin estados nuevos, sin tocar `historic_query`.

| | |
|---|---|
| **Rama base** | `fix/ci-y-timeouts` (`d6d64aa`) |
| **Fecha** | 24-ago-2026 · entrega 1 implementada y §6 decidida el 25-ago |
| **Estado** | **Entrega 1 hecha** (§4): esquema y primitivas, sin cambio de comportamiento, ya en `main`. **Entrega 2 pendiente**, y será de **un solo servicio**: §6 quedó decidida con datos de tahan. |

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

Persistir la cola no basta si nadie apunta **quién** tiene cada consulta. El cambio de fondo es
que la propiedad del trabajo se escriba en la fila: quién la tiene y hasta cuándo. Con eso, que
un proceso muera deja de ser un evento especial —es sólo un lease que nadie renueva— y la
recuperación deja de depender de que una persona se dé cuenta y pulse un botón.

```
HOY · el trabajo vive en memoria       DESPUÉS · el trabajo vive en la fila
┌──────────────────────────────┐       ┌──────────────────────────────┐
│ gunicorn -w 4                │       │ gunicorn -w 4                │
│ HTTP + BackgroundTasks       │       │ HTTP + bucle de cola         │
└──────────────────────────────┘       └──────────────────────────────┘
          ↓ reiniciar                            ↓ reiniciar
┌──────────────────────────────┐       ┌──────────────────────────────┐
│ trabajo perdido              │       │ el lease vence, otro la coge │
│ fila congelada, sin dueño    │       │ ≤15 min y sigue sola         │
│ hasta que alguien lo note    │       │ sin que nadie intervenga     │
└──────────────────────────────┘       └──────────────────────────────┘
```

Lo que se gana, en orden de importancia real:

1. **Una consulta deja de poder quedarse congelada para siempre.** Es el defecto que originó
   todo esto: `GkpH6xne`, 29 horas al 89 %, esperando a que una persona lo viera.
2. **El chequeo manual antes de cada despliegue desaparece.** Hoy hay que mirar si queda algo en
   `recibido`/`procesando` antes de reiniciar tahan, y acordarse de hacerlo. Con leases da igual:
   lo que se corte se reanuda solo.
3. **Un fallo deja de ser terminal o eterno.** `intentos` con tope y `disponible_desde` con
   backoff: se reintenta unas cuantas veces con espera y luego para, en vez de no reintentar
   nunca (hoy) o girar sin fin.
4. **Un solo camino por el que arranca trabajo.** Todo pasa por `reclamar_siguiente()`, así que
   no hay dos productores de tareas que mantener en sintonía.

**No es sólo añadir.** `background_tasks.add_task` sale de los dos endpoints.
`reclamar_para_reproceso()` se disuelve en el reclamo genérico. `/restart` se vuelve «pon
`recibido`, limpia el lease». La estimación es que `main.py` sale en negativo neto de líneas.

> **Lo que la tesis ya no dice.** Hasta el 25-ago esta sección defendía **separar la API del
> worker en dos procesos**. Se evaluó con datos de tahan y se descartó (§6): compraba media hora
> al año a cambio de una unidad de systemd con precedente de fallo silencioso. La cola se queda;
> el corte en dos, no.

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

## 5. Entrega 2 — el corte (un solo servicio)

> Escrita para la variante decidida en §6. La versión de dos procesos —`worker.py` con su unidad
> de systemd— está descartada; lo que sigue la sustituye.

### 2.1 El bucle en el `lifespan`

Un hilo por proceso de gunicorn, arrancado desde el `lifespan` que ya existe. Reutiliza el
`ConsultasDatabase`, el `ProcessPool` y el `RecoverFiles` que la API ya construye ahí, así que no
hay configuración nueva ni un segundo sitio donde ajustar `MAX_WORKERS`.

1. `liberar_expiradas()`
2. `reclamar_siguiente()`; si no hay nada, dormir `POLL_S` (5 s) y repetir
3. Procesar con `recover.procesar_consulta(id, query)` — el pipeline no se toca
4. Si lanza excepción, `fallar_con_reintento()`

**Un hilo, no un `asyncio.Task`.** El pipeline es bloqueante de principio a fin (`ProcessPool`,
I/O de disco, `boto3`), así que dentro del bucle de eventos ahogaría a uvicorn. Va en un
`threading.Thread(daemon=True)` con un `threading.Event` para el apagado.

**Cuatro bucles, uno por worker de gunicorn.** Es seguro por el mismo `UPDATE` atómico de §3.2 —
lo cubre `test_varios_workers_a_la_vez_no_se_pisan`— y **no es una regresión de concurrencia**:
hoy ya puede haber cuatro descargas a la vez, porque cada `POST` lanza su `BackgroundTask` en el
worker que lo atienda. Lo único nuevo es que la cola drena sola, así que un primer arranque con
cola acumulada sacaría cuatro de golpe; lo acota la decisión de §8 sobre lo muy viejo.

**Verificación:** prueba de integración con el simulador —encolar sin `add_task` y comprobar que
el bucle la termina—, y una consulta real de punta a punta en tren2 antes de tocar tahan.

### 2.1-bis Apagado limpio

En el cierre del `lifespan`: señalar el `Event`, esperar un margen corto a que el bucle suelte la
consulta en curso con `reencolar()`, y seguir. No hay que apurar: si el margen no basta, o si
llega un `SIGKILL`, el lease caduca solo y otro la recoge. Es toda la gracia de tener custodia
escrita en la fila en vez de en memoria.

Lo que **sí** hay que respetar es el orden — soltar el lease antes de cerrar el `ProcessPool`,
para no dejar una consulta reclamada por un proceso que ya no puede avanzarla.

### 2.2 Podar `main.py`

- `POST /query` — quitar `background_tasks.add_task`; el `INSERT` ya deja la consulta encolada.
- `POST /query/{id}/restart` — `reclamar_para_reproceso()` → `reencolar()`; el cerrojo lo hereda
  de la cola.
- `lifespan` — el `ProcessPool` y `RecoverFiles` **se quedan**: los usa el bucle. Lo que se
  añade es arrancar el hilo y pararlo.
- `BackgroundTasks` desaparece de los *imports* y de las dos firmas.

Lo que **no** cambia: el 409 del purge sigue haciendo falta —el bucle puede estar escribiendo en
ese directorio ahora mismo, esté en el mismo proceso o no— y `ESTADOS_EN_VUELO` se queda como
está. `server.sh` tampoco cambia: sigue habiendo un solo proceso que matar.

### 2.3 Que `/health` no empeore

El circuit breaker de S3 es un singleton de módulo, y hoy `/health` ya miente a medias: cada uno
de los 4 workers de gunicorn tiene el suyo y se enseña el del que conteste. Con el bucle en el
mismo proceso eso no empeora —el breaker sigue donde está—, pero conviene añadir a `/health` lo
que ahora sí se puede saber: si el hilo de la cola está vivo, y cuántas consultas hay en
`recibido` y en `procesando`.

Una cola que crece con todos los hilos muertos es el modo de fallo de esta variante, y es el
único que no se ve desde fuera: la API responde 202 tan contenta.

**Verificación:** matar el hilo a mano y comprobar que `/health` lo reporta, en vez de callar.

### 2.4 Documentación

- `CLAUDE.md` — la sección de arquitectura describe `BackgroundTasks` en memoria como un hecho
  central, y el bloque de estados explica `ESTADOS_EN_VUELO` y `/restart` a partir de eso. Hay
  que reescribir ambos.
- `README.md` — `/restart` cambia de significado: ya no reencola *y* lanza, sólo reencola.
- `DEPLOYMENT_GUIDE.md` — no hay unidad nueva, pero sí un paso nuevo: `migrate_db.py`.

---

## 6. Un servicio o dos — **decidido: uno**

> **Decidido el 25-ago-2026 con datos de tahan.** El bucle vive en el proceso de la API. No hay
> `worker.py` ni unidad nueva. Lo que sigue conserva el razonamiento y los números, porque la
> decisión se puede revisar y entonces conviene saber sobre qué se tomó.

Lo que estaba en juego era **quién corre el bucle**, no la cola.

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

**Resultado: un servicio.** Lo que sigue es el razonamiento; los números de tahan que lo
cerraron están al final.

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

### Los números de tahan (25-ago), que cerraron la decisión

**Reinicios: 4 en 6 meses.** Unos 8 al año. El dato que importa no es ése sino que **uno de esos
cuatro fue el que congeló a `GkpH6xne`**: una colisión de cada cuatro reinicios, pero del orden
de dos consultas congeladas al año en términos absolutos.

**Duraciones: la muestra no sirve, y saberlo es parte del resultado.** Las seis filas que quedan
en tahan son todas de menos de dos minutos, pero son las que *sobrevivieron*: la purga del 24-ago
se llevó las seis grandes de la estudiante (34 657 archivos, 1.1 TB entre las seis) y `GkpH6xne`
se borró el 19-ago. La tabla perdió exactamente las consultas largas, que son las únicas que un
reinicio puede pillar. La magnitud real la da la bitácora: `GkpH6xne` llevaba 1073 archivos y
20 GB sin haber llegado a la mitad, o sea horas.

**La cuenta.** Dos servicios ahorrarían ≤15 min de reanudación unas dos veces al año —media hora
anual— a cambio de una unidad de systemd cuyo modo de fallo silencioso tiene precedente
demostrado en esta misma máquina. No se sostiene. Y lo que sí importaba —la consulta congelada
para siempre, y tener que darse cuenta a mano— lo arregla la cola en las dos variantes por igual.

**Qué invertiría la decisión:** que los despliegues pasen a ser frecuentes, que aparezca la
necesidad real de frenar las descargas sin tirar la API, o que una sola máquina deje de dar
abasto. Ninguna de las tres está cerca hoy.

> **La decisión no costó nada aplazarla, y tampoco costaría revertirla.** La entrega 1 es
> idéntica en los dos caminos: las cuatro columnas, el reclamo, los leases, los reintentos y sus
> pruebas no cambian ni una línea según quién corra el bucle. Si algún día hay que volver a los
> dos procesos, es sacar el bucle de §5 a un `worker.py` y escribir su unidad — nada de lo ya
> hecho se rehace.

**Si alguna vez se vuelve a dos servicios**, hay un detalle de systemd que es fácil errar y que
destruiría el beneficio entero:

- `Wants=historic-worker.service` en la API — arrancar la API arranca el worker, y **reiniciar
  la API no reinicia el worker**. Es la correcta.
- `PartOf=` o `BindsTo=` — reiniciar la API reinicia el worker, que es justo lo que se estaría
  intentando evitar.

Y `server.sh` quedaría desfasado: su `pkill -9 -f "gunicorn.*$APP"` no tocaría al worker, así que
un `restart` por esa vía dejaría el worker huérfano con la API nueva.

> **No confundir con lo descartado en §10.** Lo que se descarta ahí es un *rescate* dentro del
> `lifespan` **sin cola**: un barrido que reencola huérfanas y ya. Lo decidido aquí es la cola
> completa —leases, reintentos, backoff, una sola fuente de verdad— y sólo comparte con aquélla
> el sitio donde corre. La diferencia está desarrollada en §10.

---

## 7. Despliegue

**La entrega 1 se despliega sola**, y es lo acordado el 25-ago. Son tres pasos y no cambia el
comportamiento: respaldo del `.db` (`sqlite3 .backup` + gzip, como el cron), `migrate_db.py`, y
desplegar el código. Nadie llama todavía a las primitivas nuevas, así que revertir es un `git
checkout`: las cuatro columnas se quedan sin usar y no estorban.

### El corte de la entrega 2

Con un solo servicio es un despliegue normal: desplegar y reiniciar. La propiedad que lo hace
seguro es que **una consulta encolada no se pierde aunque nadie la procese todavía**, así que la
ventana entre el reinicio y el primer tick del bucle no tiene consecuencias.

| # | Paso | Si sale mal |
|---|---|---|
| 1 | Respaldo del `.db` (`sqlite3 .backup` + gzip, como el cron) | — |
| 2 | Desplegar y reiniciar. Lo que hubiera en vuelo se corta, y el bucle lo recoge al vencer su lease | Revertir el código y reiniciar; las filas encoladas las recoge el `/restart` de siempre |
| 3 | Verificar en el journal un ciclo completo: liberación, reclamo, avance, `completado` | — |
| 4 | Reiniciar a propósito con una descarga en curso, y comprobar que **se reanuda sola** en ≤ un lease | Es el criterio de aceptación de toda la entrega; si no pasa, no está hecha |

El paso 4 es el que de verdad prueba el cambio. Conviene hacerlo con una consulta que tarde lo
bastante para dar tiempo a reiniciar a mitad — y ahí las de tahan de menos de dos minutos no
sirven: hace falta uno de los rangos grandes.

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
| **Los hilos mueren y la cola crece en silencio.** Es el modo de fallo propio de esta variante: la API sigue aceptando y respondiendo 202 | `/health` expone si el hilo vive y cuánto hay encolado (2.3). Sin eso, no se ve desde fuera |
| Un hilo bloqueante conviviendo con uvicorn en el mismo proceso | El trabajo pesado ya está en el `ProcessPool`; el hilo sólo orquesta. Aun así hay que medir la latencia de `GET /query/{id}` con una descarga en curso |
| Escritura concurrente sobre SQLite desde cuatro procesos | WAL ya está activo y el `timeout` de conexión es 30 s; el volumen es de unas pocas consultas al día. Lo cubre `test_varios_workers_a_la_vez_no_se_pisan` |
| Una consulta que tumba al proceso una y otra vez | `intentos` con tope la manda a `error` en vez de dejarla girando — y cuenta en el reclamo, así que también atrapa las que mueren sin poder registrar nada |
| Al arrancar con cola acumulada salen cuatro de golpe | La decisión de §8 sobre lo muy viejo acota qué entra; conviene desplegar con la cola vacía la primera vez |

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
| **Dos procesos**: la API encola, un `worker.py` con su unidad consume | Evaluado con datos el 25-ago y descartado: ahorra media hora al año a cambio de una unidad de systemd con precedente de fallo silencioso en esta misma máquina. El razonamiento completo y los números están en §6. |
| Rescate al arranque dentro del `lifespan`, **sin cola** | Corre en el mismo sitio que lo decidido en §6, y ahí acaba el parecido. No apunta quién tiene cada consulta, así que si el proceso vuelve a morir se está igual y sólo salva el *siguiente* arranque; no cuenta intentos, así que una consulta que reviente el proceso se reencola en cada arranque —con `Restart=always`, un bucle que se alimenta solo—; no espera entre reintentos; y deja dos caminos por los que arranca trabajo en vez de uno. |

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
| 2.1 Bucle en el `lifespan` | `main.py` + apagado limpio | Medio · reusa el pipeline sin tocarlo |
| 2.2 Podar `main.py` | 2 endpoints | Medio · es el corte |
| 2.3 `/health` | endpoint | Bajo · pero es lo único que hace visible el fallo de 2.1 |
| 2.4 Docs | 3 archivos | Ninguno |

**1.1 a 1.3 están hechas y en `main`** (`5b8fccc`), desplegables solas: no cambian el
comportamiento. La 2 puede esperar a que haya hueco para vigilar el corte, y si nunca llega, la 1
no estorba — son cuatro columnas sin usar.

---

*Entrega 1 implementada y en `main` (`5b8fccc`), junto al arreglo del CI y la API key en
`POST /query`. §6 decidida el 25-ago con datos de tahan: un solo servicio. La entrega 2 sigue
siendo propuesta.*
