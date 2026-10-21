#!/usr/bin/env python3
"""Migra 2026 al árbol diario de Lustre (conus y fd, L1b y L2).

Origen, dos estructuras:

  semanal    /depot/goes16/abi/{nivel}/{dominio}/2026/SS/*.tgz
             un .tgz por instante; días 001-077 de 2026.
  temporal   /depot/goes-east/abi/{nivel}/{dominio}/2026/JJJ/*.nc
             .nc sueltos, todo en una carpeta; días 076-083.

Destino, el árbol que ya lee el recuperador (historic_server, recover.py):

  /depot/goes-east/{nivel}/abi/{dominio}/2026/JJJ/
      l1b: los .nc sueltos.
      l2:  una subcarpeta por producto con la letra del dominio y, si el
           nombre trae banda, `-Cbb`: ACHAC/, CMIPC-C13/, DMWC-C07/.

Espacio: L1b ocupa lo mismo (sus .nc ya vienen comprimidos; el .tgz no
ganaba nada). Los L2 de los .tgz y de la estructura temporal NO traen
compresión interna —el gzip los reducía 4-5x—, así que se comprimen con
`nccopy -d 4 -s` (deflate 4 + shuffle, lo que trae hoy el árbol diario).
Medido en cirrus el 21-oct-2026: un .tgz L2 fd de 1254 MB queda en 1017 MB,
uno conus de 145 MB en 124 MB, y la copia temporal baja a ~1/4. Sin
comprimir, los ~16.6 TB de .tgz de L2 serían ~75 TB y llenarían /depot.

Tiempo: comprimir es CPU (nccopy, un núcleo por archivo). Con un proceso,
~103 s por .tgz L2 fd y ~15 s por conus: ~750 h-CPU en total, así que hay
que darle procesos (cirrus tiene 48 núcleos). Cada proceso retiene en
staging el día que trabaja (~150 GB un día L2 fd) hasta borrar sus .tgz.

Cómo lo hace, por día (nivel, dominio, JJJ):

  1. Arma el día en  /depot/goes-east/.migracion/staging/...  extrayendo
     sus .tgz y tomando los .nc de la estructura temporal (rename en L1b;
     en L2, comprimidos).
  2. Si el día no tiene errores ni conflictos, lo pasa al destino con un
     solo rename. El recuperador nunca ve un día a medias: si viera la
     carpeta del día ya no buscaría en el árbol semanal y le faltarían
     archivos.
  3. Borra cada .tgz sólo cuando todos sus miembros están en destino con el
     tamaño esperado (salvo --conservar-origen), y la carpeta temporal del
     día si quedó vacía.

Se puede cortar y volver a lanzar: lo extraído se apunta en un manifiesto por
.tgz (.migracion/manifiestos/) y lo ya hecho se salta. Sin --ejecutar sólo
dice qué haría.

Uso, en cirrus como lanotadm:

  python3 migrar_2026.py                       # simulacro de todo
  python3 migrar_2026.py --solo l1b/conus --dias 076-083 --ejecutar
  nohup nice -n 19 python3 migrar_2026.py --ejecutar --procesos 32 \\
      > ~/migrar_2026.out 2>&1 &

Escrito para Python 3.6 (el de cirrus).
"""

import argparse
import errno
import fcntl
import json
import logging
import multiprocessing
import os
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import time

ANIO = 2026
NIVELES = ("l1b", "l2")
DOMINIOS = ("conus", "fd")

_TGZ_DIA = re.compile(r"-s(\d{4})(\d{3})\d{4}\.tgz$")
_NC_DIA = re.compile(r"_s(\d{4})(\d{3})\d+_e")
# CG_ABI-L2-CMIPC-M6C13_G19_s... -> ("CMIPC", "13");  ..-L2-ACHAC-M4_G19.. -> ("ACHAC", None)
_L2_PRODUCTO = re.compile(r"^[A-Z]{2}_ABI-L2-([A-Z0-9]+)-M\d(?:C(\d{2}))?_")

log = logging.getLogger("migrar_2026")


# --- Reglas de nombres --------------------------------------------------------

def subcarpeta(nivel, nombre):
    """Subcarpeta del día donde va un .nc: "" en L1b, None si no se reconoce."""
    if nivel == "l1b":
        return ""
    m = _L2_PRODUCTO.match(nombre)
    if not m:
        return None
    producto, banda = m.groups()
    return producto + ("-C" + banda if banda else "")


def dia_de_nc(nombre):
    m = _NC_DIA.search(nombre)
    return (m.group(1), m.group(2)) if m else None


# --- Rutas --------------------------------------------------------------------

class Rutas(object):
    def __init__(self, vieja, nueva):
        self.vieja = vieja
        self.nueva = nueva
        self.trabajo = os.path.join(nueva, ".migracion")

    def semanal(self, nivel, dominio):
        return os.path.join(self.vieja, "abi", nivel, dominio, str(ANIO))

    def temporal(self, nivel, dominio, jjj=None):
        base = os.path.join(self.nueva, "abi", nivel, dominio, str(ANIO))
        return os.path.join(base, jjj) if jjj else base

    def final(self, nivel, dominio, jjj):
        return os.path.join(self.nueva, nivel, "abi", dominio, str(ANIO), jjj)

    def staging(self, nivel, dominio, jjj):
        return os.path.join(self.trabajo, "staging", nivel, dominio, str(ANIO), jjj)

    def manifiesto(self, nivel, dominio, tgz):
        return os.path.join(self.trabajo, "manifiestos", nivel, dominio, str(ANIO),
                            os.path.basename(tgz) + ".json")


# --- Descubrimiento -----------------------------------------------------------

def descubrir(rutas, combinaciones, dias):
    """{(nivel, dominio, jjj): {"tgz": [...], "temporal": ruta | None}}"""
    trabajo = {}

    def entrada(clave):
        return trabajo.setdefault(clave, {"tgz": [], "temporal": None})

    for nivel, dominio in combinaciones:
        semanal = rutas.semanal(nivel, dominio)
        if os.path.isdir(semanal):
            for semana in sorted(os.listdir(semanal)):
                carpeta = os.path.join(semanal, semana)
                if not os.path.isdir(carpeta):
                    continue
                for nombre in sorted(os.listdir(carpeta)):
                    m = _TGZ_DIA.search(nombre)
                    if not m or int(m.group(1)) != ANIO:
                        continue
                    jjj = m.group(2)
                    if dias and jjj not in dias:
                        continue
                    entrada((nivel, dominio, jjj))["tgz"].append(os.path.join(carpeta, nombre))
        temporal = rutas.temporal(nivel, dominio)
        if os.path.isdir(temporal):
            for jjj in sorted(os.listdir(temporal)):
                if not re.match(r"^\d{3}$", jjj) or (dias and jjj not in dias):
                    continue
                entrada((nivel, dominio, jjj))["temporal"] = os.path.join(temporal, jjj)
    return trabajo


# --- Operaciones de archivo ---------------------------------------------------

def _tamano(ruta):
    try:
        return os.stat(ruta).st_size
    except OSError:
        return None


def _makedirs(ruta):
    try:
        os.makedirs(ruta)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def _escribir_json(ruta, datos):
    _makedirs(os.path.dirname(ruta))
    tmp = ruta + ".part"
    with open(tmp, "w") as f:
        json.dump(datos, f)
    os.replace(tmp, ruta)


def _leer_json(ruta):
    try:
        with open(ruta) as f:
            return json.load(f)
    except (IOError, OSError, ValueError):
        return None


def _barrer_parciales(raiz):
    for dirpath, _, archivos in os.walk(raiz):
        for a in archivos:
            if a.endswith(".part"):
                os.remove(os.path.join(dirpath, a))


def _quitar_vacias(raiz):
    """Borra las carpetas vacías bajo raiz, de abajo arriba, raiz incluida."""
    for dirpath, _, _ in sorted(os.walk(raiz), key=lambda t: -len(t[0])):
        try:
            os.rmdir(dirpath)
        except OSError:
            pass


# --- Migración de un día ------------------------------------------------------

def _comprimir(compresor, origen, destino):
    """nccopy -d 4 -s: deflate 4 con shuffle, como el árbol diario actual.

    Los L2 dentro de los .tgz viejos y en la estructura temporal no traen
    compresión interna (el gzip del .tgz los reducía 4-5x); sin esto, los
    ~16.6 TB de .tgz de L2 se volverían ~75 TB extraídos.
    """
    r = subprocess.run(list(compresor) + [origen, destino], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if r.returncode != 0:
        if os.path.exists(destino):
            os.remove(destino)
        raise IOError("%s falló (%d): %s" % (compresor[0], r.returncode,
                                             r.stderr.decode("utf-8", "replace").strip()[:300]))


def migrar_dia(rutas, nivel, dominio, jjj, fuentes, ejecutar, borrar, forzar, compresor=("nccopy", "-d", "4", "-s")):
    st = {"dia": "%s/%s/%s" % (nivel, dominio, jjj), "tgz": len(fuentes["tgz"]),
          "extraidos": 0, "movidos": 0, "duplicados": 0, "ya_en_destino": 0,
          "conflictos": [], "errores": [], "tgz_borrados": 0, "estado": ""}
    final = rutas.final(nivel, dominio, jjj)
    staging = rutas.staging(nivel, dominio, jjj)
    temporal = fuentes["temporal"]

    if not ejecutar:
        n_temp = len(os.listdir(temporal)) if temporal else 0
        st["estado"] = "simulacro: %d tgz, %d .nc en temporal, destino %s" % (
            len(fuentes["tgz"]), n_temp, "existe (se fusiona)" if os.path.isdir(final) else "nuevo")
        return st

    _makedirs(staging)
    _barrer_parciales(staging)

    comprimir = nivel == "l2"

    def ya_esta(rel, tam):
        """¿rel ya está en staging o en destino? Devuelve su tamaño o None.

        En L1b se exige el tamaño del origen. En L2 no se puede comparar: al
        comprimir cambia. Basta con que exista, porque nada llega a su nombre
        final si no es completo (siempre .part y rename).
        """
        for base in (staging, final):
            t = _tamano(os.path.join(base, rel))
            if t is not None and (comprimir or t == tam):
                return t
        return None

    def colocar(origen, rel, mtime):
        """Pone `origen` en staging/rel: tal cual en L1b, comprimido en L2."""
        destino = os.path.join(staging, rel)
        _makedirs(os.path.dirname(destino))
        parcial = destino + ".part"
        if comprimir:
            _comprimir(compresor, origen, parcial)
        else:
            os.rename(origen, parcial)
        os.utime(parcial, (mtime, mtime))
        os.replace(parcial, destino)
        return _tamano(destino)

    # 1a. Estructura temporal: L1b con rename; L2 se comprime al staging y se
    #     borra el original (la copia temporal de L2 no está comprimida).
    if temporal and os.path.isdir(temporal):
        for nombre in sorted(os.listdir(temporal)):
            origen = os.path.join(temporal, nombre)
            if not (nombre.endswith(".nc") and os.path.isfile(origen)):
                continue
            sub = subcarpeta(nivel, nombre)
            if sub is None or dia_de_nc(nombre) != (str(ANIO), jjj):
                st["conflictos"].append("irreconocible o de otro día: %s" % origen)
                continue
            rel = os.path.join(sub, nombre)
            if ya_esta(rel, _tamano(origen)) is not None:
                os.remove(origen)
                st["duplicados"] += 1
                continue
            if os.path.exists(os.path.join(staging, rel)) or os.path.exists(os.path.join(final, rel)):
                st["conflictos"].append("mismo nombre, otro tamaño: %s" % origen)
                continue
            try:
                colocar(origen, rel, os.stat(origen).st_mtime)
            except (IOError, OSError) as e:
                st["errores"].append("%s: %s" % (origen, e))
                continue
            if comprimir:
                os.remove(origen)
            st["movidos"] += 1

    # 1b. Árbol semanal: extraer cada .tgz al staging (y comprimir, en L2).
    for tgz in fuentes["tgz"]:
        ruta_man = rutas.manifiesto(nivel, dominio, tgz)
        man = _leer_json(ruta_man)
        if man and man.get("completo"):
            continue
        miembros = []  # [ruta relativa, tamaño final en destino]
        try:
            with tarfile.open(tgz, "r:gz") as tar:
                for m in tar:
                    if not m.isfile():
                        continue
                    nombre = os.path.basename(m.name)
                    if not nombre.endswith(".nc"):
                        continue
                    sub = subcarpeta(nivel, nombre)
                    if sub is None or dia_de_nc(nombre) != (str(ANIO), jjj):
                        st["conflictos"].append("irreconocible o de otro día: %s en %s" % (nombre, tgz))
                        continue
                    rel = os.path.join(sub, nombre)
                    tam = ya_esta(rel, m.size)
                    if tam is not None:
                        miembros.append([rel, tam])
                        st["ya_en_destino"] += 1
                        continue
                    if os.path.exists(os.path.join(staging, rel)) or os.path.exists(os.path.join(final, rel)):
                        st["conflictos"].append("mismo nombre, otro tamaño: %s en %s" % (nombre, tgz))
                        continue
                    crudo = os.path.join(staging, rel) + ".crudo.part"
                    _makedirs(os.path.dirname(crudo))
                    with tar.extractfile(m) as src, open(crudo, "wb") as dst:
                        shutil.copyfileobj(src, dst, 4 * 1024 * 1024)
                    if _tamano(crudo) != m.size:
                        raise IOError("tamaño distinto al extraer %s" % nombre)
                    try:
                        miembros.append([rel, colocar(crudo, rel, m.mtime)])
                    finally:
                        if os.path.exists(crudo):
                            os.remove(crudo)
                    st["extraidos"] += 1
        except (tarfile.TarError, IOError, OSError, EOFError) as e:
            st["errores"].append("%s: %s" % (tgz, e))
            continue
        _escribir_json(ruta_man, {"tgz": tgz, "miembros": miembros, "completo": True})

    # 2. Pasar el día al destino.
    if (st["errores"] or st["conflictos"]) and not forzar:
        st["estado"] = "retenido en staging (errores o conflictos; revise y relance, o --forzar)"
        return st
    if not os.path.isdir(final):
        _makedirs(os.path.dirname(final))
        os.rename(staging, final)
    else:
        for dirpath, _, archivos in os.walk(staging):
            for a in archivos:
                origen = os.path.join(dirpath, a)
                rel = os.path.relpath(origen, staging)
                destino = os.path.join(final, rel)
                if not os.path.exists(destino):
                    _makedirs(os.path.dirname(destino))
                    os.rename(origen, destino)
                elif _tamano(destino) == _tamano(origen):
                    os.remove(origen)
                else:
                    st["conflictos"].append("en destino con otro tamaño: %s" % destino)
        _quitar_vacias(staging)
        if st["conflictos"] and not forzar:
            st["estado"] = "fusionado con conflictos; origen intacto"
            return st

    # 3. Borrar el origen ya verificado.
    if borrar:
        for tgz in fuentes["tgz"]:
            man = _leer_json(rutas.manifiesto(nivel, dominio, tgz))
            if not (man and man.get("completo")) or not os.path.exists(tgz):
                continue
            faltan = [rel for rel, tam in man["miembros"]
                      if _tamano(os.path.join(final, rel)) != tam]
            if faltan:
                st["errores"].append("%s: %d miembros sin verificar en destino; no se borra" % (tgz, len(faltan)))
                continue
            os.remove(tgz)
            man["borrado"] = time.strftime("%Y-%m-%dT%H:%M:%S")
            _escribir_json(rutas.manifiesto(nivel, dominio, tgz), man)
            st["tgz_borrados"] += 1
        if temporal and os.path.isdir(temporal):
            try:
                os.rmdir(temporal)
            except OSError:
                st["conflictos"].append("la carpeta temporal no quedó vacía: %s" % temporal)
    st["estado"] = "migrado"
    return st


# --- Programa -----------------------------------------------------------------

def _configurar_log(ruta=None):
    fmt = logging.Formatter("%(asctime)s %(processName)s %(levelname)s %(message)s")
    log.setLevel(logging.INFO)
    log.handlers = []
    manejadores = [logging.StreamHandler(sys.stdout)]
    if ruta:
        manejadores.append(logging.FileHandler(ruta))
    for h in manejadores:
        h.setFormatter(fmt)
        log.addHandler(h)


def _rango_dias(texto):
    dias = set()
    for parte in texto.split(","):
        if "-" in parte:
            a, b = parte.split("-")
            dias.update("%03d" % d for d in range(int(a), int(b) + 1))
        else:
            dias.add("%03d" % int(parte))
    return dias


def _trabajar(args):
    rutas, (nivel, dominio, jjj), fuentes, ejecutar, borrar, forzar, compresor = args
    t = time.time()
    try:
        st = migrar_dia(rutas, nivel, dominio, jjj, fuentes, ejecutar, borrar, forzar, compresor)
    except Exception as e:  # que un día raro no tumbe a los demás
        st = {"dia": "%s/%s/%s" % (nivel, dominio, jjj), "estado": "falló",
              "errores": ["%s: %s" % (type(e).__name__, e)], "conflictos": []}
    st["segundos"] = round(time.time() - t, 1)
    return st


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--ejecutar", action="store_true", help="hacer los cambios (sin esto, simulacro)")
    p.add_argument("--conservar-origen", action="store_true", help="no borrar .tgz ni carpetas temporales")
    p.add_argument("--forzar", action="store_true", help="pasar al destino días con errores o conflictos")
    p.add_argument("--solo", action="append", metavar="NIVEL/DOMINIO", help="p. ej. l1b/conus; repetible")
    p.add_argument("--dias", help="días del año: 001-083, 76,77, ...")
    p.add_argument("--procesos", type=int, default=4)
    p.add_argument("--raiz-vieja", default="/depot/goes16")
    p.add_argument("--raiz-nueva", default="/depot/goes-east")
    p.add_argument("--compresor", default="nccopy -d 4 -s",
                   help="orden para comprimir L2: recibe origen y destino al final")
    a = p.parse_args(argv)
    compresor = tuple(shlex.split(a.compresor))

    rutas = Rutas(a.raiz_vieja, a.raiz_nueva)
    if a.ejecutar:
        _makedirs(rutas.trabajo)
        _configurar_log(os.path.join(rutas.trabajo, "migrar_2026.log"))
        candado = open(os.path.join(rutas.trabajo, "migrar_2026.lock"), "w")
        try:
            fcntl.flock(candado, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            log.error("Ya hay otra migración corriendo (%s). Salgo.", candado.name)
            return 2
    else:
        _configurar_log()  # el simulacro no escribe nada en /depot

    combinaciones = [(n, d) for n in NIVELES for d in DOMINIOS]
    if a.solo:
        pedidas = {tuple(s.split("/")) for s in a.solo}
        combinaciones = [c for c in combinaciones if c in pedidas]
    dias = _rango_dias(a.dias) if a.dias else None

    trabajo = descubrir(rutas, combinaciones, dias)
    log.info("%s: %d días por migrar (%d .tgz, %d carpetas temporales), %d procesos",
             "EJECUCIÓN" if a.ejecutar else "SIMULACRO", len(trabajo),
             sum(len(f["tgz"]) for f in trabajo.values()),
             sum(1 for f in trabajo.values() if f["temporal"]), a.procesos)

    if any(k[0] == "l2" for k in trabajo) and not shutil.which(compresor[0]):
        log.error("No encuentro %r para comprimir L2. Instálelo o use --solo con l1b.", compresor[0])
        return 2
    tareas = [(rutas, k, trabajo[k], a.ejecutar, a.ejecutar and not a.conservar_origen, a.forzar, compresor)
              for k in sorted(trabajo)]
    totales = {"migrado": 0, "retenido": 0, "falló": 0}
    resumenes = os.path.join(rutas.trabajo, "resumen")
    pool = multiprocessing.Pool(a.procesos) if a.procesos > 1 else None
    try:
        for st in (pool.imap_unordered(_trabajar, tareas) if pool else map(_trabajar, tareas)):
            nivel = logging.WARNING if (st.get("errores") or st.get("conflictos")) else logging.INFO
            log.log(nivel, "%s  %s  (%ss) %s", st["dia"], st["estado"], st.get("segundos"),
                    {k: v for k, v in st.items() if k in ("tgz", "extraidos", "movidos", "duplicados",
                                                         "ya_en_destino", "tgz_borrados")})
            for x in st.get("errores", []) + st.get("conflictos", []):
                log.warning("    %s", x)
            if a.ejecutar:
                _escribir_json(os.path.join(resumenes, st["dia"].replace("/", "_") + ".json"), st)
            if st["estado"].startswith("simulacro"):
                continue
            clave = "migrado" if st["estado"] == "migrado" else ("falló" if st["estado"] == "falló" else "retenido")
            totales[clave] += 1
    finally:
        if pool:
            pool.close()
            pool.join()
    if a.ejecutar:
        log.info("Fin: %d días migrados, %d retenidos, %d fallidos", totales["migrado"], totales["retenido"], totales["falló"])
    return 1 if (totales["retenido"] or totales["falló"]) else 0


if __name__ == "__main__":
    sys.exit(main())
