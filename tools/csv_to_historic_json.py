import csv
import json
import argparse
from datetime import datetime
import re

def parse_date_mmddyyyy(s: str) -> str:
    """
    Acepta MM/DD/YYYY con o sin ceros iniciales (ej. 7/4/2019).
    Devuelve YYYYMMDD.
    """
    s = s.strip()
    parts = s.split("/")
    if len(parts) != 3 or not all(p.strip() for p in parts):
        raise ValueError(f"Fecha inválida: {s}")
    m = int(parts[0])
    d = int(parts[1])
    y = int(parts[2])
    dt = datetime(y, m, d)
    return dt.strftime("%Y%m%d")

def parse_times_cell(cell: str) -> list:
    """
    Extrae horarios en formatos:
    - HH:MM
    - HH:MM-HH:MM (intervalos de tiempo)
    Se admiten múltiples patrones separados por ';', ',', espacios
    y casos concatenados (p.ej. '06:30-09:30:12:00-15:00').
    """
    if not cell:
        return []
    # Encuentra todas las ocurrencias HH:MM o HH:MM-HH:MM
    pattern = re.compile(r'(\d{1,2}:\d{2})(?:\s*-\s*(\d{1,2}:\d{2}))?')
    times = []
    seen = set()
    for m in pattern.finditer(cell):
        start = m.group(1)
        end = m.group(2)
        # Validar formatos
        datetime.strptime(start, "%H:%M")
        if end:
            datetime.strptime(end, "%H:%M")
            token = f"{start}-{end}"
        else:
            token = start
        if token not in seen:
            seen.add(token)
            times.append(token)
    if not times:
        raise ValueError(f"Horario inválido: {cell}")
    return times

def normalize_nivel(n: str) -> str:
    """
    Normaliza el nivel a 'L1b' o 'L2' (case-insensitive).
    """
    if not n:
        raise ValueError("El argumento --nivel es obligatorio.")
    n_up = n.strip().upper()
    if n_up in ("L1B", "L1BLEVEL", "LEVEL1B"):
        return "L1b"
    if n_up in ("L2", "LEVEL2"):
        return "L2"
    raise ValueError(f"Nivel inválido: {n}. Use 'L1b' o 'L2'.")

def convert_csv_to_schema(csv_path: str, sat: str, nivel: str, dominio: str,
                          productos: list, bandas: list, creado_por: str, version: str = None) -> dict:
    fechas = {}
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for rownum, row in enumerate(reader, start=1):
            if not row:
                continue
            if len(row) < 2:
                raise ValueError(f"Fila {rownum}: se requieren al menos 2 columnas (fecha, horarios).")
            date_raw, times_raw = row[0], row[1]

            # Detectar y saltar encabezado (ej. "Fecha", "Date", etc.)
            if rownum == 1:
                try:
                    _ = parse_date_mmddyyyy(date_raw)
                except Exception:
                    # Saltar primera fila si no parsea como fecha
                    continue

            yyyymmdd = parse_date_mmddyyyy(date_raw)
            times_list = parse_times_cell(times_raw)
            if not times_list:
                continue
            fechas.setdefault(yyyymmdd, []).extend(times_list)

    # Normalizaciones
    nivel_norm = normalize_nivel(nivel)
    productos = productos or None
    bandas = bandas or None

    # Construir request y eliminar campos nulos
    request = {
        "sat": sat or None,
        "nivel": nivel_norm,
        "dominio": dominio or None,
        "productos": productos,
        "bandas": bandas,
        "fechas": fechas,
        "creado_por": creado_por or None,
    }
    if version:
        request["version"] = version

    # Remover claves con valor None
    request = {k: v for k, v in request.items() if v is not None}
    return request

def main():
    ap = argparse.ArgumentParser(description="Convierte CSV (MM/DD/YYYY; horarios) a JSON del esquema historic_query_schema.json")
    ap.add_argument("csv", help="Ruta al CSV con columnas: fecha (MM/DD/YYYY), horarios separados por ;")
    ap.add_argument("--sat", default="GOES-16")
    ap.add_argument("--nivel", required=True, help="L1B o L2")
    ap.add_argument("--dominio", required=True, help="fd o conus")
    ap.add_argument("--productos", default="", help="Productos separados por coma, ej: ACHA,CMIP")
    ap.add_argument("--bandas", default="", help="Bandas separadas por coma, ej: 13,02 o ALL")
    ap.add_argument("--creado_por", required=True, help="Email o usuario")
    ap.add_argument("--version", default=None)
    ap.add_argument("--out", default="historic_request.json", help="Ruta de salida JSON")
    args = ap.parse_args()

    productos = [p.strip() for p in args.productos.split(",") if p.strip()] or None
    bandas = [b.strip() for b in args.bandas.split(",") if b.strip()] or None

    data = convert_csv_to_schema(
        csv_path=args.csv,
        sat=args.sat,
        nivel=args.nivel,
        dominio=args.dominio,
        productos=productos,
        bandas=bandas,
        creado_por=args.creado_por,
        version=args.version
    )
    with open(args.out, "w", encoding="utf-8") as out:
        json.dump(data, out, ensure_ascii=False, indent=2)
    print(f"Generado: {args.out}")

if __name__ == "__main__":
    main()