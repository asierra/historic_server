#!/usr/bin/env python3
"""
Extrae la consulta (query) desde la base a partir de un consulta_id.
Puede devolver la consulta almacenada (formato interno con fechas YYYYJJJ)
o la solicitud original como se envió (campo '_original_request' con fechas YYYYMMDD).

Uso básico:
    python tools/get_query.py <consulta_id>

Opciones:
    --db PATH         Ruta a la base de datos SQLite (por defecto usa $DB_PATH o consultas_goes.db)
    --out FILE        Escribe la salida a un archivo en lugar de stdout
    --raw             Imprime minificado (sin pretty-print)
    --original        Devuelve solo el objeto '_original_request' si existe
"""
import argparse
import json
import os
import sqlite3
import sys
from typing import Optional

DEFAULT_DB = os.environ.get("DB_PATH", "consultas_goes.db")


def fetch_query(db_path: str, consulta_id: str) -> Optional[str]:
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT query FROM consultas WHERE id = ?",
                (consulta_id,),
            ).fetchone()
            if not row:
                return None
            return row["query"]
    except Exception as e:
        print(f"[error] No se pudo leer la base de datos '{db_path}': {e}", file=sys.stderr)
        sys.exit(2)


def main():
    ap = argparse.ArgumentParser(description="Exporta la consulta para un consulta_id (interna u original)")
    ap.add_argument("consulta_id", help="ID de la consulta")
    ap.add_argument("--db", default=DEFAULT_DB, help=f"Ruta a la base de datos SQLite (default: %(default)s)")
    ap.add_argument("--out", default=None, help="Archivo de salida (si se omite, imprime a stdout)")
    ap.add_argument("--raw", action="store_true", help="Imprime minificado (sin pretty-print)")
    ap.add_argument("--original", action="store_true", help="Devuelve solo el objeto '_original_request' si existe")
    args = ap.parse_args()

    q = fetch_query(args.db, args.consulta_id)
    if q is None:
        print(f"[error] No se encontró la consulta '{args.consulta_id}' en {args.db}", file=sys.stderr)
        sys.exit(1)

    # Decidir si devolvemos el JSON completo (interno) o solo el original
    output: str
    if args.original:
        try:
            obj = json.loads(q)
            orig = obj.get("_original_request")
            if orig is None:
                print("[error] No se encontró '_original_request' en la consulta almacenada", file=sys.stderr)
                sys.exit(3)
            if args.raw:
                output = json.dumps(orig, ensure_ascii=False, separators=(",", ":"))
            else:
                output = json.dumps(orig, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[error] No se pudo interpretar el JSON almacenado: {e}", file=sys.stderr)
            sys.exit(2)
    else:
        # Devolver la consulta almacenada (completa)
        if args.raw:
            output = q
        else:
            try:
                obj = json.loads(q)
                output = json.dumps(obj, ensure_ascii=False, indent=2)
            except Exception:
                # Si por alguna razón no parsea, devolvemos tal cual
                output = q

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(output)
        print(f"OK: query exportada a {args.out}")
    else:
        print(output)


if __name__ == "__main__":
    main()
