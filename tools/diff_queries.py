#!/usr/bin/env python3
"""
Construye una nueva consulta con horarios pendientes, restando las fechas/horarios
de una consulta A (base) menos una consulta B (excluir).

Uso:
    python tools/diff_queries.py --base victor_lustre.json --excluir victor_s3.json --out victor_lustre_pendiente.json

Notas:
    - Opera por fecha YYYYMMDD; si alguna fecha no aparece en 'excluir', se copia íntegra.
    - Soporta rangos "HH:MM-HH:MM" y tiempos puntuales "HH:MM".
    - Une y normaliza intervalos internamente antes de restar.
    - Semántica discreta por minuto: al restar [a,b] se excluyen ambos extremos;
        al restar un tiempo puntual "HH:MM" (a==b) se elimina ese minuto exacto.
"""
import argparse
import json
from typing import List, Tuple, Dict


def parse_time(s: str) -> int:
    h, m = s.split(":")
    return int(h) * 60 + int(m)


def to_str(t: int) -> str:
    return f"{t//60:02d}:{t%60:02d}"


def parse_interval(s: str) -> Tuple[int, int]:
    if "-" in s:
        a, b = s.split("-")
        return (parse_time(a), parse_time(b))
    t = parse_time(s)
    return (t, t)


def merge_intervals(ints: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    if not ints:
        return []
    ints = sorted((min(a, b), max(a, b)) for a, b in ints)
    merged = [ints[0]]
    for a, b in ints[1:]:
        la, lb = merged[-1]
        if a <= lb:  # solapa o contiguo
            merged[-1] = (la, max(lb, b))
        else:
            merged.append((a, b))
    return merged


def subtract(base: List[Tuple[int, int]], excl: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    """Resta la unión de 'excl' a los intervalos 'base' usando minutos discretos inclusivos.
    Devuelve segmentos que no incluyen ninguno de los minutos en 'excl'.
    """
    if not base:
        return []
    base = merge_intervals(base)
    excl = merge_intervals(excl)
    result: List[Tuple[int, int]] = []
    for x0, x1 in base:
        segments = [(x0, x1)]
        for a, b in excl:
            new_segments: List[Tuple[int, int]] = []
            for s0, s1 in segments:
                if b < s0 or a > s1:
                    # disjuntos
                    new_segments.append((s0, s1))
                else:
                    # solapan: recortar
                    # excluir inclusive [a,b] ajustando por un minuto
                    left_end = a - 1
                    right_start = b + 1
                    if left_end >= s0:
                        new_segments.append((s0, left_end))
                    if right_start <= s1:
                        new_segments.append((right_start, s1))
            segments = new_segments
            if not segments:
                break
        result.extend(segments)
    # Normalizar intervalos inválidos (a==b representa tiempo puntual)
    norm = []
    for a, b in result:
        if a > b:
            continue
        norm.append((a, b))
    return merge_intervals(norm)


def format_intervals(ints: List[Tuple[int, int]], prefer_points: bool = True) -> List[str]:
    out = []
    for a, b in ints:
        if prefer_points and a == b:
            out.append(to_str(a))
        else:
            out.append(f"{to_str(a)}-{to_str(b)}")
    return out


def build_remaining(base_query: Dict, excl_query: Dict) -> Dict:
    # Copiar campos principales desde base
    out = {}
    for key in ["sat", "nivel", "dominio", "bandas", "creado_por", "productos"]:
        if key in base_query and base_query[key] is not None:
            out[key] = base_query[key]

    base_fechas: Dict[str, List[str]] = base_query.get("fechas", {}) or {}
    excl_fechas: Dict[str, List[str]] = excl_query.get("fechas", {}) or {}

    result_fechas: Dict[str, List[str]] = {}
    for fecha, rangos in base_fechas.items():
        base_ints = [parse_interval(s) for s in rangos]
        excl_ints = [parse_interval(s) for s in excl_fechas.get(fecha, [])]
        remaining = subtract(base_ints, excl_ints)
        if remaining:
            result_fechas[fecha] = format_intervals(remaining)

    out["fechas"] = result_fechas
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="JSON base (de donde restar)")
    ap.add_argument("--excluir", required=True, help="JSON con horarios ya satisfechos")
    ap.add_argument("--out", required=True, help="Archivo de salida")
    args = ap.parse_args()

    with open(args.base, "r", encoding="utf-8") as f:
        base_query = json.load(f)
    with open(args.excluir, "r", encoding="utf-8") as f:
        excl_query = json.load(f)

    remaining = build_remaining(base_query, excl_query)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(remaining, f, ensure_ascii=False, indent=2)
    print(f"OK: escrito {args.out}")


if __name__ == "__main__":
    main()
