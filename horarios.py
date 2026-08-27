"""Interpretación de las ventanas horarias de una consulta.

Una ventana se escribe `HH:MM-HH:MM`, o `HH:MM` a secas para un instante.

Cuando el fin es anterior al inicio, la ventana **cruza la medianoche**:
`22:00-02:00` cubre de las 22:00 a las 23:59 y de las 00:00 a las 02:00 del
**mismo día juliano**. Ésa es la interpretación que el estimador ya usaba
(`config.py`, cálculo de `minute_range` con `% 1440`); la recuperación no la
implementaba, y una ventana así validaba con estimación distinta de cero pero
recuperaba cero archivos, terminando en `completado` sin aviso al usuario.

Este módulo es la única definición de esa semántica. Todo lo que decida si un
archivo cae dentro de una ventana debe pasar por aquí.
"""

MINUTOS_DIA = 24 * 60


def _a_minutos(texto: str) -> int:
    """`HH:MM`, `HHMM` o `HH` -> minutos desde la medianoche."""
    s = texto.strip().replace(":", "")
    if not s.isdigit() or len(s) not in (2, 4):
        raise ValueError(f"Hora no reconocida: {texto!r}")
    horas = int(s[:2])
    minutos = int(s[2:4]) if len(s) == 4 else 0
    if not (0 <= horas <= 23 and 0 <= minutos <= 59):
        raise ValueError(f"Hora fuera de rango: {texto!r}")
    return horas * 60 + minutos


def parsear(horario_str: str):
    """`(inicio, fin, cruza_medianoche)` en minutos desde la medianoche.

    Un instante sin guion devuelve `inicio == fin`. Lanza ValueError si el
    formato no se reconoce; quien llame decide si omitir la ventana o fallar.
    """
    partes = horario_str.split("-")
    if len(partes) > 2:
        raise ValueError(f"Ventana con más de un guion: {horario_str!r}")
    inicio = _a_minutos(partes[0])
    fin = _a_minutos(partes[1]) if len(partes) > 1 else inicio
    return inicio, fin, fin < inicio


def contiene(horario_str: str, minuto_del_dia: int) -> bool:
    """¿El minuto cae dentro de la ventana? Extremos incluidos."""
    inicio, fin, cruza = parsear(horario_str)
    if cruza:
        return minuto_del_dia >= inicio or minuto_del_dia <= fin
    return inicio <= minuto_del_dia <= fin


def alguna_contiene(horarios_list, minuto_del_dia: int) -> bool:
    """¿Alguna de las ventanas contiene el minuto? Las ilegibles se omiten."""
    for horario_str in horarios_list:
        try:
            if contiene(horario_str, minuto_del_dia):
                return True
        except ValueError:
            continue
    return False


def minuto_de_archivo(hora, minuto) -> int:
    """Minutos desde la medianoche a partir de las dos piezas de un nombre."""
    return int(hora) * 60 + int(minuto)


def horas_cubiertas(horario_str):
    """Horas (0-23) que la ventana toca, en orden, para enumerar prefijos.

    Con cruce de medianoche devuelve las dos partes: `22:00-02:00` da
    [22, 23, 0, 1, 2]. Es lo que S3 necesita para listar por hora.
    """
    inicio, fin, cruza = parsear(horario_str)
    h_ini, h_fin = inicio // 60, fin // 60
    if not cruza:
        return list(range(h_ini, h_fin + 1))
    return list(range(h_ini, 24)) + list(range(0, h_fin + 1))


def duracion_minutos(horario_str) -> int:
    """Extensión de la ventana. Un instante mide 0."""
    inicio, fin, cruza = parsear(horario_str)
    if cruza:
        return (MINUTOS_DIA - inicio) + fin
    return fin - inicio


def duracion_horas(horario_str) -> float:
    return duracion_minutos(horario_str) / 60.0
