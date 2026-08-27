"""Ventanas horarias, con y sin cruce de medianoche.

El caso que motivó el módulo: la consulta `tNkrYxmZ` (abril de 2026) pidió 31
días con la ventana `01:01-00:00`. El estimador la interpretaba envolviendo
—`config.py`— pero la recuperación comparaba `inicio <= hhmm <= fin`, que para
esa ventana no es cierto nunca. La consulta terminó en `completado` con cero
archivos y el usuario no recibió aviso.
"""
import pytest

import horarios


# --- lectura de la ventana ---------------------------------------------------

@pytest.mark.parametrize("texto, esperado", [
    ("00:00-23:59", (0, 1439, False)),
    ("08:30-12:00", (510, 720, False)),
    ("14:30", (870, 870, False)),          # instante
    ("22:00-02:00", (1320, 120, True)),    # cruza
    ("01:01-00:00", (61, 0, True)),        # la de producción
    ("0830-1200", (510, 720, False)),      # sin dos puntos
])
def test_parsear(texto, esperado):
    assert horarios.parsear(texto) == esperado


@pytest.mark.parametrize("texto", ["", "24:00-01:00", "08:70", "ocho", "1-2-3", "8:0:0"])
def test_parsear_rechaza_basura(texto):
    with pytest.raises(ValueError):
        horarios.parsear(texto)


# --- pertenencia -------------------------------------------------------------

def test_ventana_normal():
    assert horarios.contiene("08:00-12:00", 8 * 60)        # extremo inicial
    assert horarios.contiene("08:00-12:00", 10 * 60)
    assert horarios.contiene("08:00-12:00", 12 * 60)       # extremo final
    assert not horarios.contiene("08:00-12:00", 7 * 60 + 59)
    assert not horarios.contiene("08:00-12:00", 12 * 60 + 1)


def test_ventana_que_cruza_medianoche():
    v = "22:00-02:00"
    assert horarios.contiene(v, 22 * 60)                   # extremo inicial
    assert horarios.contiene(v, 23 * 60 + 59)              # antes de medianoche
    assert horarios.contiene(v, 0)                         # medianoche
    assert horarios.contiene(v, 2 * 60)                    # extremo final
    assert not horarios.contiene(v, 2 * 60 + 1)            # justo después
    assert not horarios.contiene(v, 12 * 60)               # el hueco de en medio


def test_ventana_de_produccion_cubre_casi_todo_el_dia():
    """`01:01-00:00` deja fuera un solo minuto, no el día entero."""
    dentro = [m for m in range(horarios.MINUTOS_DIA)
              if horarios.contiene("01:01-00:00", m)]
    assert len(dentro) == horarios.MINUTOS_DIA - 60
    assert 0 in dentro and 61 in dentro
    assert 60 not in dentro            # 01:00, el minuto anterior al inicio


def test_instante_solo_se_contiene_a_si_mismo():
    assert horarios.contiene("14:30", 870)
    assert not horarios.contiene("14:30", 871)


def test_alguna_contiene_omite_las_ilegibles():
    assert horarios.alguna_contiene(["basura", "22:00-02:00"], 0)
    assert not horarios.alguna_contiene(["basura"], 0)


# --- horas cubiertas, para los prefijos de S3 --------------------------------

def test_horas_cubiertas_normal():
    assert horarios.horas_cubiertas("08:30-11:35") == [8, 9, 10, 11]


def test_horas_cubiertas_con_cruce_no_queda_vacio():
    """El bug de S3: `range(22, 3)` no listaba ningún prefijo."""
    assert horarios.horas_cubiertas("22:00-02:00") == [22, 23, 0, 1, 2]
    assert horarios.horas_cubiertas("01:01-00:00") == list(range(1, 24)) + [0]


# --- duración ----------------------------------------------------------------

def test_duracion_normal():
    assert horarios.duracion_horas("08:00-12:00") == 4.0
    assert horarios.duracion_horas("14:30") == 0.0


def test_duracion_con_cruce_no_es_negativa():
    """`total_horas` guardaba -1.02 h para la ventana de producción."""
    assert horarios.duracion_horas("22:00-02:00") == 4.0
    assert horarios.duracion_minutos("01:01-00:00") == horarios.MINUTOS_DIA - 61
    assert horarios.duracion_horas("01:01-00:00") > 0


# --- el filtro de recuperación, extremo a extremo ----------------------------

def _nombre(hhmm):
    # Nombre de objeto de S3: el separador antes del sello es «_s»
    # (`_FILENAME_TIMESTAMP_RE` en recover.py). Los .tgz de Lustre usan «-s».
    return f"OR_ABI-L1b-RadC-M6C13_G16_s2022001{hhmm}000_e_c.nc"


def test_filtro_de_recuperacion_recupera_la_ventana_que_cruza():
    from recover import filter_files_by_time
    archivos = [_nombre("2300"), _nombre("0100"), _nombre("1200")]
    dentro = filter_files_by_time(archivos, "2022001", ["22:00-02:00"])
    assert set(dentro) == {_nombre("2300"), _nombre("0100")}


def test_filtro_de_recuperacion_sin_cruce_no_cambia():
    from recover import filter_files_by_time
    archivos = [_nombre("0900"), _nombre("1300")]
    assert filter_files_by_time(archivos, "2022001", ["08:00-12:00"]) == [_nombre("0900")]


def test_filtro_ignora_otro_dia():
    from recover import filter_files_by_time
    assert filter_files_by_time([_nombre("2300")], "2022002", ["22:00-02:00"]) == []
