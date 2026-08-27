"""Aplicación de `historic_query_schema.json`, el contrato de la consulta.

Ese archivo es la **fuente de verdad** del formato de consulta, y hasta el
27-ago-2026 no lo aplicaba nadie: `jsonschema` ni siquiera estaba en
`requirements.txt` y la única referencia estaba en un docstring de
`tools/csv_to_historic_json.py`. Lo que corría era `HistoricQueryRequest`
(`schemas.py`), bastante más laxo — `fechas: Dict[str, List[str]]` no valida ni
las claves ni los horarios, así que una clave como `'-20260101'` o un horario
como `'no es una hora'` llegaban enteros al procesador.

Los dos conviven a propósito y en este orden: el esquema decide qué se acepta,
Pydantic da la estructura tipada para el resto del código. Siguen discrepando en
`nivel` (obligatorio aquí, opcional allá) y `dominio` (opcional aquí porque los
satélites polares no lo llevan, obligatorio allá). Alinear Pydantic obliga a
auditar los usos de `dominio`, que hoy no puede ser `None` en ningún punto, así
que se pospuso al trabajo de soporte polar.

`historic_query` mantiene una copia de este esquema y valida con ella al recibir
la consulta, para poder decirle al usuario qué escribió mal en su idioma y en el
contexto del formulario. Esta validación no es redundante: historic_query no es
el único cliente (`tools/`, el simulador, curl a mano).
"""

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List

from jsonschema import Draft202012Validator

log = logging.getLogger(__name__)

RUTA_ESQUEMA = Path(__file__).resolve().parent / 'historic_query_schema.json'


@lru_cache(maxsize=1)
def validador() -> Draft202012Validator:
    """El validador, construido una sola vez por proceso."""
    with RUTA_ESQUEMA.open(encoding='utf-8') as f:
        esquema = json.load(f)
    Draft202012Validator.check_schema(esquema)
    return Draft202012Validator(esquema)


def errores_de_esquema(datos: Any) -> List[Dict[str, Any]]:
    """Problemas del JSON contra el contrato. Lista vacía si es válido.

    El formato imita el de `ValidationError.errors()` de Pydantic (`loc`/`msg`)
    para que el cuerpo del 422 se vea igual venga de donde venga.
    """
    if not isinstance(datos, dict):
        return [{"loc": [], "msg": "La consulta debe ser un objeto JSON.",
                 "type": "esquema.tipo"}]

    problemas = []
    for error in sorted(validador().iter_errors(datos),
                        key=lambda e: (list(map(str, e.absolute_path)), str(e.validator))):
        problemas.append({
            "loc": list(error.absolute_path),
            "msg": error.message,
            "type": f"esquema.{error.validator}",
        })

    if problemas:
        log.warning("Consulta rechazada por el esquema: %s",
                    "; ".join(f"{p['loc']}: {p['msg']}" for p in problemas))
    return problemas
