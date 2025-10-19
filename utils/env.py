import os
from typing import Optional

FALSE_VALUES = {"0", "false", "no", "off", "disabled"}
TRUE_VALUES = {"1", "true", "yes", "on", "enabled"}

def env_bool(name: str, default: bool = True) -> bool:
    """
    Lee una variable de entorno booleana de forma tolerante.
    - Acepta: 1/0, true/false, yes/no, on/off, enabled/disabled (case-insensitive)
    - Si no está definida, devuelve `default`.
    - Si el valor no coincide con ningún conjunto conocido, devuelve `default`.
    """
    v: Optional[str] = os.getenv(name)
    if v is None:
        return default
    s = v.strip().lower()
    if s in TRUE_VALUES:
        return True
    if s in FALSE_VALUES:
        return False
    # Si es cadena vacía u otro valor inesperado, usa el default
    return default
