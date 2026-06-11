"""
Coerce YAML / config values that may be int, float, or str when bound to olca metadata.
"""

from __future__ import annotations


def coerce_calendar_year(value) -> int:
    """
    Parse a calendar year from YAML (often int or quoted string).

    Raises ValueError or TypeError with a short message if the value is invalid.
    """
    if value is None:
        raise ValueError("Year value is missing")
    if isinstance(value, bool):
        raise ValueError(f"Invalid year (boolean): {value!r}")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        raise ValueError(f"Year must be a whole number, got {value!r}")
    if isinstance(value, str):
        s = value.strip()
        if not s:
            raise ValueError("Year string is empty")
        return int(s, 10)
    raise TypeError(f"Unsupported year type: {type(value).__name__}")


def metadata_value_is_empty(value) -> bool:
    """True if YAML value should be treated as 'no metadata' for string-like fields."""
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, (list, tuple, dict, set)):
        return len(value) == 0
    return False


def as_lookup_str(value) -> str:
    """
    Normalize values used as dict keys / olca name lookups (actor, source, etc.).

    YAML may parse unquoted tokens as ints or floats; openLCA names are strings.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)
